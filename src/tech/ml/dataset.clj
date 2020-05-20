(ns tech.ml.dataset
  "Column major dataset abstraction for efficiently manipulating
  in memory datasets."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.parallel.utils :as par-util]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.string-table :as str-table]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.pipeline.column-filters :as col-filters]
            [tech.ml.dataset.dynamic-int-list :as int-list]
            [tech.ml.dataset.parse :as dt-parse]
            [tech.ml.dataset.parse.name-values-seq :as parse-nvs]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.base :as ds-base]
            [tech.ml.dataset.modelling]
            [tech.ml.dataset.math]
            [tech.v2.datatype.casting :as casting]
            [tech.parallel.for :as parallel-for]
            [clojure.math.combinatorics :as comb]
            [clojure.tools.logging :as log]
            [clojure.set :as set])
  (:import [smile.clustering KMeans GMeans XMeans PartitionClustering]
           [java.util HashSet Map List Iterator Collection ArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [org.roaringbitmap RoaringBitmap])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth shuffle
                            rand-nth assoc dissoc]))


(set! *warn-on-reflection* true)

(par-util/export-symbols tech.ml.dataset.base
                        dataset-name
                        set-dataset-name
                        ds-row-count
                        row-count
                        ds-column-count
                        column-count
                        metadata
                        set-metadata
                        maybe-column
                        column
                        columns
                        column-name->column-map
                        column-names
                        has-column?
                        columns-with-missing-seq
                        add-column
                        new-column
                        remove-column
                        remove-columns
                        drop-columns
                        update-column
                        order-column-names
                        update-columns
                        rename-columns
                        select
                        unordered-select
                        select-columns
                        select-rows
                        drop-rows
                        remove-rows
                        missing
                        add-or-update-column
                        value-reader
                        mapseq-reader
                        ds-group-by
                        ds-group-by-column
                        group-by->indexes
                        group-by-column->indexes
                        group-by
                        group-by-column
                        sort-by
                        sort-by-column
                        ds-sort-by
                        ds-sort-by-column
                        ->sort-by
                        ->sort-by-column
                        filter
                        filter-column
                        ds-filter
                        ds-filter-column
                        unique-by
                        unique-by-column
                        aggregate-by
                        aggregate-by-column
                        ds-concat
                        concat
                        ds-take-nth
                        take-nth
                        ->dataset
                        ->>dataset
                        from-prototype
                        dataset->string
                        write-csv!)


(defn shape
  "Returns shape in row-major format of [n-columns n-rows]."
  [dataset]
  (dtype/shape dataset))


(par-util/export-symbols tech.ml.dataset.join
                         hash-join
                         inner-join
                         right-join
                         left-join)


(par-util/export-symbols tech.ml.dataset.impl.dataset
                         new-dataset)

(par-util/export-symbols tech.ml.dataset.parse.name-values-seq
                         name-values-seq->dataset)


(defn assoc
  "If column exists, replace.  Else append new column.  The datatype of the new column
  will the the datatype of the coldata.

  coldata may be a sequence in which case 'vec' will be called and the datatype will be
  :object.

  coldata may be a reader, in which case the datatype will be the datatype of the
  reader.  One way to make a reader is tech.v2.datatype/make-reader or anything
  deriving from java.util.List and java.util.RandomAccess will do.

  coldata may also be a new column (tech.ml.dataset.column/new-column) in which case
  the missing set and the column metadata can be provided."
  ([dataset colname coldata & more]
   (do
     (when-not (== 0 (rem (count more) 2))
       (throw (Exception. "assoc requires an even number of arguments")))
     (->> more
          (partition 2)
          (reduce #(add-or-update-column %1 (first %2) (second %2))
                  (add-or-update-column dataset colname coldata)))))
  ([dataset colname coldata]
   (add-or-update-column dataset colname coldata)))


(defn dissoc
  "Remove one more more columns from the dataset."
  ([dataset colname & more]
   (->> more
        (reduce #(remove-column %1 %2)
                (remove-column dataset colname))))
  ([dataset colname]
   (remove-column dataset colname)))


(defn head
  "Get the first n row of a dataset.  Equivalent to
  `(select-rows ds (range n)).  Arguments are reversed, however, so this can
  be used in ->> operators."
  ([n dataset]
   (select-rows dataset (range n)))
  ([dataset]
   (head 5 dataset)))


(defn tail
  "Get the last n rows of a dataset.  Equivalent to
  `(select-rows ds (range ...)).  Argument order is dataset-last, however, so this can
  be used in ->> operators."
  ([n dataset]
   (let [n-rows (row-count dataset)
         start-idx (max 0 (- n-rows (long n)))]
     (select-rows dataset (range start-idx n-rows))))
  ([dataset]
   (tail 5 dataset)))


(defn shuffle
  [dataset]
  (select-rows dataset (clojure.core/shuffle (range (row-count dataset)))))


(defn sample
  "Sample n-rows from a dataset.  Defaults to sampling *without* replacement."
  ([n replacement? dataset]
   (let [row-count (row-count dataset)
         n (long n)]
     (if replacement?
       (select-rows dataset (repeatedly n #(rand-int row-count)))
       (select-rows dataset (take (min n row-count)
                                  (clojure.core/shuffle (range row-count)))))))
  ([n dataset]
   (sample n false dataset))
  ([dataset]
   (sample 5 false dataset)))


(defn rand-nth
  "Return a random row from the dataset in map format"
  [dataset]
  (clojure.core/rand-nth (mapseq-reader dataset)))


(defn column->dataset
  "Transform a column into a sequence of maps using transform-fn.
  Return dataset created out of the sequence of maps."
  ([dataset colname transform-fn options]
   (->> (pmap transform-fn (dataset colname))
        (->>dataset options)))
  ([dataset colname transform-fn]
   (column->dataset dataset colname transform-fn {})))


(defn append-columns
  [dataset column-seq]
  (new-dataset (dataset-name dataset)
               (metadata dataset)
               (clojure.core/concat (columns dataset) column-seq)))


(defn column-map
  "Produce a new column as the result of mapping a fn over other columns.
  The result column will have a datatype of the widened combination of all
  the input column datatypes.
  The result column's missing indexes is the union of all input columns."
  [dataset result-colname map-fn colname & colnames]
  (let [all-colnames (->> (clojure.core/concat [colname]
                                               colnames)
                          (remove nil?)
                          vec)
        ;;Select the src columns to generate an error.
        src-ds (select-columns dataset all-colnames)
        missing-set (if (== 1 (count all-colnames))
                      (ds-col/missing (src-ds (first all-colnames)))
                      (reduce dtype-proto/set-or
                              (map ds-col/missing src-ds)))
        result-reader (apply dtype/typed-reader-map
                             map-fn (columns src-ds))]
    (add-or-update-column
     dataset result-colname
     (ds-col/new-column result-colname result-reader {} missing-set))))


(defn column-cast
  "Cast a column to a new datatype.  This is never a lazy operation.  If the old
  and new datatypes match and no cast-fn is provided then dtype/clone is called
  on the column.

  colname may be a scalar or a tuple of [src-col dst-col].

  datatype may be a datatype enumeration or a tuple of
  [datatype cast-fn] where cast-fn may return either a new value,
  :tech.ml.dataset.parse/missing, or :tech.ml.dataset.parse/parse-failure.
  Exceptions are propagated to the caller.  The new column has at least the
  existing missing set (if no attempt returns :missing or :cast-failure).
  :cast-failure means the value gets added to metadata key :unparsed-data
  and the index gets added to :unparsed-indexes.


  If the existing datatype is string, then tech.ml.datatype.column/parse-column
  is called.

  Casts between numeric datatypes need no cast-fn but one may be provided.
  Casts to string need no cast-fn but one may be provided.
  Casts from string to anything will call tech.ml.dataset.column/parse-column."
  [dataset colname datatype]
  (let [[src-colname dst-colname] (if (instance? Collection colname)
                                    colname
                                    [colname colname])
        src-col (dataset src-colname)
        src-dtype (dtype/get-datatype src-col)
        [dst-dtype cast-fn] (if (instance? Collection datatype)
                              datatype
                              [datatype nil])]
    (add-or-update-column
     dataset dst-colname
     (cond
       (and (= src-dtype dst-dtype)
            (nil? cast-fn))
       (dtype/clone src-col)
       (= src-dtype :string)
       (ds-col/parse-column datatype src-col)
       :else
       (let [cast-fn (or cast-fn
                         (cond
                           (= dst-dtype :string)
                           str
                           (or (= :boolean dst-dtype)
                               (casting/numeric-type? dst-dtype))
                           #(casting/cast % dst-dtype)
                           :else
                           (throw (Exception.
                                   (format "Cast fn must be provided for datatype %"
                                           dst-dtype)))))
             ^RoaringBitmap missing (dtype-proto/as-roaring-bitmap
                                     (ds-col/missing src-col))
             ^RoaringBitmap new-missing (dtype/clone missing)
             col-reader (dtype-pp/reader-converter (dtype/->reader src-col))
             n-elems (dtype/ecount col-reader)
             unparsed-data (ArrayList.)
             unparsed-indexes (bitmap/->bitmap)
             result (if (= dst-dtype :string)
                      (str-table/make-string-table n-elems)
                      (dtype/make-container :java-array dst-dtype n-elems))
             res-writer (dtype/->writer result)
             missing-val (col-impl/datatype->missing-value dst-dtype)]
         (parallel-for/parallel-for
          idx
          n-elems
          (if (.contains missing idx)
            (res-writer idx missing-val)
            (let [existing-val (col-reader idx)
                  new-val (cast-fn existing-val)]
              (cond
                (= new-val :tech.ml.dataset.parse/missing)
                (locking new-missing
                  (.add new-missing idx)
                  (res-writer idx missing-val))
                (= new-val :tech.ml.dataset.parse/parse-failure)
                (locking new-missing
                  (res-writer idx missing-val)
                  (.add new-missing idx)
                  (.add unparsed-indexes idx)
                  (.add unparsed-data existing-val))
                :else
                (res-writer idx new-val)))))
         (ds-col/new-column dst-colname result (clojure.core/assoc
                                                (meta src-col)
                                                :unparsed-indexes unparsed-indexes
                                                :unparsed-data unparsed-data)
                            missing))))))


(defn columnwise-reduce
  "In parallel, run function over each column and produce a new dataset with a
  single row of the result.  Returns a new dataset with the same columns as the
  original.  Exceptions are logged with 'warn' and a missing value for that column
  is produced.

  In addition to being a function from column->scalar, reduce-fn may be a map of
  colname to reduction function in which case only columns which have entries in
  the map are reduced, other columns are dropped.

  This function is deprecated -
  https://github.com/techascent/tech.ml.dataset/issues/43

  "
  [reduce-fn dataset]
  (let [colnames (if (instance? Map reduce-fn)
                   (let [keyset (.keySet ^Map reduce-fn)]
                     (clojure.core/filter #(.contains keyset  %)
                                          (column-names dataset)))
                   (column-names dataset))
        dataset (select-columns dataset colnames)]
    (->> dataset
         (pmap (fn [col]
                 (let [colname (ds-col/column-name col)]
                   [colname
                    (try
                      (if (instance? Map reduce-fn)
                        (when-let [map-reduce-fn (get reduce-fn colname)]
                          (map-reduce-fn col))
                        (reduce-fn col))
                      (catch Throwable e
                        (log/warnf "Error processing column %s: %s"
                                   (ds-col/column-name col) e)
                        nil))])))
         (into {})
         (vector)
         (->>dataset {:dataset-name (dataset-name dataset)}))))


(defn columnwise-concat
  "Given a dataset and a list of columns, produce a new dataset with
  the columns concatenated to a new column with a :column column indicating
  which column the original value came from.  Any columns not mentioned in the
  list of columns are duplicated.

  Example:
user> (-> [{:a 1 :b 2 :c 3 :d 1} {:a 4 :b 5 :c 6 :d 2}]
          (ds/->dataset)
          (ds/columnwise-concat [:c :a :b]))
null [6 3]:

| :column | :value | :d |
|---------+--------+----|
|      :c |      3 |  1 |
|      :c |      6 |  2 |
|      :a |      1 |  1 |
|      :a |      4 |  2 |
|      :b |      2 |  1 |
|      :b |      5 |  2 |

  Options:

  value-column-name - defaults to :value
  colname-column-name - defaults to :column
  "
  ([dataset colnames {:keys [value-column-name
                             colname-column-name]
                       :or {value-column-name :value
                            colname-column-name :column}
                      :as _options}]
   (let [row-count (row-count dataset)
         colname-set (set colnames)
         leftover-columns (remove (comp colname-set
                                        ds-col/column-name)
                                  dataset)]
     ;;Note this is calling dataset's concat, not clojure.core's concat
     ;;Use apply instead of reduce so that the concat function can see the
     ;;entire dataset list at once.  This makes a more efficient reader implementation
     ;;for each column if all the datasets are the same length which in this case
     ;;they are guaranteed to be.
     (apply concat (map (fn [col-name]
                          (let [data (dataset col-name)]
                            (new-dataset
                             ;;confusing...
                             (clojure.core/concat
                              [(ds-col/new-column colname-column-name
                                                  (const-rdr/make-const-reader
                                                   col-name :object row-count))
                               (ds-col/set-name data value-column-name)]
                              leftover-columns))))
                        colnames))))
  ([dataset colnames]
   (columnwise-concat dataset colnames {})))


(defn column-labeled-mapseq
  "Given a dataset, return a sequence of maps where several columns are all stored
  in a :value key and a :label key contains a column name.  Used for quickly creating
  timeseries or scatterplot labeled graphs.  Returns a lazy sequence, not a reader!

  See also `columnwise-concat`

  Return a sequence of maps with
  {... - columns not in colname-seq
   :value - value from one of the value columns
   :label - name of the column the value came from
  }"
  [dataset value-colname-seq]
  (->> (columnwise-concat dataset value-colname-seq
                          {:value-column-name :value
                           :colname-column-name :label})
       (mapseq-reader)))


(defn unroll-column
  "Unroll a column that has some (or all) sequential data as entries.
  Returns a new dataset with same columns but with other columns duplicated
  where the unroll happened.  Column now contains only scalar data.

  Any missing indexes are dropped.

user> (-> (ds/->dataset [{:a 1 :b [2 3]}
                              {:a 2 :b [4 5]}
                              {:a 3 :b :a}])
               (ds/unroll-column :b {:indexes? true}))
  _unnamed [5 3]:

| :a | :b | :indexes |
|----+----+----------|
|  1 |  2 |        0 |
|  1 |  3 |        1 |
|  2 |  4 |        0 |
|  2 |  5 |        1 |
|  3 | :a |        0 |

  Options -
  :datatype - datatype of the resulting column if one aside from :object is desired.
  :indexes? - If true, create a new column that records the indexes of the values from
    the original column.  Can also be a truthy value (like a keyword) and the column
    will be named this."
  ([dataset column-name]
   (unroll-column dataset column-name {}))
  ([dataset column-name options]
   (let [coldata (dtype/->reader (dataset column-name))
         result-datatype (or (:datatype options) :object)
         idx-colname (when-let [idx-name (:indexes? options)]
                       (if (boolean? idx-name)
                         :indexes
                         idx-name))
         ^RoaringBitmap missing (ds-col/missing (dataset column-name))
         cast-fn (if (casting/numeric-type? result-datatype)
                   #(casting/cast % result-datatype)
                   identity)
         [indexes container idx-container]
         (parallel-for/indexed-map-reduce
          (dtype/ecount coldata)
          (fn [^long start-idx ^long len]
            (let [^List container (col-impl/make-container result-datatype)
                  indexes (LongArrayList.)
                  ^List idx-container (when idx-colname
                                        (IntArrayList.))]
              (dotimes [iter len]
                (let [idx (+ iter start-idx)]
                  (when-not (.contains missing idx)
                    (let [data-item (coldata idx)]
                      (if (or (dtype/reader? data-item)
                              (instance? Iterable data-item))
                        (let [^Iterator src-iter (if (instance? Iterable data-item)
                                                   (.iterator ^Iterable data-item)
                                                   (.iterator ^Iterable
                                                              (dtype/->reader
                                                               data-item)))]
                          (loop [continue? (.hasNext src-iter)
                                 inner-idx 0]
                            (when continue?
                              (.add container (cast-fn (.next src-iter)))
                              (.add indexes idx)
                              (when idx-colname
                                (.add idx-container (unchecked-int
                                                     inner-idx)))
                              (recur (.hasNext src-iter)
                                     (unchecked-inc inner-idx)))))
                        ;;Else treat value as scalar
                        (do
                          (.add container (cast-fn data-item))
                          (.add indexes idx)
                          (when idx-colname
                            (.add idx-container (unchecked-int 0)))))))))
              [indexes container idx-container]))
          (partial clojure.core/reduce
                   (fn [[lhs-indexes lhs-container lhs-idx-container]
                        [rhs-indexes rhs-container rhs-idx-container]]
                     (.addAll ^List lhs-indexes ^List rhs-indexes)
                     (.addAll ^List lhs-container ^List rhs-container)
                     (when lhs-idx-container
                       (.addAll ^List lhs-idx-container ^List rhs-idx-container))
                     [lhs-indexes lhs-container lhs-idx-container])))]
     (-> (remove-column dataset column-name)
         (select-rows indexes)
         (add-or-update-column column-name (col-impl/new-column
                                            column-name
                                            container))
         (#(if idx-container
             (add-or-update-column % idx-colname idx-container)
             %))))))


(defn ->distinct-by-column
  "Drop successive rows where we already have a given key."
  [ds colname]
  (let [coldata (ds colname)
        colset (HashSet.)
        bitmap (bitmap/->bitmap)
        n-elems (dtype/ecount coldata)
        obj-rdr (typecast/datatype->reader :object coldata)]
    (dotimes [idx n-elems]
      (when (.add colset (.read obj-rdr idx))
        (.add bitmap idx)))
    (select-rows ds bitmap)))


(defn n-permutations
  "Return n datasets with all permutations n of the columns possible.
  N must be less than (column-count dataset))."
  [n dataset]
  (ds-base/check-dataset-wrong-position n)
  (when-not (< n (first (dtype/shape dataset)))
    (throw (ex-info (format "%d permutations of %d columns"
                            n (first (dtype/shape dataset)))
                    {})))
  (->> (comb/combinations (column-names dataset) n)
       (map set)
       ;;assume order doesn't matter
       distinct
       (map (partial select-columns dataset))))


(defn n-feature-permutations
  "Given a dataset with at least one inference target column, produce all datasets
  with n feature columns and the label columns."
  [n dataset]
  (ds-base/check-dataset-wrong-position n)
  (let [label-columns (col-filters/target? dataset)
        feature-columns (col-filters/not label-columns dataset)]
    (when-not (seq label-columns)
      (throw (ex-info "No label columns indicated" {})))
    (->> (comb/combinations feature-columns n)
         (map set)
         ;;assume order doesn't matter
         distinct
         (map (comp (partial select-columns dataset)
                    (partial clojure.core/concat label-columns))))))


(par-util/export-symbols tech.ml.dataset.modelling
                        set-inference-target
                        column-label-map
                        inference-target-label-map
                        dataset-label-map
                        inference-target-label-inverse-map
                        num-inference-classes
                        feature-ecount
                        model-type
                        column-values->categorical
                        reduce-column-names
                        has-column-label-map?
                        ->k-fold-datasets
                        ->train-test-split
                        ->row-major)


(par-util/export-symbols tech.ml.dataset.math
                        correlation-table
                        k-means
                        g-means
                        x-means
                        compute-centroid-and-global-means
                        impute-missing-by-centroid-averages
                        interpolate-loess)


(defn all-descriptive-stats-names
  "Returns the names of all descriptive stats in the order they will be returned
  in the resulting dataset of descriptive stats.  This allows easy filtering
  in the form for
  (descriptive-stats ds {:stat-names (->> (all-descriptive-stats-names)
                                          (remove #{:values :num-distinct-values}))})"
  []
  [:col-name :datatype :n-valid :n-missing
   :min :quartile-1 :mean :mode :quartile-3 :max
   :standard-deviation :skew :n-values :values :histogram])


(defn descriptive-stats
  "Get descriptive statistics across the columns of the dataset.
  In addition to the standard stats.
  Options:
  :stat-names - defaults to (remove #{:values :num-distinct-values}
                                    (all-descriptive-stats-names))
  :n-categorical-values - Number of categorical values to report in the 'values'
     field. Defaults to 21."
  ([dataset]
   (descriptive-stats dataset {}))
  ([dataset options]
   (let [stat-names (or (:stat-names options)
                        (->> (all-descriptive-stats-names)
                             ;;This just is too much information for small repls.
                             (remove #{:values :n-values :quartile-1 :quartile-3 :histogram})))
         stats-ds
         (->> (->dataset dataset)
              (pmap (fn [ds-col]
                      (let [n-missing (dtype/ecount (ds-col/missing ds-col))
                            n-valid (- (dtype/ecount ds-col)
                                       n-missing)
                            col-dtype (dtype/get-datatype ds-col)
                            col-reader (dtype/->reader ds-col
                                                       col-dtype
                                                       {:missing-policy :elide})]
                        (merge
                         {:col-name (ds-col/column-name ds-col)
                          :datatype col-dtype
                          :n-valid n-valid
                          :n-missing n-missing}
                         (cond
                           (dtype-dt/datetime-datatype? col-dtype)
                           (dtype-dt-ops/millisecond-descriptive-stats col-reader)
                           (and (not (:categorical? (ds-col/metadata ds-col)))
                                (casting/numeric-type? col-dtype))
                           (dfn/descriptive-stats col-reader
                                                  #{:min :quartile-1 :mean :quartile-3
                                                    :max :standard-deviation :skew})
                           :else
                           (let [histogram (->> (frequencies col-reader)
                                                (clojure.core/sort-by second >))
                                 max-categorical-values (or (:n-categorical-values options) 21)]
                             (merge
                              {:mode (ffirst histogram)
                               :n-values (count histogram)}
                              {:values
                               (->> (map first histogram)
                                    (take max-categorical-values)
                                    (vec))}
                              (when (< (count histogram) max-categorical-values)
                                {:histogram histogram}))))))))
              (clojure.core/sort-by (comp str :col-name))
              ->dataset)
         existing-colname-set (->> (column-names stats-ds)
                                   set)]
     ;;This orders the columns by the ordering of stat-names but if for instance
     ;;there were no numeric or no string columns it still works.
     (-> stats-ds
         (select-columns (->> stat-names
                              (clojure.core/filter existing-colname-set)))
         (set-dataset-name (str (dataset-name dataset) ": descriptive-stats"))))))


(defn brief
  "Get a brief description, in mapseq form of a dataset.  A brief description is
  the mapseq form of descriptive stats."
  ([ds options]
   (->> (descriptive-stats ds options)
        (mapseq-reader)
        ;;Remove nil entries from the data.
        (map #(->> (clojure.core/filter second %)
                   (into {})))))
  ([ds]
   (brief ds {:stat-names (all-descriptive-stats-names)
              :n-categorical-values nil})))


(defn reverse-map-categorical-columns
  "Given a dataset where we have converted columns from a categorical representation
  to either a numeric reprsentation or a one-hot representation, reverse map
  back to the original dataset given the reverse mapping of label->number in
  the column's metadata."
  [dataset {:keys [column-name-seq]}]
  (let [label-map (dataset-label-map dataset)
        column-name-seq (or column-name-seq
                            (column-names dataset))
        column-name-seq (reduce-column-names dataset column-name-seq)
        dataset
        (clojure.core/reduce
         (fn [dataset colname]
           (if (contains? label-map colname)
             (add-or-update-column dataset colname
                                   (categorical/column-values->categorical
                                    dataset colname label-map))
             dataset))
         dataset
         column-name-seq)]
    (select-columns dataset column-name-seq)))


(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown
  on missing values or if nil should be inserted in the map.  IF a label map is passed
  in then for the columns that are present in the label map a reverse mapping is done
  such that the flyweight maps contain the labels and not their encoded values."
  [dataset & {:keys [column-name-seq
                     error-on-missing-values?
                     number->string?]
              :or {error-on-missing-values? true}}]
  (let [column-name-seq (or column-name-seq
                            (column-names dataset))
        dataset (if number->string?
                  (reverse-map-categorical-columns dataset {:column-name-seq
                                                            column-name-seq})
                  (select-columns dataset column-name-seq))]
    (when-let [missing-columns
               (when error-on-missing-values?
                 (seq (columns-with-missing-seq dataset)))]
      (throw (Exception. (format "Columns with missing data detected: %s"
                                 missing-columns))))
    (mapseq-reader dataset)))


(defn labels
  "Given a dataset and an options map, generate a sequence of label-values.
  If label count is 1, then if there is a label-map associated with column
  generate sequence of labels by reverse mapping the column(s) back to the original
  dataset values.  If there are multiple label columns results are presented in
  a dataset."
  [dataset]
  (when-not (seq (col-filters/target? dataset))
    (throw (ex-info "No label columns indicated" {})))
  (let [original-label-column-names (->> (col-filters/inference? dataset)
                                         (reduce-column-names dataset))
        dataset (reverse-map-categorical-columns
                 dataset {:column-name-seq
                          original-label-column-names})]
    (if (= 1 (column-count dataset))
      (dtype/->reader (first dataset))
      dataset)))
