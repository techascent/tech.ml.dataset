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
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.pipeline.column-filters :as col-filters]
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
           [java.util HashSet Map List]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [org.roaringbitmap RoaringBitmap])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth shuffle
                            rand-nth]))


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
                        column-map
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


(par-util/export-symbols tech.ml.dataset.join
                         hash-join
                         inner-join
                         right-join
                         left-join)


(par-util/export-symbols tech.ml.dataset.impl.dataset
                         new-dataset)

(par-util/export-symbols tech.ml.dataset.parse.name-values-seq
                         name-values-seq->dataset)


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


(defn columnwise-reduce
  "In parallel, run function over each column and produce a new dataset with a
  single row of the result.  Returns a new dataset with the same columns as the
  original.  Exceptions are logged with 'warn' and a missing value for that column
  is produced.

  In addition to being a function from column->scalar, reduce-fn may be a map of
  colname to reduction function in which case only columns which have entries in
  the map are reduced, other columns are dropped.  "
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

  Options -
  :datatype - datatype of the resulting column if one aside from :object is desired."
  ([dataset column-name]
   (unroll-column dataset column-name {}))
  ([dataset column-name options]
   (let [coldata (dtype/->reader (dataset column-name))
         result-datatype (or (:datatype options) :object)
         ^RoaringBitmap missing (ds-col/missing (dataset column-name))
         cast-fn (if (casting/numeric-type? result-datatype)
                   #(casting/cast % result-datatype)
                   identity)
         [indexes container]
         (parallel-for/indexed-map-reduce
          (dtype/ecount coldata)
          (fn [^long start-idx ^long len]
            (let [^List container (col-impl/make-container result-datatype)
                  indexes (LongArrayList.)]
              (dotimes [iter len]
                (let [idx (+ iter start-idx)]
                  (when-not (.contains missing idx)
                    (let [data-item (coldata idx)]
                      (if (instance? Iterable data-item)
                        (parallel-for/doiter
                         data data-item
                         (.add container (cast-fn data))
                         (.add indexes idx))
                        (do
                          (.add container (cast-fn data-item))
                          (.add indexes idx)))))))
              [indexes container]))
          (partial clojure.core/reduce
                   (fn [[lhs-indexes lhs-container]
                        [rhs-indexes rhs-container]]
                     (.addAll ^List lhs-indexes ^List rhs-indexes)
                     (.addAll ^List lhs-container ^List rhs-container)
                     [lhs-indexes lhs-container])))]
     (-> (remove-column dataset column-name)
         (select-rows indexes)
         (add-or-update-column column-name (col-impl/new-column
                                            column-name
                                            container))))))


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
   :mean :mode :min :max :standard-deviation :skew
   :n-values :values])


(defn descriptive-stats
  "Get descriptive statistics across the columns of the dataset.
  In addition to the standard stats.
  Options:
  :stat-names - defaults to (remove #{:values :num-distinct-values}
                                    (all-descriptive-stats-names))
  :n-categorical-values - Number of categorical values to report in the 'values'
     field. nil means all values, defaults to 21."
  ([dataset]
   (descriptive-stats dataset {}))
  ([dataset options]
   (let [stat-names (or (:stat-names options)
                        (->> (all-descriptive-stats-names)
                             ;;This just is too much information for small repls.
                             (remove #{:values :n-values})))
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
                                                  #{:min :mean :max
                                                    :standard-deviation :skew})
                           :else
                           (let [histogram (->> (frequencies col-reader)
                                                (clojure.core/sort-by second >))]
                             (merge
                              {:mode (ffirst histogram)
                               :n-values (count histogram)}
                              {:values
                               (->> (map first histogram)
                                    (take (or (:n-categorical-values options)
                                              21))
                                    vec)})))))))
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
