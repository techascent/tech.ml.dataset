(ns tech.v3.dataset
  "Column major dataset abstraction for efficiently manipulating
  in memory datasets."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.update-reader :as update-reader]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.categorical :as ds-cat]
            ;;csv/tsv load/save provided by default
            [tech.v3.dataset.io.univocity]
            [tech.v3.dataset.io.nippy]
            [tech.v3.libs.smile.data :as smile-data]
            [clojure.set :as set])
  (:import [java.util List Iterator Collection ArrayList ]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype PrimitiveList]
           [clojure.lang IFn])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth shuffle
                            rand-nth update]))


(set! *warn-on-reflection* true)


(defonce ^{:doc "Datasets will now identify their major versions"
           :tag 'long} major-version 5)


(export-symbols tech.v3.dataset.base
                dataset-name
                set-dataset-name
                row-count
                column-count
                column
                columns
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
                drop-missing
                add-or-update-column
                group-by->indexes
                group-by-column->indexes
                group-by
                group-by-column
                sort-by
                sort-by-column
                filter
                filter-column
                unique-by
                unique-by-column
                concat
                concat-copying
                concat-inplace
                take-nth
                ensure-array-backed
                dataset->data
                data->dataset)


(export-symbols tech.v3.dataset.readers
                value-reader
                mapseq-reader)


(export-symbols tech.v3.dataset.io
                ->dataset
                ->>dataset
                write!)


(defmacro bind->
  "Threads like `->` but binds name to expr like `as->`:


```clojure
(ds/bind-> (ds/->dataset \"test/data/stocks.csv\") ds
           (assoc :logprice2 (dfn/log1p (ds \"price\")))
           (assoc :logp3 (dfn/* 2 (ds :logprice2)))
           (ds/select-columns [\"price\" :logprice2 :logp3])
           (ds-tens/dataset->tensor)
           (first))
```"
  [expr name & forms]
  `(as-> ~expr ~name
     ~@(map (fn [form]
              (if (seq? form)
                (with-meta `(~(first form) ~name ~@(next form)) (meta form))
                (list form name)))
            forms)))



(defn shape
  "Returns shape in column-major format of [n-columns n-rows]."
  [dataset]
  (dtype/shape dataset))


(export-symbols tech.v3.dataset.impl.dataset
                new-dataset)

(export-symbols tech.v3.dataset.missing
                select-missing replace-missing)


(defn head
  "Get the first n row of a dataset.  Equivalent to
  `(select-rows ds (range n)).  Arguments are reversed, however, so this can
  be used in ->> operators."
  ([dataset n]
   (-> (select-rows dataset (range n))
       (vary-meta clojure.core/assoc :print-index-range (range n))))
  ([dataset]
   (head dataset 5)))


(defn tail
  "Get the last n rows of a dataset.  Equivalent to
  `(select-rows ds (range ...)).  Argument order is dataset-last, however, so this can
  be used in ->> operators."
  ([dataset n]
   (let [n-rows (row-count dataset)
         start-idx (max 0 (- n-rows (long n)))]
     (-> (select-rows dataset (range start-idx n-rows))
         (vary-meta clojure.core/assoc :print-index-range (range n)))))
  ([dataset]
   (tail dataset 5)))


(defn shuffle
  [dataset]
  (select-rows dataset (clojure.core/shuffle (range (row-count dataset)))))


(defn sample
  "Sample n-rows from a dataset.  Defaults to sampling *without* replacement."
  ([dataset replacement? n]
   (let [row-count (row-count dataset)
         n (long n)]
     (-> (if replacement?
           (select-rows dataset (repeatedly n #(rand-int row-count)))
           (select-rows dataset (take (min n row-count)
                                      (clojure.core/shuffle (range row-count)))))
         (vary-meta clojure.core/assoc :print-index-range (range n)))))
  ([dataset n]
   (sample dataset false n))
  ([dataset]
   (sample dataset false 5)))


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


(defn- ->column-seq
  [c-seq-or-ds]
  (if (sequential? c-seq-or-ds)
    c-seq-or-ds
    (columns c-seq-or-ds)))


(defn append-columns
  [dataset column-seq]
  (new-dataset (dataset-name dataset)
               (meta dataset)
               (clojure.core/concat (columns dataset)
                                    (->column-seq column-seq))))


(defn filter-dataset
  "Filter the columns of the dataset returning a new dataset.  This pathway is
  designed to work with the tech.v3.dataset.column-filters namespace.

  * If filter-fn-or-ds is a dataset, it is returned.
  * If filter-fn-or-ds is sequential, then select-columns is called.
  * If filter-fn-or-ds is :all, all columns are returned
  * If filter-fn-or-ds is an instance of IFn, the dataset is passed into it."
  [dataset filter-fn-or-ds]
  (cond
    (ds-impl/dataset? filter-fn-or-ds)
    filter-fn-or-ds
    (sequential? filter-fn-or-ds)
    (select-columns dataset filter-fn-or-ds)
    (or (nil? filter-fn-or-ds)
        (= :all filter-fn-or-ds))
    dataset
    (instance? IFn filter-fn-or-ds)
    (filter-fn-or-ds dataset)
    :else
    (errors/throwf "Unrecoginzed filter mechanism: %s" filter-fn-or-ds)))


(defn update
  "Update this dataset.  Filters this dataset into a new dataset,
  applies update-fn, then merges the result into original dataset.

  This pathways is designed to work with the tech.v3.dataset.column-filters namespace.


  * `filter-fn-or-ds` is a generalized parameter.  May be a function,
     a dataset or a sequence of columns
  *  update-fn must take the dataset as the first argument and must return
     a dataset.

```clojure
(ds/bind-> (ds/->dataset dataset) ds
           (ds/remove-column \"Id\")
           (ds/update cf/string ds/replace-missing-value \"NA\")
           (ds/update-elemwise cf/string #(get {\"\" \"NA\"} % %))
           (ds/update cf/numeric ds/replace-missing-value 0)
           (ds/update cf/boolean ds/replace-missing-value false)
           (ds/update-columnwise (cf/union (cf/numeric ds) (cf/boolean ds))
                                 #(dtype/elemwise-cast % :float64)))
```
"
  [lhs-ds filter-fn-or-ds update-fn & args]
  (let [filtered-ds (filter-dataset lhs-ds filter-fn-or-ds)]
    (merge lhs-ds (apply update-fn filtered-ds args))))


(defn update-columnwise
  "Call update-fn on each column of the dataset.  Returns the dataset.
  See arguments to update"
  [dataset filter-fn-or-ds cwise-update-fn & args]
  (update dataset filter-fn-or-ds
          (fn [filtered-ds]
            (reduce (fn [filtered-ds filter-col]
                      (assoc filtered-ds (:name (meta filter-col))
                             (apply cwise-update-fn filter-col args)))
                    filtered-ds
                    (columns filtered-ds)))))


(defn replace-missing-value
  ([dataset filter-fn-or-ds scalar-value]
   (update-columnwise dataset filter-fn-or-ds
                      (fn [col]
                        (let [missing (ds-col/missing col)]
                          (if (.isEmpty missing)
                            col
                            (update-reader/update-reader
                             col (bitmap/bitmap-value->bitmap-map
                                  missing scalar-value)))))))
  ([dataset scalar-value]
   (replace-missing-value dataset identity scalar-value)))


(defn update-elemwise
  "Replace all elements in selected columns by calling selected function on each
  element.  column-name-seq must be a sequence of column names if provided.
  filter-fn-or-ds has same rules as update.  Implicitly clears the missing set so
  function must deal with type-specific missing values correctly.
  Returns new dataset"
  ([dataset filter-fn-or-ds map-fn]
   (update-columnwise dataset filter-fn-or-ds
                      #(dtype/emap map-fn (dtype/elemwise-datatype %) %)))
  ([dataset map-fn]
   (update-elemwise dataset identity map-fn)))


(defn assoc-metadata
  "Set metadata across a set of columns."
  [dataset filter-fn-or-ds k v & args]
  (let [n-args (count args)
        _ (errors/when-not-error (== 0 (rem n-args 2))
            "Assoc must have an even number of arguments")]
    (apply update-columnwise dataset filter-fn-or-ds
           vary-meta assoc k v args)))


(defn categorical->number
  "Convert string columns to numeric columns.
  See tech.v3.dataset.categorical/fit-categorical-map."
  ([dataset filter-fn-or-ds]
   (categorical->number dataset filter-fn-or-ds nil nil))
  ([dataset filter-fn-or-ds table-args]
   (categorical->number dataset filter-fn-or-ds table-args nil))
  ([dataset filter-fn-or-ds table-args result-datatype]
   (update dataset filter-fn-or-ds
           (fn [filtered-ds]
             (reduce
              (fn [filtered-ds colname]
                (let [fit-data
                      (ds-cat/fit-categorical-map
                       filtered-ds colname table-args result-datatype)]
                  (ds-cat/transform-categorical-map filtered-ds fit-data)))
              filtered-ds
              (column-names filtered-ds))))))


(defn categorical->one-hot
  "Convert string columns to numeric columns.
  See tech.v3.dataset.categorical/fit-one-hot"
  ([dataset filter-fn-or-ds]
   (categorical->one-hot dataset filter-fn-or-ds nil nil))
  ([dataset filter-fn-or-ds table-args]
   (categorical->one-hot dataset filter-fn-or-ds table-args nil))
  ([dataset filter-fn-or-ds table-args result-datatype]
   (let [filtered-ds (filter-dataset dataset filter-fn-or-ds)
         filtered-cnames (column-names filtered-ds)]
     (merge (apply dissoc dataset filtered-cnames)
            (reduce
             (fn [filtered-ds colname]
               (let [fit-data
                     (ds-cat/fit-one-hot
                      filtered-ds colname table-args result-datatype)]
                 (ds-cat/transform-one-hot filtered-ds fit-data)))
             filtered-ds
             (column-names filtered-ds))))))


(defn column-cast
  "Cast a column to a new datatype.  This is never a lazy operation.  If the old
  and new datatypes match and no cast-fn is provided then dtype/clone is called
  on the column.

  colname may be a scalar or a tuple of [src-col dst-col].

  datatype may be a datatype enumeration or a tuple of
  [datatype cast-fn] where cast-fn may return either a new value,
  :tech.v3.dataset.parse/missing, or :tech.v3.dataset.parse/parse-failure.
  Exceptions are propagated to the caller.  The new column has at least the
  existing missing set (if no attempt returns :missing or :cast-failure).
  :cast-failure means the value gets added to metadata key :unparsed-data
  and the index gets added to :unparsed-indexes.


  If the existing datatype is string, then tech.v3.datatype.column/parse-column
  is called.

  Casts between numeric datatypes need no cast-fn but one may be provided.
  Casts to string need no cast-fn but one may be provided.
  Casts from string to anything will call tech.v3.dataset.column/parse-column."
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
             col-reader (dtype/->reader src-col)
             n-elems (dtype/ecount col-reader)
             unparsed-data (ArrayList.)
             unparsed-indexes (bitmap/->bitmap)
             result (if (= dst-dtype :string)
                      (str-table/make-string-table n-elems)
                      (dtype/make-container :jvm-heap dst-dtype n-elems))
             res-writer (dtype/->writer result)
             missing-val (col-base/datatype->missing-value dst-dtype)]
         (pfor/parallel-for
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



(defn columnwise-concat
  "Given a dataset and a list of columns, produce a new dataset with
  the columns concatenated to a new column with a :column column indicating
  which column the original value came from.  Any columns not mentioned in the
  list of columns are duplicated.

  Example:
```clojure
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
```

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
         leftover-columns (->> (vals dataset)
                               (remove (comp colname-set
                                             ds-col/column-name)))]
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
                                                  (dtype/const-reader col-name row-count))
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
```clojure
  {... - columns not in colname-seq
   :value - value from one of the value columns
   :label - name of the column the value came from
  }
```"
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

```clojure
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
```

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
         (pfor/indexed-map-reduce
          (dtype/ecount coldata)
          (fn [^long start-idx ^long len]
            (let [container (col-base/make-container result-datatype)
                  indexes (dtype/make-list :int64)
                  ^PrimitiveList idx-container
                  (when idx-colname
                    (dtype/make-list :int32))]
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
                              (.addLong indexes idx)
                              (when idx-colname
                                (.addLong idx-container inner-idx))
                              (recur (.hasNext src-iter)
                                     (unchecked-inc inner-idx)))))
                        ;;Else treat value as scalar
                        (do
                          (.add container (cast-fn data-item))
                          (.addLong indexes idx)
                          (when idx-colname
                            (.addLong idx-container 0))))))))
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



(defn all-descriptive-stats-names
  "Returns the names of all descriptive stats in the order they will be returned
  in the resulting dataset of descriptive stats.  This allows easy filtering
  in the form for
  (descriptive-stats ds {:stat-names (->> (all-descriptive-stats-names)
                                          (remove #{:values :num-distinct-values}))})"
  []
  [:col-name :datatype :n-valid :n-missing
   :min :quartile-1 :mean :mode :median :quartile-3 :max
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
                             (remove #{:median :values :n-values
                                       :quartile-1 :quartile-3 :histogram})))
         numeric-stats (set/intersection
                        #{:min :quartile-1 :mean :median
                          :quartile-3
                          :max :standard-deviation :skew}
                        (set stat-names))
         stats-ds
         (->> (->dataset dataset)
              (columns)
              (pmap (fn [ds-col]
                      (let [n-missing (dtype/ecount (ds-col/missing ds-col))
                            n-valid (- (dtype/ecount ds-col)
                                       n-missing)
                            col-dtype (dtype/get-datatype ds-col)]
                        (merge
                         {:col-name (ds-col/column-name ds-col)
                          :datatype col-dtype
                          :n-valid n-valid
                          :n-missing n-missing}
                         (cond
                           (dtype-dt/datetime-datatype? col-dtype)
                           (dtype-dt/millisecond-descriptive-statistics
                            numeric-stats
                            nil
                            ds-col)
                           (and (not (:categorical? (meta ds-col)))
                                (casting/numeric-type? col-dtype))
                           (dfn/descriptive-statistics numeric-stats ds-col)
                           :else
                           (let [histogram (->> (frequencies ds-col)
                                                (clojure.core/sort-by second >))
                                 max-categorical-values (or (:n-categorical-values
                                                             options) 21)]
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
         (set-dataset-name (str (dataset-name dataset) ": descriptive-stats"))
         ;;Always print all the columns after descriptive stats
         (vary-meta clojure.core/assoc
                    :print-index-range (range (column-count dataset)))))))


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


(defn dataset->smile-dataframe
  "Convert a dataset to a smile dataframe.

  This operation may clone columns if they aren't backed by java heap arrays.
  See ensure-array-backed

  It is important to note that smile supports a subset of the functionality in
  tech.v3.dataset.  One difference is smile columns have string column names and
  have no missing set.

  Returns a smile.data.DataFrame"
  ^smile.data.DataFrame [ds]
  (-> (ensure-array-backed ds)
      (smile-data/dataset->dataframe)))
