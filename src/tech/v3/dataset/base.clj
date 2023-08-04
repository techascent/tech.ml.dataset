(ns ^:no-doc tech.v3.dataset.base
  "Base dataset bare bones implementation.  Methods here are used in further
  implementations and they are exposed to users."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.unary-pred :as un-pred]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.parallel.for :refer [pmap] :as pfor]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.set :as set])
  (:import [tech.v3.datatype ObjectReader PackedLocalDate]
           [tech.v3.dataset.impl.dataset Dataset]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset.string_table StringTable]
           [tech.v3.dataset Text]
           [java.util List LinkedHashMap Map Arrays HashMap
            ArrayList LinkedHashSet Map$Entry]
           [java.util.function LongConsumer]
           [ham_fisted IMutList MapForward Reductions]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IFn])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth reverse pmap]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn set-dataset-name
  [dataset ds-name]
  (vary-meta dataset assoc :name ds-name))


(defn row-count
  ^long [dataset-or-col]
  (if dataset-or-col
    (ds-proto/row-count dataset-or-col)
    0))


(defn column-count
  ^long [dataset]
  (if dataset
    (ds-proto/column-count dataset)
    0))


(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (when dataset (vals dataset)))


(defn column
  [dataset colname]
  (ds-proto/column dataset colname))


(defn column-names
  "In-order sequence of column names"
  [dataset]
  (some->> dataset
           vals
           (map ds-proto/column-name)))


(defn has-column?
  [dataset column-name]
  (some-> dataset (contains? column-name)))


(defn columns-with-missing-seq
  "Return a sequence of:
```clojure
  {:column-name column-name
   :missing-count missing-count
  }
```
  or nil of no columns are missing data."
  [dataset]
  (->> (columns dataset)
       (map (fn [col]
              (let [missing-count (dtype/ecount (ds-col/missing col))]
                (when-not (= 0 missing-count)
                  {:column-name (ds-col/column-name col)
                   :missing-count missing-count}))))
       (remove nil?)
       seq))


(defn add-column
  "Add a new column. Error if name collision"
  [dataset column]
  (if dataset
    (assoc dataset (ds-proto/column-name column) column)
    (ds-impl/new-dataset [column])))


(defn new-column
  "Create a new column from some values"
  [dataset column-name values]
  (assoc (or dataset (ds-impl/empty-dataset)) column-name values))


(defn remove-column
  "Same as:

```clojure
(dissoc dataset col-name)
```"
  [dataset col-name]
  (dissoc dataset col-name))


(declare select-columns)


(defn remove-columns
  "Remove columns indexed by column name seq or column filter function.
  For example:

```clojure
  (remove-columns DS [:A :B])
  (remove-columns DS cf/categorical)
```"
  [dataset colname-seq-or-fn]
  (let [colname-seq (set (if (fn? colname-seq-or-fn)
                           (column-names (colname-seq-or-fn dataset))
                           colname-seq-or-fn))]
    (select-columns dataset
                    (->> (column-names dataset)
                         (clojure.core/filter (complement colname-seq))))))


(defn drop-columns
  "Same as remove-columns. Remove columns indexed by column name seq or
  column filter function.
  For example:

  ```clojure
  (drop-columns DS [:A :B])
  (drop-columns DS cf/categorical)
  ```"
  [dataset colname-seq-or-fn]
  (when dataset
    (remove-columns dataset colname-seq-or-fn)))


(defn update-column
  "Update a column returning a new dataset.  update-fn is a column->column
  transformation.  Error if column does not exist."
  [dataset col-name update-fn]
  (update (or dataset (ds-impl/empty-dataset)) col-name update-fn))


(defn order-column-names
  "Order a sequence of columns names so they match the order in the
  original dataset.  Missing columns are placed last."
  [dataset colname-seq]
  (let [colname-set (set colname-seq)
        ordered-columns (->> (columns dataset)
                             (map ds-col/column-name)
                             (clojure.core/filter colname-set))]
    (clojure.core/concat ordered-columns
                         (remove (set ordered-columns) colname-seq))))


(defn update-columns
  "Update a sequence of columns selected by column name seq or column selector
  function.

  For example:

  ```clojure
  (update-columns DS [:A :B] #(dfn/+ % 2))
  (update-columns DS cf/numeric #(dfn// % 2))
  ```"
  [dataset column-name-seq-or-fn update-fn]
  (errors/when-not-error
   dataset
   "No dataset passed in to update-columns.")
  (let [column-name-seq (if (fn? column-name-seq-or-fn)
                          (column-names (column-name-seq-or-fn dataset))
                          column-name-seq-or-fn)]
    (reduce (fn [dataset colname]
              (update-column dataset colname update-fn))
            dataset
            column-name-seq)))


(defn add-or-update-column
  "If column exists, replace.  Else append new column."
  ([dataset colname column]
   (assoc (or dataset (ds-impl/empty-dataset)) colname column))
  ([dataset column]
   (add-or-update-column dataset (ds-col/column-name column) column)))


(defn assoc-ds
  "If dataset is not nil, calls `clojure.core/assoc`. Else creates a new empty dataset and
  then calls `clojure.core/assoc`.  Guaranteed to return a dataset (unlike assoc)."
  [dataset cname cdata & args]
  (apply clojure.core/assoc (or dataset (ds-impl/new-dataset []))
         cname cdata args))


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
  colname-seq - one of:
    - :all - all the columns
    - sequence of column names - those columns in that order.
    - implementation of java.util.Map - column order is dictate by map iteration order
       selected columns are subsequently named after the corresponding value in the map.
       similar to `rename-columns` except this trims the result to be only the columns
       in the map.
  selection - either keyword :all, a list of indexes to select, or a list of booleans where
    the index position of each true value indicates an index to select. When providing indices,
    duplicates will select the specified index position more than once.
  "
  [dataset colname-seq selection]
  (when (and selection (dtype-proto/has-constant-time-min-max? selection))
    (let [lmin (long (dtype-proto/constant-time-min selection))
          lmax (long (dtype-proto/constant-time-max selection))]
      (errors/when-not-errorf
       (< lmax (row-count dataset))
       "Index sequence range [%d-%d] out of dataset row range [0-%d]"
       lmin lmax (dec (row-count dataset)))))
  (-> dataset
      (ds-proto/select-columns colname-seq)
      (ds-proto/select-rows selection)))


(defn select-by-index
  "Trim dataset according to this sequence of indexes.  Returns a new dataset.

  col-index and row-index - one of:

    - :all - all the columns
    - list of indexes. May contain duplicates.  Negative values will be counted from
      the end of the sequence."
  [dataset col-index row-index]
  (let [make-pos (fn [^long total ^long x] (if (neg? x) (+ x total) x))
        col-index (if (number? col-index) [col-index] col-index)
        row-index (if (number? row-index) [row-index] row-index)
        colname-seq (if (sequential? col-index)
                      (->> (dtype/emap (partial make-pos (column-count dataset)) :int64 col-index)
                           (map #(nth (column-names dataset) %)))
                      col-index)
        row-index (if (sequential? row-index)
                    (dtype/emap (partial make-pos (row-count dataset)) :int64 row-index)
                    row-index)]
    (select dataset colname-seq row-index)))

(defn unordered-select
  "Perform a selection but use the order of the columns in the existing table; do
  *not* reorder the columns based on colname-seq.  Useful when doing selection based
  on sets or persistent hash maps."
  [dataset colname-seq index-seq]
  (let [colname-seq (cond
                      (instance? Map colname-seq)
                      (->> (column-names dataset)
                           (map (fn [colname]
                                  (when-let [cn-seq (get colname-seq colname)]
                                    [colname cn-seq])))
                           (remove nil?)
                           (reduce (fn [^Map item [k v]]
                                     (.put item k v)
                                     item)
                                   (LinkedHashMap.)))
                      (= :all colname-seq)
                      colname-seq
                      :else
                      (order-column-names dataset colname-seq))]
    (select dataset colname-seq index-seq)))


(defn select-columns
  "Select columns from the dataset by:

  - seq of column names
  - column selector function
  - `:all` keyword

  For example:

  ```clojure
  (select-columns DS [:A :B])
  (select-columns DS cf/numeric)
  (select-columns DS :all)
  ```"
  [dataset colname-seq-or-fn]
  (let [colname-seq (if (fn? colname-seq-or-fn)
                          (column-names (colname-seq-or-fn dataset))
                          colname-seq-or-fn)]
    (ds-proto/select-columns dataset colname-seq)))

(defn select-columns-by-index
  "Select columns from the dataset by seq of index(includes negative) or :all.

  See documentation for `select-by-index`."
  [dataset col-index]
  (select-by-index dataset col-index :all))

(defn rename-columns
  "Rename columns using a map or vector of column names.

  Does not reorder columns; rename is in-place for maps and
  positional for vectors."
  [dataset colnames]
  (let [col-map? (map? colnames)
        col-vector? (vector? colnames)
        existing-cols (vals dataset)]

    (cond (not (or col-map? col-vector?))
          (throw (ex-info "column names must be a map or vector"
                          {:column-names-type (type colnames)}))
          (and col-vector? (not= (count existing-cols)
                                 (count colnames)))
          (throw (ex-info "rename column vector must be of equal size to current column count"
                          {:current-column-count (count existing-cols)
                           :target-column-count (count colnames)})))
    (->> (if col-map?
           (map
            (fn [col]
              (let [old-name (ds-col/column-name col)]
                (if (contains? colnames old-name)
                  (ds-col/set-name col (get colnames old-name))
                  col)))
            existing-cols)
           (map
            #(ds-col/set-name %1 %2)
            existing-cols
            colnames))
         (ds-impl/new-dataset (dataset-name dataset) (meta dataset)))))


(defn- preprocess-row-indexes
  [row-indexes ^long n-rows]
  (let [bounded-reader (fn [b]
                         (let [row-indexes (dtype/->reader b)]
                           (dtype/make-reader :int64 (dtype/ecount row-indexes)
                                              (let [val (.readLong row-indexes idx)]
                                                (if (< val 0)
                                                  (+ val n-rows)
                                                  val)))))]
    (cond
      (instance? RoaringBitmap row-indexes)
      row-indexes
      (nil? row-indexes) []
      (number? row-indexes)
      (let [row-indexes (long row-indexes)
            row-indexes (if (< row-indexes 0)
                          (+ n-rows row-indexes)
                          row-indexes)]
        [row-indexes])
      (dtype-proto/convertible-to-range? row-indexes)
      (let [r (dtype-proto/->range row-indexes nil)
            rs (long (dtype-proto/range-start r))
            ri (long (dtype-proto/range-increment r))
            re (+ rs (* ri (dtype/ecount ri)))]
        (if (or (== 0 (dtype/ecount r))
                (and (>= rs 0) (pos? re)))
          r
          (bounded-reader r)))
      :else
      (-> (case (argtypes/arg-type row-indexes)
            :scalar [row-indexes]
            :reader row-indexes
            (dtype/make-container :int64 (take n-rows row-indexes)))
          (bounded-reader)))))


(defn select-rows
  "Select rows from the dataset or column."
  ([dataset-or-col row-indexes options]
   (if (and (== 0 (row-count dataset-or-col))
            (get options :allow-empty?))
     dataset-or-col
     (ds-proto/select-rows dataset-or-col row-indexes)))
  ([dataset-or-col row-indexes]
   (select-rows dataset-or-col row-indexes nil)))


(defn drop-rows
  "Drop rows from dataset or column"
  [dataset-or-col row-indexes]
  (cond-> dataset-or-col
    (not (zero? (dtype/ecount row-indexes)))
    (select-rows (set/difference
                  (bitmap/->bitmap 0 (row-count dataset-or-col))
                  (if (instance? RoaringBitmap row-indexes)
                    row-indexes
                    (col-impl/simplify-row-indexes
                     (row-count dataset-or-col)
                     row-indexes))))))


(defn remove-rows
  "Same as drop-rows."
  [dataset-or-col row-indexes]
  (drop-rows dataset-or-col row-indexes))


(defn missing
  "Given a dataset or a column, return the missing set as a roaring bitmap"
  ^RoaringBitmap [dataset-or-col]
  (when dataset-or-col
    (ds-proto/missing dataset-or-col)))


(defn drop-missing
  "Remove missing entries by simply selecting out the missing indexes."
  ([dataset-or-col]
   (drop-rows dataset-or-col (missing dataset-or-col)))
  ([ds colname]
   (drop-rows ds (missing (column ds colname)))))


(defn select-missing
  "Remove missing entries by simply selecting out the missing indexes"
  [dataset-or-col]
  (select-rows dataset-or-col (missing dataset-or-col)))


(defn reverse-rows
  "Reverse the rows in the dataset or column."
  [dataset-or-col]
  (cond-> dataset-or-col
    (not-empty dataset-or-col)
    (select-rows (hamf/range (unchecked-dec (row-count dataset-or-col)) -1 -1))))


(defn supported-column-stats
  "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."
  [dataset]
  stats/all-descriptive-stats-names)


(defn filter
  "dataset->dataset transformation.  Predicate is passed a map of
  colname->column-value."
  [dataset predicate]
  (some->> dataset
           ds-readers/mapseq-reader
           (argops/argfilter predicate)
           (select-rows dataset)))


(defn filter-column
  "Filter a given column by a predicate.  Predicate is passed column values.
  If predicate is *not* an instance of Ifn it is treated as a value and will
  be used as if the predicate is #(= value %).

  The 2-arity form of this function reads the column as a boolean reader so for
  instance numeric 0 values are false in that case as are Double/NaN, Float/NaN.  Objects are
  only false if nil?.

  Returns a dataset."
  ([dataset colname predicate]
   (when dataset
     (let [predicate (if (instance? IFn predicate)
                       predicate
                       (let [pred-dtype (dtype/get-datatype predicate)]
                         (cond
                           (casting/integer-type? pred-dtype)
                           (let [predicate (long predicate)]
                             (fn [^long arg] (== arg predicate)))
                           (casting/float-type? pred-dtype)
                           (let [predicate (double predicate)]
                             (fn [^double arg] (== arg predicate)))
                           :else
                           #(= predicate %))))]
       (->> (column dataset colname)
            (argops/argfilter predicate)
            (select dataset :all)))))
  ([dataset colname]
   (some->> (column dataset colname)
            unary-pred/bool-reader->indexes
            (select-rows dataset))))


(defn- finalize-index-map
  [idx-map ds options]
  (let [finalizer (get options :group-by-finalizer identity)]
    (dorun (hamf/pmap (fn [^Map$Entry e]
                        (.setValue e (finalizer (select-rows ds (deref (.getValue e))))))
                      idx-map))
    idx-map))


(defn group-by->indexes
  "(Non-lazy) - Group a dataset and return a map of key-fn-value->indexes where indexes
  is an in-order contiguous group of indexes."
  ([dataset key-fn options]
   (when dataset
     (argops/arggroup-by key-fn options (ds-proto/rows dataset options))))
  ([dataset key-fn] (group-by->indexes dataset key-fn nil)))



(defn group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  a map of colname->column-value.

  Options - options are passed into dtype arggroup:

  * `:group-by-finalizer` - when provided this is run on each dataset immediately after the
     rows are selected.  This can be used to immediately perform a reduction on each new
     dataset which is faster than doing it in a separate run."
  ([dataset key-fn options]
   (when dataset
     (-> (group-by->indexes dataset key-fn (merge {:skip-finalize? true} options))
         (finalize-index-map dataset options))))
  ([dataset key-fn] (group-by dataset key-fn nil)))


(defn group-by-column->indexes
    "(Non-lazy) - Group a dataset by a column return a map of column-val->indexes
  where indexes is an in-order contiguous group of indexes.

  Options are passed into dtype's arggroup method."
  ([dataset colname options]
   (when dataset (argops/arggroup options (column dataset colname))))
  ([dataset colname]
   (group-by-column->indexes dataset colname nil)))


(defn group-by-column
  "Return a map of column-value->dataset.

  * `:group-by-finalizer` - when provided this is run on each dataset immediately after the
     rows are selected.  This can be used to immediately perform a reduction on each new
     dataset which is faster than doing it in a separate run."
  ([dataset colname options]
   (when dataset
     (-> (group-by-column->indexes dataset colname (merge {:skip-finalize? true} options))
         (finalize-index-map dataset options))))
  ([dataset colname] (group-by-column dataset colname nil)))


(defn sort-by
  "Sort a dataset by a key-fn and compare-fn.

  * `key-fn` - function from map to sort value.
  * `compare-fn` may be one of:
     - a clojure operator like clojure.core/<
     - `:tech.numerics/<`, `:tech.numerics/>` for unboxing comparisons of primitive
        values.
     - clojure.core/compare
     - A custom java.util.Comparator instantiation.

  Options:

  * `:nan-strategy` - General missing strategy.  Options are `:first`, `:last`, and
    `:exception`.
  * `:parallel?` - Uses parallel quicksort when true and regular quicksort when false."
  ([dataset key-fn compare-fn & [options]]
   (when dataset
     (->> (ds-readers/mapseq-reader dataset)
          (dtype/emap key-fn :object)
          (argops/argsort compare-fn options)
          (select-rows dataset))))
  ([dataset key-fn]
   (sort-by dataset key-fn nil)))


(defn sort-by-column
  "Sort a dataset by a given column using the given compare fn.

  * `compare-fn` may be one of:
     - a clojure operator like clojure.core/<
     - `:tech.numerics/<`, `:tech.numerics/>` for unboxing comparisons of primitive
        values.
     - clojure.core/compare
     - A custom java.util.Comparator instantiation.

  Options:

  * `:nan-strategy` - General missing strategy.  Options are `:first`, `:last`, and
    `:exception`.
  * `:parallel?` - Uses parallel quicksort when true and regular quicksort when false."
  ([dataset colname compare-fn & [options]]
   (when dataset
     (->> (argops/argsort compare-fn options (packing/unpack (column dataset colname)))
          (select-rows dataset))))
  ([dataset colname]
   (sort-by-column dataset colname nil)))


(defn- non-empty-column
  [ds cname]
  (when-let [col (ds cname)]
    (when (> (dtype/ecount col) (dtype/ecount (ds-proto/missing col)))
      col)))


(defn- do-concat
  [reader-concat-fn dataset other-datasets]
  (let [datasets (->> (clojure.core/concat [dataset] (remove nil? other-datasets))
                      (remove nil?)
                      seq)]

    (cond
      (nil? datasets)
      nil
      (== 1 (count datasets))
      (first datasets)
      :else
      (let [column-names (-> (doto (LinkedHashSet.)
                               (.addAll (->> (map column-names datasets)
                                             (apply clojure.core/concat))))
                             (vec))
            column-dtypes
            (->> column-names
                 (mapv (fn [cname]
                         (->> datasets
                              (map #(non-empty-column % cname))
                              (remove nil?)
                              (map dtype/elemwise-datatype)
                              (reduce (fn ([lhs-dtype rhs-dtype]
                                           (if (identical? lhs-dtype rhs-dtype)
                                             lhs-dtype
                                             (casting/simple-operation-space
                                              (packing/unpack-datatype lhs-dtype)
                                              (packing/unpack-datatype rhs-dtype))))
                                        ([] nil)))))))]
        (->>
         (map vector column-names column-dtypes)
         (map
          (fn [[colname dtype]]
            (let [dtype (or dtype :boolean)
                  columns
                  (->> datasets
                       (map (fn [ds]
                              (if-let [retval (non-empty-column ds colname)]
                                retval
                                (let [rc (row-count ds)
                                      missing (bitmap/->bitmap (range rc))
                                      mv (col-base/dtype->missing-val-map dtype)
                                      cd (dtype/const-reader mv rc)]
                                  (ds-col/new-column
                                   #:tech.v3.dataset{:data cd
                                                     :missing missing
                                                     :name colname
                                                     :metadata {:name colname}
                                                     :datatype dtype
                                                     :force-datatype? true}))))))
                  column-values (try (reader-concat-fn dtype columns)
                                     (catch Throwable e
                                       (throw (ex-info (format
                                                        "Failed to concat column %s" colname)
                                                       {:error e}))))
                  missing
                  (->> (reduce
                        (fn [[missing offset] col]
                          [(set/union
                            missing
                            (bitmap/offset (ds-col/missing col) offset))
                           (+ (long offset) (dtype/ecount col))])
                        [(->bitmap) 0]
                        columns)
                       (first))
                  first-col (first columns)]
              (ds-col/new-column colname
                                 column-values
                                 (meta first-col)
                                 missing))))
         (ds-impl/new-dataset (dataset-name dataset)))))))


(defn check-empty
  [dataset]
  (when (not (zero? (column-count dataset)))
    dataset))



(defn concat-inplace
  "Concatenate datasets in place.  Respects missing values.  Datasets must all have the
  same columns.  Result column datatypes will be a widening cast of the datatypes."
  ([dataset & datasets]
   (do-concat #(dtype/concat-buffers %1 %2)
              dataset datasets))
  ([] nil))


(defn- coalesce-blocks!
  "Copy a sequence of blocks of countable things into a larger
  countable thing."
  [dst src-seq]
  (reduce (fn [offset src-item]
            (let [n-elems (dtype/ecount src-item)]
              (dtype/copy! src-item (dtype/sub-buffer dst offset n-elems))
              (+ (long offset) n-elems)))
          0
          src-seq)
  dst)


(defn concat-copying
  "Concatenate datasets into a new dataset copying data.  Respects missing values.
  Datasets must all have the same columns.  Result column datatypes will be a widening
  cast of the datatypes."
  ([dataset & datasets]
   (let [datasets (->> (clojure.core/concat [dataset] (remove nil? datasets))
                       (remove nil?)
                       seq)
         n-rows (long (reduce + (map row-count datasets)))]
     (do-concat #(coalesce-blocks! (dtype/make-container :jvm-heap %1 n-rows)
                                   %2)
                (first datasets) (rest datasets))))
  ([] nil))


(defn concat
  "Concatenate datasets in place using a copying-concatenation.
  See also concat-inplace as it may be more efficient for your use case if you have
  a small number (like less than 3) of datasets."
  ([dataset & datasets]
   (apply concat-copying dataset datasets))
  ([] nil))


(defn- sorted-int32-sequence
  [idx-seq]
  (let [data (dtype/->int-array idx-seq)]
    (Arrays/sort data)
    data))


(defn unique-by
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key,idx-seq->idx.  Defaults to #(first %2)."
  ([dataset {:keys [keep-fn]
            :or {keep-fn #(first %2)}
             :as _options}
    map-fn]
   (when dataset
     (->> (group-by->indexes dataset map-fn)
          (map (fn [[k v]] (keep-fn k v)))
          (sorted-int32-sequence)
          (select-rows dataset))))
  ([dataset map-fn]
   (unique-by dataset {} map-fn)))


(defn unique-by-column
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key, idx-seq->idx.  Defaults to #(first %2)."
  ([dataset
    {:keys [keep-fn]
     :or {keep-fn #(first %2)}
     :as _options}
    colname]
   (when dataset
     (->> (group-by-column->indexes dataset colname)
          (map (fn [[k v]] (keep-fn k v)))
          (sorted-int32-sequence)
          (select dataset :all))))
  ([dataset colname]
   (unique-by-column dataset {} colname)))


(defn take-nth
  [dataset n-val]
  (select dataset :all (->> (range (second (dtype/shape dataset)))
                            (clojure.core/take-nth n-val))))

(casting/add-object-datatype! :dataset Dataset)


(defn dataset->string
  ^String [ds]
  (.toString ^Object ds))


(defn ensure-array-backed
  "Ensure the column data in the dataset is stored in pure java arrays.  This is
  sometimes necessary for interop with other libraries and this operation will
  force any lazy computations to complete.  This also clears the missing set
  for each column and writes the missing values to the new arrays.

  Columns that are already array backed and that have no missing values are not
  changed and retuned.

  The postcondition is that dtype/->array will return a java array in the appropriate
  datatype for each column.

  Options:

  * `:unpack?` - unpack packed datetime types.  Defaults to true"
  ([ds {:keys [unpack?]
        :or {unpack? true}
        :as _options}]
   (reduce (fn [ds col]
             (let [colname (ds-col/column-name col)
                   col (if unpack?
                         (packing/unpack col)
                         col)]
               (assoc ds colname #:tech.v3.dataset{:data (dtype-cmc/->array col)
                                                   :missing (ds-col/missing col)
                                                   :metadata (meta col)
                                                   :force-datatype? true})))
           ds
           (columns ds)))
  ([ds]
   (ensure-array-backed ds {})))


(defn column->string-table
  ^StringTable [^Column col]
  (if-let [retval (when (instance? StringTable (.data col))
                    (.data col))]
    retval
    (throw (Exception. (format "Column %s does not contain a string table"
                               (ds-col/column-name col))))))


(defn ensure-column-string-table
  "Ensure this column is backed by a string table.
  If not, return a new column that is.
  Column must be :string datatype."
  ^StringTable [col]
  (when-not (= :string (dtype/get-datatype col))
    (throw (Exception.
            (format "Column %s does not have :string datatype"
                    (ds-col/column-name col)))))
  (if (not (instance? StringTable (.data ^Column col)))
    (str-table/string-table-from-strings col)
    (.data ^Column col)))


(defn ensure-dataset-string-tables
  "Given a dataset, ensure every string column is backed by a string table."
  [ds]
  (reduce
   (fn [ds col]
     (if (= :string (dtype/get-datatype col))
       (let [missing (ds-col/missing col)
             metadata (meta col)
             colname (:name metadata)
             str-t (ensure-column-string-table col)]
         (assoc ds (ds-col/column-name col)
                (ds-col/new-column colname str-t metadata missing)))
       ds))
   ds
   (vals ds)))


(defn- column->string-data
  [str-col]
  (let [coldata (.data ^Column str-col)
        ^StringTable str-table
        (if (instance? StringTable coldata)
          coldata
          (str-table/string-table-from-strings coldata))
        ^IMutList str-data-buf (dtype/make-container :list :int8 0)
        ^IMutList offset-buf (dtype/make-container :list :int32 0)
        data-ary (dtype-cmc/->array (dtype/->array-buffer (.data str-table)))]
    (hamf-rf/consume!
     #(do
        (.addLong offset-buf (.size str-data-buf))
        (when %
          (let [str-bytes (.getBytes ^String %)]
            (.addAllReducible str-data-buf (hamf/->random-access str-bytes)))))
     (.int->str str-table))
    ;;One extra long makes deserializing a bit less error prone.
    (.addLong offset-buf (.size str-data-buf))
    {:string-data (dtype/->array str-data-buf)
     :offsets (dtype/->array offset-buf)
     ;;Between version 1 and 2 we changed the string table to be just
     ;;an array of strings intead of a hashmap.
     ;;For version 3, we had to serialize string data into byte arrays
     ;;as java string serialization is considered a security risk
     :version 3
     :entries data-ary}))


(defn- string-data->column-data
  [{:keys [string-table entries version] :as entry}]
  (let [int-data (dyn-int-list/make-from-container entries)]
    (cond
      ;;Version
      (= version 3)
      (let [{:keys [string-data offsets]} entry
            string-data ^bytes string-data
            offsets (dtype/->buffer offsets)
            n-elems (dec (.lsize offsets))
            ^IMutList int->str
            (->> (dtype/make-reader
                  :string n-elems
                  (let [start-off (.readLong offsets idx)
                        end-off (.readLong offsets (inc idx))]
                    (String. string-data start-off (- end-off start-off))))
                 (dtype/make-container :list :string))
            str->int (HashMap. (dtype/ecount int->str))]
        (dotimes [idx n-elems]
          (.put str->int (.get int->str idx) idx))
        (StringTable. int->str str->int int-data))
      (= version 2)
      (let [^List int->str (dtype-list/wrap-container string-table)
            str->int (HashMap. (dtype/ecount int->str))
            n-elems (.size int->str)]
        ;;build reverse map
        (dotimes [idx n-elems]
          (.put str->int (.get int->str idx) idx))
        (StringTable. int->str str->int int-data))
      :else
      (let [^Map str->int string-table
            int->str-ary-list (ArrayList. (count str->int))
            _ (doseq [[k v] str->int]
                (let [v (unchecked-int v)
                      list-size (.size int->str-ary-list)]
                  (dotimes [_idx (max 0 (- (inc v) list-size))]
                    (.add int->str-ary-list 0))
                  (.set int->str-ary-list v k)))]
        (StringTable. int->str-ary-list str->int int-data)))))


(defn- column->text-data
  [coldata]
  (let [^IMutList str-data-buf (dtype/make-container :list :int8 0)
        ^IMutList offset-buf (dtype/make-container :list :int64 0)]
    (pfor/consume!
     #(do
        (.addLong offset-buf (.size str-data-buf))
        (when %
          (let [str-bytes (.getBytes ^String (str %))]
            (.addAll str-data-buf (dtype/->buffer str-bytes)))))
     coldata)
    (.addLong offset-buf (.size str-data-buf))
    {:string-data (dtype/->array str-data-buf)
     :offsets (dtype/->array offset-buf)}))


(defn- text-data->column-data
  [{:keys [string-data offsets]} missing]
  (let [n-elems (dec (dtype/ecount offsets))
        string-data (dtype/->buffer string-data)
        offsets (dtype/->buffer offsets)
        ^RoaringBitmap missing missing]
    (-> (reify ObjectReader
          (elemwiseDatatype [rdr] :text)
          (lsize [rdr] n-elems)
          (readObject [rdr idx]
            (let [cur-off (.readLong offsets idx)
                  next-off (.readLong offsets (inc idx))]
              (when-not (.contains missing idx)
                (Text. (String. (dtype/->byte-array
                                 (dtype/sub-buffer string-data cur-off (- next-off cur-off)))))))))
        (dtype/clone))))


(def dataset-data-version 2)


(defn column->data
  "Given a column return a pure data representation of that column.
  The reverse operation is data->column."
  [col]
  (let [metadata (meta col)
        coldata (packing/pack (ds-proto/column-buffer col))
        dtype (dtype/elemwise-datatype coldata)
        ne (dtype/ecount col)]
    {:metadata (assoc metadata :datatype dtype)
     :missing (dtype/->array (if (< ne Integer/MAX_VALUE)
                               :int32
                               :int64)
                             (bitmap/->random-access (ds-col/missing col)))
     :name (:name metadata)
     :version dataset-data-version
     :data (cond
             (= :string dtype)
             (column->string-data col)
             (= :text dtype)
             (column->text-data col)
             ;;Nippy doesn't handle object arrays or arraylists
             ;;very well.
             (= :object (casting/flatten-datatype dtype))
             (vec col)
             ;;Store the data as a jvm-native array
             :else
             (dtype-cmc/->array dtype coldata))}))


(defn dataset->data
  "Convert a dataset to a pure clojure datastructure.  Returns a map with two keys:
  {:metadata :columns}.
  :columns is a vector of column definitions appropriate for passing directly back
  into new-dataset.
  A column definition in this case is a map of {:name :missing :data :metadata}."
  [ds]
  {:metadata (meta ds)
   :version dataset-data-version
   :columns
   (->>
    (columns ds)
    (pmap column->data)
    (vec))})


(defn- data->column-data
  "Given the result of column->data, produce a new column data description.  This
  description can be added via `add-column` - to produce a new column."
  ([version {:keys [metadata missing data]}]
   (let [{:keys [datatype name]} metadata
         missing (bitmap/->bitmap missing)]
     #:tech.v3.dataset
      {:name name
       :missing missing
       :force-datatype? true
       :metadata metadata
       :data (case datatype
               :string
               (string-data->column-data data)
               :text
               (text-data->column-data data missing)
               (if (identical? :object (casting/flatten-datatype datatype))
                 (dtype/elemwise-cast data datatype)
                 (if (= version 2)
                   (array-buffer/array-buffer data datatype)
                  ;; Convert from packed local dates of old version to new
                  ;; version.
                   (if (= datatype :packed-local-date)
                     (-> (dtype/emap #(when-not (== 0 (long %))
                                        (PackedLocalDate/asLocalDate
                                         (unchecked-int %)))
                                     :local-date data)
                         (packing/pack)
                         (dtype/clone))
                     (dtype/elemwise-cast data datatype)))))}))
  ([coldata]
   (data->column-data (:version coldata) coldata)))


(defn data->column
  "Convert a data-ized dataset created via dataset->data back into a
  full dataset"
  ([version data]
   (col-impl/new-column (data->column-data version data)))
  ([data]
   (col-impl/new-column (data->column-data data))))


(defn data->dataset
  "Convert a data-ized dataset created via dataset->data back into a
  full dataset"
  [{:keys [metadata version columns]
    :or {version 1}
    :as _input}]
  (->> columns
       (map (partial data->column-data version))
       (ds-impl/new-dataset {:dataset-name (:name metadata)} metadata)))


(defn extend-with-empty
  "Extend a dataset with empty values"
  [ds n-empty]
  (->> (columns ds)
       (map #(ds-col/extend-column-with-empty % n-empty))
       (ds-impl/new-dataset (meta ds))))
