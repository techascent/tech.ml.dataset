(ns tech.ml.dataset.base
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.builtin-op-providers :as builtin-op-providers]
            [tech.v2.datatype.readers.concat :as reader-concat]
            [tech.v2.datatype.bitmap :refer [->bitmap]]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.io :as io]
            [tech.parallel.require :as parallel-req]
            [tech.parallel.for :as parallel-for]
            [tech.parallel.utils :as par-util])
  (:import [java.io InputStream]
           [tech.v2.datatype ObjectReader]
           [java.util List HashSet]
           [org.roaringbitmap RoaringBitmap]
           [it.unimi.dsi.fastutil.longs LongArrayList])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth]))


(set! *warn-on-reflection* true)


(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn set-dataset-name
  [dataset ds-name]
  (ds-proto/set-dataset-name dataset ds-name))

(defn row-count
  ^long [dataset]
  (second (dtype/shape dataset)))

(defn ds-row-count
  [dataset]
  (row-count dataset))

(defn column-count
  ^long [dataset]
  (first (dtype/shape dataset)))

(defn ds-column-count
  [dataset]
  (column-count dataset))

(defn metadata
  [dataset]
  (ds-proto/metadata dataset))

(defn set-metadata
  [dataset meta-map]
  (ds-proto/set-metadata dataset meta-map))

(defn maybe-column
  "Return either column if exists or nil."
  [dataset column-name]
  (ds-proto/maybe-column dataset column-name))


(defn column
  "Return the column or throw if it doesn't exist."
  [dataset column-name]
  (ds-proto/column dataset column-name))

(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (ds-proto/columns dataset))


(defn column-map
  "clojure map of column-name->column"
  [datatypes]
  (->> (ds-proto/columns datatypes)
       (map (juxt ds-col/column-name identity))
       (into {})))


(defn column-names
  "In-order sequence of column names"
  [dataset]
  (->> (ds-proto/columns dataset)
       (map ds-col/column-name)))


(defn columns-with-missing-seq
  "Return a sequence of:
  {:column-name column-name
   :missing-count missing-count
  }
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
  (ds-proto/add-column dataset column))


(defn new-column
  "Create a new column from some values."
  [dataset column-name values]
  (->> (if (ds-col/is-column? values)
         (ds-col/set-name values column-name)
         (ds-col/new-column column-name values))
       (add-column dataset)))


(defn remove-column
  "Fails quietly"
  [dataset col-name]
  (ds-proto/remove-column dataset col-name))


(defn remove-columns
  [dataset colname-seq]
  (reduce ds-proto/remove-column dataset colname-seq))


(defn update-column
  "Update a column returning a new dataset.  update-fn is a column->column
  transformation.  Error if column does not exist."
  [dataset col-name update-fn]
  (ds-proto/update-column dataset col-name update-fn))


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
  "Update a sequence of columns."
  [dataset column-name-seq update-fn]
  (reduce (fn [dataset colname]
            (update-column dataset colname update-fn))
          dataset
          column-name-seq))


(defn add-or-update-column
  "If column exists, replace.  Else append new column."
  ([dataset colname column]
   (ds-proto/add-or-update-column dataset colname column))
  ([dataset column]
   (add-or-update-column dataset (ds-col/column-name column) column)))


(defn rename-columns
  "Rename a map of columns."
  [dataset colname-map]
  (reduce-kv (fn [dataset col-name new-col-name]
               (-> dataset
                   (add-or-update-column new-col-name (dataset col-name))
                   (remove-column col-name)))
             dataset
             colname-map))


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
colname-seq - either keyword :all or list of column names with no duplicates.
index-seq - either keyword :all or list of indexes.  May contain duplicates."
  [dataset colname-seq index-seq]
  (ds-proto/select dataset colname-seq index-seq))


(defn unordered-select
  "Perform a selection but use the order of the columns in the existing table; do
  *not* reorder the columns based on colname-seq.  Useful when doing selection based
  on sets."
  [dataset colname-seq index-seq]
  (select dataset
          (order-column-names dataset colname-seq)
          index-seq))


(defn select-columns
  [dataset col-name-seq]
  (select dataset col-name-seq :all))


(defn index-value-seq
  "Get a sequence of tuples:
  [idx col-value-vec]

Values are in order of column-name-seq.  Duplicate names are allowed and result in
duplicate values."
  [dataset & [reader-options]]
  (->> (ds-proto/columns dataset)
       (map #(dtype-proto/->reader % reader-options))
       ;;produce row-wise vectors of data
       (apply map vector)
       ;;Index them.
       (map-indexed vector)))


(par-util/export-symbols tech.ml.dataset.print
                         value-reader
                         mapseq-reader)


(defn supported-column-stats
  "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."
  [dataset]
  (ds-proto/supported-column-stats dataset))


(defn from-prototype
  "Create a new dataset that is the same type as this one but with a potentially
different table name and column sequence.  Take care that the columns are all of
the correct type."
  [dataset table-name column-seq]
  (ds-proto/from-prototype dataset table-name column-seq))


(defn filter
  "dataset->dataset transformation.  Predicate is passed a map of
  colname->column-value."
  [predicate dataset & [column-name-seq]]
  (->> (or column-name-seq (column-names dataset))
       (select-columns dataset)
       (mapseq-reader)
       (dfn/argfilter predicate)
       (select dataset :all)))


(defn ds-filter
  "Legacy method.  Please see filter"
  [predicate dataset & [column-name-seq]]
  (filter predicate dataset column-name-seq))


(defn filter-column
  "Filter a given column by a predicate.  Predicate is passed column values.
  truthy values are kept.  Returns a dataset."
  [predicate colname dataset]
  (->> (column dataset colname)
       (dfn/argfilter predicate)
       (select dataset :all)))


(defn ds-filter-column
  "Legacy method.  Please see filter-column"
  [predicate colname dataset]
  (->> (column dataset colname)
       (dfn/argfilter predicate)
       (select dataset :all)))


(defn group-by->indexes
  [key-fn dataset & [column-name-seq]]
  (->> (or column-name-seq (column-names dataset))
       (select-columns dataset)
       (mapseq-reader)
       (dfn/arggroup-by key-fn)))


(defn group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  a map of colname->column-value.  Selecting which columns are used in the key-fn
  using column-name-seq is optional but will greatly improve performance."
  [key-fn dataset & [column-name-seq]]
  (->> (group-by->indexes key-fn dataset column-name-seq)
       (map (fn [[k v]] [k (select dataset :all v)]))
       (into {})))


(defn ds-group-by
  "Legacy method. Please see group-by"
  [key-fn dataset & [column-name-seq]]
  (group-by key-fn dataset column-name-seq))


(defn group-by-column->indexes
  [colname dataset]
  (->> (column dataset colname)
       (dtype/->reader)
       (dfn/arggroup-by identity)))


(defn group-by-column
  "Return a map of column-value->dataset."
  [colname dataset]
  (->> (group-by-column->indexes colname dataset)
       (map (fn [[k v]]
              [k (-> (select dataset :all v)
                     (set-dataset-name k))]))
       (into {})))

(defn ds-group-by-column
  "Legacy method.  Please see group-by-column"
  [colname dataset]
  (group-by-column colname dataset))


(defn sort-by
  "Sort a dataset by a key-fn and compare-fn.  Uses clojure's sort-by under the
  covers."
  ([key-fn compare-fn dataset column-name-seq]
   (let [^List data (->> (or column-name-seq (column-names dataset))
                         (select-columns dataset)
                         (mapseq-reader))
         n-elems (.size data)]
     (->> (range n-elems)
          (clojure.core/sort-by #(key-fn (.get data %)) compare-fn)
          (select dataset :all))))
  ([key-fn compare-fn dataset]
   (sort-by key-fn compare-fn dataset :all))
  ([key-fn dataset]
   (sort-by key-fn compare dataset :all)))


(defn ds-sort-by
  "Legacy method.  Please see sort-by"
  ([key-fn compare-fn dataset column-name-seq]
   (sort-by key-fn compare-fn dataset column-name-seq))
  ([key-fn compare-fn dataset]
   (sort-by key-fn compare-fn dataset :all))
  ([key-fn dataset]
   (sort-by key-fn compare dataset :all)))


(defn sort-by-column
  "Sort a dataset by a given column using the given compare fn."
  ([colname compare-fn dataset]
   (let [^List data (dtype/->reader (dataset colname))
         n-elems (.size data)]
     (->> (range n-elems)
          (clojure.core/sort-by #(.get data %) compare-fn)
          (select dataset :all))))
  ([colname dataset]
   (sort-by-column colname compare dataset)))


(defn ds-sort-by-column
  "Legacy method.  Please see sort by column."
  ([colname compare-fn dataset]
   (sort-by-column colname compare-fn dataset))
  ([colname dataset]
   (sort-by-column colname dataset)))


(defn concat
  "Concatenate datasets.  Respects missing values.  Datasets must all have the same
  columns."
  [dataset & other-datasets]
  (let [datasets (->> (clojure.core/concat [dataset] (remove nil? other-datasets))
                      (remove nil?)
                      seq)]
    (when-let [dataset (first datasets)]
      (let [column-list
            (->> datasets
                 (mapcat (fn [dataset]
                           (->> (columns dataset)
                                (mapv (fn [col]
                                        (assoc (ds-col/metadata col)
                                               :column
                                               col
                                               :table-name (dataset-name dataset)))))))
                 (clojure.core/group-by :name))
            label-map (->> datasets
                           (map (comp :label-map metadata))
                           (apply merge))]
        (when-not (= 1 (count (->> (vals column-list)
                                   (map count)
                                   distinct)))
          (throw (ex-info "Dataset is missing a column" {})))
        (->> column-list
             (map (fn [[colname columns]]
                    (let [columns (map :column columns)
                          column-values (reader-concat/concat-readers columns)
                          missing
                          (->> (reduce
                                (fn [[missing offset] col]
                                  [(dtype/set-or missing
                                                 (dtype/set-offset (ds-col/missing col)
                                                                   offset))
                                   (+ offset (dtype/ecount col))])
                                [(->bitmap) 0]
                                columns)
                               (first))
                          first-col (first columns)]
                      (ds-col/new-column colname
                                         column-values
                                         (ds-col/metadata first-col)
                                         missing))))
             (ds-proto/from-prototype dataset (dataset-name dataset))
             (#(set-metadata % {:label-map label-map})))))))


(defn ds-concat
  "Legacy method.  Please see concat"
  [dataset & other-datasets]
  (apply concat dataset other-datasets))

(defn default-unique-by-keep-fn
  [argkey idx-seq]
  (first idx-seq))


(defn unique-by
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key,idx-seq->idx-seq.  Defaults to first."
  [map-fn dataset & {:keys [column-name-seq keep-fn]
                    :or {keep-fn default-unique-by-keep-fn}}]
  (->> (group-by->indexes map-fn dataset column-name-seq)
       (map (fn [[k v]] (keep-fn k v)))
       (select dataset :all)))


(defn unique-by-column
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key, idx-seq->idx0seq.  Defaults to first."
  [colname dataset & {:keys [keep-fn]
                      :or {keep-fn default-unique-by-keep-fn}}]
  (->> (group-by-column->indexes colname dataset)
       (map (fn [[k v]] (keep-fn k v)))
       (select dataset :all)))

(defn- perform-aggregation
  [numeric-aggregate-fn
   boolean-aggregate-fn
   default-aggregate-fn
   column-seq]
  (let [col-dtype (dtype/get-datatype (first column-seq))]
    (->
     (cond
       (casting/numeric-type? col-dtype)
       (mapv numeric-aggregate-fn column-seq)
       (= :boolean col-dtype)
       (mapv boolean-aggregate-fn column-seq)
       :else
       (mapv default-aggregate-fn column-seq))
     (dtype/->reader ( (= col-dtype :boolean)
                       :int64
                       col-dtype)))))


(defn- finish-aggregate-by
  [dataset index-groups
   numeric-aggregate-fn
   boolean-aggregate-fn
   default-aggregate-fn
   count-column-name]
  (let [index-sequences (->> index-groups
                             (map (fn [[_ v]]
                                    (int-array v))))
        count-column-name (or count-column-name
                              (if (keyword? (first (column-names dataset)))
                                :index-counts
                                "index-counts"))
        new-ds
        (->> (columns dataset)
             (map
              (fn [column]
                (->> index-sequences
                     (map (partial ds-col/select column))
                     (perform-aggregation numeric-aggregate-fn
                                          boolean-aggregate-fn
                                          default-aggregate-fn))))
             (ds-proto/from-prototype dataset (dataset-name dataset)))]
    (add-or-update-column new-ds count-column-name
                          (long-array (mapv count index-sequences)))))

(defn count-true
  [boolean-seq]
  (-> (dtype/->reader boolean-seq :int64)
      (dfn/reduce-+)))


(defn aggregate-by
  "Group the dataset by map-fn, then aggregate by the aggregate fn.
  Returns aggregated datatset.
  :aggregate-fn - passed a sequence of columns and must return a new column
  with the same number of entries as the count of the column sequences."
  [map-fn dataset & {:keys [column-name-seq
                            numeric-aggregate-fn
                            boolean-aggregate-fn
                            default-aggregate-fn
                            count-column-name]
                     :or {numeric-aggregate-fn dfn/reduce-+
                          boolean-aggregate-fn count-true
                          default-aggregate-fn first}}]
  (finish-aggregate-by dataset
                       (group-by->indexes map-fn dataset column-name-seq)
                       numeric-aggregate-fn
                       boolean-aggregate-fn
                       default-aggregate-fn
                       count-column-name))


(defn aggregate-by-column
  "Group the dataset by map-fn, then aggregate by the aggregate fn.
  Returns aggregated datatset.
  :aggregate-fn - passed a sequence of columns and must return a new column
  with the same number of entries as the count of the column sequences."
  [colname dataset & {:keys [numeric-aggregate-fn
                             boolean-aggregate-fn
                             default-aggregate-fn
                             count-column-name]
                      :or {numeric-aggregate-fn dfn/reduce-+
                           boolean-aggregate-fn count-true
                           default-aggregate-fn first}}]
  (finish-aggregate-by dataset
                       (group-by-column->indexes colname dataset)
                       numeric-aggregate-fn
                       boolean-aggregate-fn
                       default-aggregate-fn
                       count-column-name))


(defn take-nth
  [n-val dataset]
  (select dataset :all (->> (range (second (dtype/shape dataset)))
                            (clojure.core/take-nth n-val))))


(defn ds-take-nth
  "Legacy method.  Please see take-nth"
  [n-val dataset]
  (take-nth n-val dataset))


(defn ->dataset
  "Create a dataset from either csv/tsv or a sequence of maps.
   *  A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
   ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
   is csv or tsv and then `tablesaw` has column datatype detection mechanisms which
   can be overridden.
   *  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.
  Options:
  :table-name - set the name of the dataset
  :columns-types - sequence of tech.datatype datatype keywords that matches column
     order.  This overrides the tablesaw autodetect mechanism.
  :column-type-fn - Function that gets passed the first N rows of the csv or tsv and
     returns a sequence of datatype keywords that match column order.  The column names
     -if available- are passed as the first row of the csv/tsv.  This overrides the
     tablesaw autodetect mechanism.
  :header? - True of the first row of the csv/tsv contains the column names.  Defaults
     to true.
  :separator - The separator to use.  If not specified an autodetect mechanism is used.
  :column-definitions - If a sequence of maps is used, this overrides the column
  datatype detection mechanism.  See map-seq->dataset for explanation.

  Returns a new dataset"
  ([dataset
    {:keys [table-name]
     :as options}]
   (let [dataset
         (cond
           (satisfies? ds-proto/PColumnarDataset dataset)
           dataset
           (instance? InputStream dataset)
           (ds-impl/parse-dataset dataset options)

           (string? dataset)
           (let [^String dataset dataset
                 gzipped? (.endsWith dataset ".gz")
                 json? (or (.endsWith dataset ".json")
                           (.endsWith dataset ".json.gz"))
                 tsv? (or (.endsWith dataset ".tsv")
                          (.endsWith dataset ".tsv.gz"))
                 options (if (and tsv? (not (contains? options :separator)))
                           (assoc options :separator \tab)
                           options)
                 options (if (and json? (not (contains? options :key-fn)))
                           (assoc options :key-fn keyword)
                           options)
                 open-fn (if json?
                           #(-> (apply io/get-json % (apply clojure.core/concat
                                                            options))
                                (ds-impl/map-seq->dataset options))
                           ds-impl/parse-dataset)]
             (with-open [istream (if gzipped?
                                   (io/gzip-input-stream dataset)
                                   (io/input-stream dataset))]
               (open-fn istream)))
           :else
           (ds-impl/map-seq->dataset dataset options))]
     (if table-name
       (ds-proto/set-dataset-name dataset table-name)
       dataset)))
  ([dataset]
   (->dataset dataset {})))


(defn ->>dataset
  "Please see documentation of ->dataset.  Options are the same."
  ([options dataset]
   (->dataset dataset options))
  ([dataset]
   (->dataset dataset)))


(defn dataset->string
  ^String [ds]
  (with-out-str
    ((parallel-req/require-resolve 'tech.ml.dataset.print/print-dataset) ds)))


(defmacro dtype->group-by
  [datatype]
  (case datatype
    :int32 `dfn/arggroup-by-int
    :int64 `dfn/arggroup-by))



(defmacro join-by-column-impl
  [datatype colname lhs rhs options]
  `(let [colname# ~colname
         lhs# ~lhs
         rhs# ~rhs
         options# ~options
         lhs-col# (lhs# colname#)
         rhs-col# (rhs# colname#)
         lhs-missing?# (:lhs-missing? options#)
         rhs-missing?# (:rhs-missing? options#)
         lhs-dtype# (dtype/get-datatype lhs-col#)
         rhs-dtype# (dtype/get-datatype rhs-col#)
         op-dtype# (cond
                     (= lhs-dtype# rhs-dtype#)
                     rhs-dtype#
                     (and (casting/numeric-type? lhs-dtype#)
                          (casting/numeric-type? rhs-dtype#))
                     (builtin-op-providers/widest-datatype lhs-dtype# rhs-dtype#)
                     :else
                     :object)

         idx-groups# ((dtype->group-by ~datatype)
                      identity (dtype/->reader lhs-col# op-dtype#))
         ^List rhs-col# (dtype/->reader rhs-col# op-dtype#)
         n-elems# (dtype/ecount rhs-col#)
         {lhs-indexes# :lhs-indexes
          rhs-indexes# :rhs-indexes
          rhs-missing# :rhs-missing
          lhs-found-vals# :lhs-found-vals}
         (parallel-for/indexed-map-reduce
          n-elems#
          (fn [^long outer-idx# ^long n-indexes#]
            (let [lhs-indexes# (builtin-op-providers/dtype->storage-constructor ~datatype)
                  rhs-indexes# (builtin-op-providers/dtype->storage-constructor ~datatype)
                  rhs-missing# (->bitmap)
                  lhs-found-vals# (HashSet.)]
              (dotimes [inner-idx# n-indexes#]
                (let [idx# (+ outer-idx# inner-idx#)
                      rhs-val# (.get rhs-col# idx#)]
                  (if-let [item# (typecast/datatype->list-cast-fn
                                  ~datatype
                                  (get idx-groups# rhs-val#))]
                    (do
                      (when lhs-missing?# (.add lhs-found-vals# rhs-val#))
                      (dotimes [n-iters# (.size item#)] (.add rhs-indexes# idx#))
                      (.addAll lhs-indexes# item#))
                    (when rhs-missing?# (.add rhs-missing# idx#)))))
              {:lhs-indexes lhs-indexes#
               :rhs-indexes rhs-indexes#
               :rhs-missing rhs-missing#
               :lhs-found-vals lhs-found-vals#}))
          (partial reduce (fn [accum# nextmap#]
                            (->> accum#
                                 (map (fn [[k# v#]]
                                        (let [next-v# (nextmap# k#)]
                                          [k#
                                           (case k#
                                             :lhs-found-vals
                                             (do
                                               (.addAll ^HashSet v# ^HashSet next-v#)
                                               v#)
                                             :rhs-missing
                                             (do
                                               (dtype/set-add-block! v# next-v#)
                                               v#)
                                             (do
                                               (.addAll
                                                (typecast/datatype->list-cast-fn
                                                 ~datatype v#)
                                                (typecast/datatype->list-cast-fn
                                                 ~datatype (nextmap# k#)))
                                               v#))])))
                                 (into {})))))
         lhs-missing# (when lhs-missing?#
                        (reduce (fn [lhs-missing# lhs-missing-key#]
                                  (dtype/set-add-block! lhs-missing#
                                                        (get idx-groups#
                                                             lhs-missing-key#))
                                 lhs-missing#)
                               (->bitmap)
                               (->> (keys idx-groups#)
                                    (remove #(.contains ^HashSet lhs-found-vals# %)))))]
     (merge
      {:join-table
       (let [lhs-cols# (->> (columns lhs#)
                           (map #(ds-col/select % lhs-indexes#)))
             rhs-cols# (->> (columns (remove-column rhs# colname#))
                           (map #(ds-col/select % rhs-indexes#)))]
         (from-prototype lhs# "join-table" (clojure.core/concat lhs-cols# rhs-cols#)))
       :lhs-indexes lhs-indexes#
       :rhs-indexes rhs-indexes#}
      (when rhs-missing?#
        {:rhs-missing rhs-missing#
         :rhs-outer-join (select rhs# :all rhs-missing#)})
      (when lhs-missing?#
        {:lhs-missing lhs-missing#
         :lhs-outer-join (select lhs# :all lhs-missing#)}))))


(defn join-by-column-int32
  [colname lhs rhs options]
  (join-by-column-impl :int32 colname lhs rhs options))


(defn join-by-column-int64
  [colname lhs rhs options]
  (join-by-column-impl :int64 colname lhs rhs options))


(defn join-by-column
  "Join by column.  For efficiency, lhs should be smaller than rhs.
  An options map can be passed in with optional arguments:
  :lhs-missing? Calculate the missing lhs indexes and left outer join table.
  :rhs-missing? Calculate the missing rhs indexes and right outer join table.
  :operation-space - either :int32 or :int64.  Defaults to :int32.
  Returns
  {:join-table - joined-table
   :lhs-indexes - matched lhs indexes
   :rhs-indexes - matched rhs indexes
   ;; -- when rhs-missing? is true --
   :rhs-missing - missing indexes of rhs.
   :rhs-outer-join - rhs outer join table.
   ;; -- when lhs-missing? is true --
   :lhs-missing - missing indexes of lhs.
   :lhs-outer-join - lhs outer join table.
  }"
  ([colname lhs rhs]
   (join-by-column colname lhs rhs {}))
  ([colname lhs rhs {:keys [operation-space]
                     :or {operation-space :int32}
                     :as options}]
   (case operation-space
     :int32 (join-by-column-int32 colname lhs rhs options)
     :int64 (join-by-column-int64 colname lhs rhs options))))
