(ns tech.ml.dataset.base
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.readers.concat :as reader-concat]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.io :as io]
            [tech.parallel.require :as parallel-req]
            [tech.parallel.utils :as par-util])
  (:import [java.io InputStream]
           [tech.v2.datatype ObjectReader]
           [java.util List]
           [it.unimi.dsi.fastutil.longs LongArrayList]))


(set! *warn-on-reflection* true)


(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn set-dataset-name
  [dataset ds-name]
  (ds-proto/set-dataset-name dataset ds-name))

(defn ds-row-count
  [dataset]
  (second (dtype/shape dataset)))

(defn ds-column-count
  [dataset]
  (first (dtype/shape dataset)))

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
              (let [missing-count (count (ds-col/missing col))]
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
                             (filter colname-set))]
    (concat ordered-columns
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


(defn ds-filter
  "dataset->dataset transformation.  Predicate is passed a map of
  colname->column-value."
  [predicate dataset & [column-name-seq]]
  (->> (or column-name-seq (column-names dataset))
       (select-columns dataset)
       (mapseq-reader)
       (dfn/argfilter predicate)
       (select dataset :all)))


(defn ds-filter-column
  "Filter a given column by a predicate.  Predicate is passed column values.
  truthy values are kept.  Returns a dataset."
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


(defn ds-group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  a map of colname->column-value.  Selecting which columns are used in the key-fn
  using column-name-seq is optional but will greatly improve performance."
  [key-fn dataset & [column-name-seq]]
  (->> (group-by->indexes key-fn dataset column-name-seq)
       (map (fn [[k v]] [k (select dataset :all v)]))
       (into {})))


(defn group-by-column->indexes
  [colname dataset]
  (->> (column dataset colname)
       (dtype/->reader)
       (dfn/arggroup-by identity)))


(defn ds-group-by-column
  "Return a map of column-value->dataset."
  [colname dataset]
  (->> (group-by-column->indexes colname dataset)
       (map (fn [[k v]]
              [k (-> (select dataset :all v)
                     (set-dataset-name k))]))
       (into {})))


(defn ds-sort-by
  ([key-fn compare-fn dataset column-name-seq]
   (->> (or column-name-seq (column-names dataset))
        (select-columns dataset)
        (mapseq-reader)
        (map-indexed vector)
        (sort-by (comp key-fn second) compare-fn)
        (map first)
        (select dataset :all)))
  ([key-fn compare-fn dataset]
   (ds-sort-by key-fn compare-fn dataset :all))
  ([key-fn dataset]
   (ds-sort-by key-fn compare dataset :all)))


(defn ds-sort-by-column
  ([colname compare-fn dataset]
   (ds-sort-by #(get % colname) compare-fn dataset [colname]))
  ([colname dataset]
   (ds-sort-by-column colname < dataset)))


(defn ds-concat
  [dataset & other-datasets]
  (let [datasets (->> (concat [dataset] (remove nil? other-datasets))
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
                 (group-by :name))
            label-map (->> datasets
                           (map (comp :label-map metadata))
                           (apply merge))]
        (when-not (= 1 (count (->> (vals column-list)
                                   (map count)
                                   distinct)))
          (throw (ex-info "Dataset is missing a column" {})))
        (->> column-list
             (map (fn [[_colname columns]]
                    (let [columns (map :column columns)
                          column-values (reader-concat/concat-readers columns)
                          first-col (first columns)]
                      (ds-col/new-column (ds-col/column-name first-col)
                                         column-values
                                         (ds-col/metadata first-col)))))
             (ds-proto/from-prototype dataset (dataset-name dataset))
             (#(set-metadata % {:label-map label-map})))))))

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


(defn ds-take-nth
  [n-val dataset]
  (select dataset :all (->> (range (second (dtype/shape dataset)))
                            (take-nth n-val))))


(defn ds-map-values
  "Note this returns a sequence, not a dataset."
  [dataset map-fn & [column-name-seq]]
  (->> (index-value-seq (select dataset (or (seq column-name-seq) :all) :all))
       (map (fn [[_idx col-values]]
              (apply map-fn col-values)))))


(defn ds-column-map
  "Map a function columnwise across datasets and produce a new dataset.
  column sequence.  Note this does not produce a new dataset as that would
  preclude remove,filter on nil values."
  [map-fn first-ds & ds-seq]
  (let [all-datasets (concat [first-ds] ds-seq)]
       ;;first order the columns
       (->> all-datasets
            (map columns)
            (apply map map-fn))))


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
                           #(-> (apply io/get-json % (apply concat options))
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


(defn join-by-column
  "Join by column.  For efficiency, lhs should be smaller than rhs as it is sorted
  in memory."
  [colname lhs rhs & {:keys [error-on-missing?]}]
  (let [_ (println "initial group by")
        idx-groups (time (group-by-column->indexes colname lhs))
        rhs-col (typecast/datatype->reader :object (rhs colname))
        lhs-indexes (LongArrayList.)
        rhs-indexes (LongArrayList.)
        n-elems (dtype/ecount rhs-col)]
    ;;This would have to be parallelized and thus have a separate set of indexes
    ;;that were merged later in order to really be nice.  tech.datatype has no
    ;;parallized operation that will result in this.
    _ (println "array list join")
    (time
     (loop [idx 0]
       (when (< idx n-elems)
         (if-let [^LongArrayList item (get idx-groups (.read rhs-col idx))]
           (do
             (dotimes [n-iters (.size item)] (.add rhs-indexes idx))
             (.addAll lhs-indexes item))
           (when error-on-missing?
             (throw (Exception. (format "Failed to find column value %s"
                                        (.read rhs-col idx))))))
         (recur (unchecked-inc idx)))))
    (println "final column select")
    (time
     (let [lhs-cols (->> (columns lhs)
                         (map #(ds-col/select % lhs-indexes)))
           rhs-cols (->> (columns (remove-column rhs colname))
                         (map #(ds-col/select % rhs-indexes)))]
       (from-prototype lhs "join-table" (concat lhs-cols rhs-cols))))))
