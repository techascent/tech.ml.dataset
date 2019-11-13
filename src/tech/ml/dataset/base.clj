(ns tech.ml.dataset.base
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.readers.concat :as reader-concat]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.parallel.require :as parallel-req])
  (:import [java.io InputStream]
           [tech.v2.datatype ObjectReader]
           [java.util List]))


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
  ([dataset column-name values {:keys [datatype
                                       container-type]
                                :or {container-type :tablesaw-column}
                                :as options}]
   (let [datatype (or datatype (dtype/get-datatype values))]
     (->> (if (ds-col/is-column? values)
            (ds-col/set-name values column-name)
            (if-let [col (first (columns dataset))]
              (ds-col/new-column col datatype values {:name column-name})
              (dtype/make-container container-type datatype values
                                    (assoc options
                                           :column-name column-name))))
          (add-column dataset))))
  ([dataset column-name values]
   (new-column dataset column-name values {})))


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


(defn value-reader
  "Return a reader that produces a vector of column values per index."
  ^ObjectReader [dataset]
  (let [n-elems (long (second (dtype/shape dataset)))
        readers (->> (columns dataset)
                     (map dtype/->reader))]
    (reify ObjectReader
      (lsize [rdr] n-elems)
      (read [rdr idx] (vec (map #(.get ^List % idx) readers))))))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value"
  [dataset]
  (let [colnames (column-names dataset)]
    (->> (value-reader dataset)
         (unary-op/unary-reader
          :object
          (zipmap colnames x)))))


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
       (map-indexed vector)
       (group-by (comp key-fn second))))


(defn ds-group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  a map of colname->column-value.  Selecting which columns are used in the key-fn
  using column-name-seq is optional but will greatly improve performance."
  [key-fn dataset & [column-name-seq]]
  (->> (group-by->indexes key-fn dataset column-name-seq)
       (map (fn [[k v]] [k (select dataset :all (map first v))]))
       (into {})))


(defn group-by-column->indexes
  [colname dataset]
  (->> (column dataset colname)
       (dtype/->reader)
       (map-indexed vector)
       (group-by second)))


(defn ds-group-by-column
  "Return a map of column-value->dataset."
  [colname dataset]
  (->> (group-by-column->indexes colname dataset)
       (map (fn [[k v]]
              [k (-> (select dataset :all (map first v))
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
  (let [datasets  (concat [dataset] (remove nil? other-datasets))
        column-list
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
                  (ds-col/new-column first-col
                                     (dtype/get-datatype first-col)
                                     column-values
                                     (ds-col/metadata first-col)))))
         (ds-proto/from-prototype dataset (dataset-name dataset))
         (#(set-metadata % {:label-map label-map})))))


(defn unique-by
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from (seq [idx col-val])->idx.  Defaults to ffirst."
  [map-fn dataset & {:keys [column-name-seq keep-fn]
                    :or {keep-fn ffirst}}]
  (->> (group-by->indexes map-fn dataset column-name-seq)
       (map (fn [[_ v]] (keep-fn v)))
       (select dataset :all)))


(defn unique-by-column
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from (seq [idx col-val])->idx.  Defaults to ffirst."
  [colname dataset & {:keys [keep-fn]
                      :or {keep-fn ffirst}}]
  (->> (group-by-column->indexes colname dataset)
       (map (fn [[_ v]] (keep-fn v)))
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
                                    (int-array (map first v)))))
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

(defn map-seq->dataset
  "Given a sequence of maps, construct a dataset.  Defaults to a tablesaw-based
  dataset.  Options are:
  :scan-depth - number of maps to scan in order to decifer column type.
  :column-definintions - sequence of {:name :datatype} maps that decide the columns
  without using autoscan.
  :table-name - name of the new table
  :dataset-constructor - Function to use to transform the sequence of maps into a
  dataset.  Defaults to a tablesaw-based dataset."
  [map-seq {:keys [table-name
                   dataset-constructor]
            :or {table-name "_unnamed"
                 dataset-constructor 'tech.libs.tablesaw/map-seq->tablesaw-dataset}
            :as options}]
  (-> ((parallel-req/require-resolve dataset-constructor)
       map-seq options)
      (set-dataset-name table-name)))


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
           (or (instance? InputStream dataset)
               (string? dataset))
           (apply
            (parallel-req/require-resolve 'tech.libs.tablesaw/path->tablesaw-dataset)
            dataset (apply concat options))
           :else
           (map-seq->dataset dataset options))]
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


(defn name-values-seq->dataset
  "Given a sequence of [name data-seq], produce a columns.  If data-seq is
  of unknown (:object) datatype, the first item is checked. If it is a number,
  then doubles are used.  If it is a string, then strings are used for the
  column datatype.
  All sequences must be the same length.
  Returns a new dataset"
  [name-values-seq & {:keys [column-container-type dataset-name]
                      :or {column-container-type :tablesaw-column
                           dataset-name "_unnamed"}}]
  (when (= column-container-type :tablesaw-column)
    (require '[tech.libs.tablesaw]))
  (let [sizes (->> (map (comp dtype/ecount second) name-values-seq)
                   distinct)]
    (when-not (= 1 (count sizes))
      (throw (ex-info (format "Different sized columns detected: %s" sizes) {})))
    (->> name-values-seq
         (map (fn [[colname values-seq]]
                (let [col-dtype (dtype/get-datatype values-seq)
                      col-dtype (if (= col-dtype :object)
                                  (if (number? (first values-seq))
                                    :float64
                                    :string)
                                  col-dtype)]
                  (dtype/make-container column-container-type
                                        col-dtype
                                        values-seq
                                        {:name colname}))))
         ((parallel-req/require-resolve
           'tech.ml.dataset.generic-columnar-dataset/make-dataset)
          dataset-name))))


(defn dataset->string
  ^String [ds]
  (with-out-str
    ((parallel-req/require-resolve 'tech.ml.dataset.print/print-dataset) ds)))
