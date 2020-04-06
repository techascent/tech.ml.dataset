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
            [tech.ml.dataset.parse :as ds-parse]
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
  (->> (columns dataset)
       (map (fn [col]
              (if-let [new-name (get colname-map (ds-col/column-name col))]
                (ds-col/set-name col new-name)
                col)))
       (ds-impl/new-dataset (dataset-name dataset)
                            (metadata dataset))))


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
                          col-dtypes (set (map dtype/get-datatype columns))
                          _ (when-not (== 1 (count col-dtypes))
                              (throw (Exception.
                                      (format
                                       "Column %s has mismatched datatypes: %s"
                                       colname
                                       col-dtypes))))
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


(defn- get-file-info
  [^String file-str]
  (let [gzipped? (.endsWith file-str ".gz")
        json? (or (.endsWith file-str ".json")
                  (.endsWith file-str ".json.gz"))
        tsv? (or (.endsWith file-str ".tsv")
                 (.endsWith file-str ".tsv.gz"))]
    {:gzipped? gzipped?
     :json? json?
     :tsv? tsv?}))


(defn ->dataset
  "Create a dataset from either csv/tsv or a sequence of maps.
   *  A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
   ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
   is csv or tsv and then `tablesaw` has column datatype detection mechanisms which
   can be overridden.
   *  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.
  Options:
  :table-name - set the name of the dataset.
  :column-whitelist - either sequence of string column names or sequence of column
       indices of columns to whitelist.
  :column-blacklist - either sequence of string column names or sequence of column
       indices of columns to blacklist.
  :num-rows - Number of rows to read
  :header-row? - Defaults to true, indicates the first row is a header.
  :separator - Add a character separator to the list of separators to auto-detect.
  :csv-parser - Implementation of univocity's AbstractParser to use.  If not provided
       a default permissive parser is used.  This way you parse anything that univocity
       supports (so flat files and such).
  :skip-bad-rows? - For really bad files, some rows will not have the right column
      counts for all rows.  This skips rows that fail this test.
  :parser-fn -
   - keyword - all columns parsed to this datatype
   - ifn? - called with two arguments: (parser-fn column-name-or-idx column-data)
          - Return value must be implement tech.ml.dataset.parser.PColumnParser in
            which case that is used or can return nil in which case the default
            column parser is used.
   - map - the header-name-or-idx is used to lookup value.  If not nil, then
           can be either of the two above.  Else the default column parser is used.
  :parser-scan-len - Length of initial column data used for parser-fn's datatype
       detection routine. Defaults to 100.

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
                 {:keys [gzipped? json? tsv?]}
                 (get-file-info dataset)
                 options (if (and tsv? (not (contains? options :separator)))
                           (assoc options :separator \tab)
                           options)
                 options (if (and json? (not (contains? options :key-fn)))
                           (assoc options :key-fn keyword)
                           options)
                 open-fn (if json?
                           #(-> (apply io/get-json % (apply clojure.core/concat
                                                            options))
                                (ds-impl/map-seq->dataset
                                 (merge {:table-name dataset}
                                        options)))
                           #(ds-impl/parse-dataset %
                                                   (merge {:table-name dataset}
                                                          options)))]
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

(defn- data->string
  ^String [data-item]
  (if data-item
    (cond
      (string? data-item) data-item
      (keyword? data-item) (name data-item)
      (symbol? data-item) (name data-item)
      :else (.toString ^Object data-item))))


(defmacro datatype->string-reader
  [datatype column]
  `(let [reader# (typecast/datatype->reader ~datatype ~column)
         ^RoaringBitmap missing# (ds-col/missing ~column)
         n-elems# (.lsize reader#)]
     (reify ObjectReader
       (lsize [this#] n-elems#)
       (read [this# idx#]
         (when-not (.contains missing# idx#)
           (data->string (.read reader# idx#)))))))


(defn write-csv!
  "Write a dataset to a tsv or csv output stream.  Closes output if a stream
  is passed in.  File output format will be inferred if output is a string -
    - .csv, .tsv - switches between tsv, csv.  Tsv is the default.
    - *.gz - write to a gzipped stream.
  At this time writing to json is not supported.
  options -
  :separator - in case output isn't a string, you can use either \\, or \\tab to switch
    between csv or tsv output respectively."
  ([ds output options]
   (let [{:keys [gzipped? tsv?]}
         (when (string? output)
           (get-file-info output))
         headers (into-array String (map data->string
                                         (column-names ds)))
         ^List str-readers
         (->> (columns ds)
              (mapv (fn [coldata]
                      (case (dtype/get-datatype coldata)
                        :int16 (datatype->string-reader :int16 coldata)
                        :int32 (datatype->string-reader :int32 coldata)
                        :int64 (datatype->string-reader :int64 coldata)
                        :float32 (datatype->string-reader :float32 coldata)
                        :float64 (datatype->string-reader :float64 coldata)
                        (datatype->string-reader :object coldata)))))
         tsv? (or tsv? (= \tab (:separator options)))
         n-cols (column-count ds)
         n-rows (row-count ds)
         str-rdr (reify ObjectReader
                   (lsize [rdr] n-rows)
                   (read [rdr row-idx]
                     (let [^"[Ljava.lang.String;" str-array (make-array
                                                             String n-cols)]
                       (dotimes [col-idx n-cols]
                         (aset str-array col-idx ^String
                               (.read ^ObjectReader
                                      (.get str-readers col-idx)
                                      row-idx)))
                       str-array)))
         output (if gzipped?
                  (io/gzip-output-stream! output)
                  output)
         output-options (if tsv?
                          (merge options
                                 {:separator \tab})
                          (merge options
                                 {:separator \,}))]
     (ds-parse/write! output headers str-rdr output-options)))
  ([ds output]
   (write-csv! ds output {})))
