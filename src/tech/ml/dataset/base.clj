(ns tech.ml.dataset.base
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.builtin-op-providers :as builtin-op-providers]
            [tech.v2.datatype.readers.concat :as reader-concat]
            [tech.v2.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.parse.mapseq :as ds-parse-mapseq]
            [tech.io :as io]
            [tech.parallel.require :as parallel-req]
            [tech.parallel.for :as parallel-for]
            [tech.parallel.require :as parallel-require]
            [tech.parallel.utils :as par-util]
            [clojure.tools.logging :as log])
  (:import [java.io InputStream File]
           [tech.v2.datatype ObjectReader]
           [tech.ml.dataset.impl.dataset Dataset]
           [java.util List HashSet LinkedHashMap Map Arrays]
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


(defn column-name->column-map
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


(defn has-column?
  [dataset column-name]
  (try
    (boolean (ds-proto/column dataset column-name))
    (catch Throwable e
      false)))


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
  "Create a new column from some values"
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
  "Same as drop-columns"
  [dataset colname-seq]
  (reduce ds-proto/remove-column dataset colname-seq))


(defn drop-columns
  "Same as remove-columns"
  [dataset col-name-seq]
  (remove-columns dataset col-name-seq))


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


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
  colname-seq - one of:
    - :all - all the columns
    - sequence of column names - those columns in that order.
    - implementation of java.util.Map - column order is dictate by map iteration order
       selected columns are subsequently named after the corresponding value in the map.
       similar to `rename-columns` except this trims the result to be only the columns
       in the map.
  index-seq - either keyword :all or list of indexes.  May contain duplicates.
  "
  [dataset colname-seq index-seq]
  (ds-proto/select dataset colname-seq index-seq))


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
  [dataset col-name-seq]
  (select dataset col-name-seq :all))


(defn rename-columns
  "Rename columns using a map.  Does not reorder columns."
  [dataset colname-map]
  (->> (ds-proto/columns dataset)
       (map (fn [col]
              (if-let [new-name (get colname-map (ds-col/column-name col))]
                (ds-col/set-name col new-name)
                col)))
       (ds-impl/new-dataset (dataset-name dataset)
                            (metadata dataset))))


(defn select-rows
  [dataset row-indexes]
  (select dataset :all row-indexes))


(defn drop-rows
  "Same as remove-rows."
  [dataset row-indexes]
  (let [n-rows (row-count dataset)
        row-indexes (if (dtype/reader? row-indexes)
                      row-indexes
                      (take n-rows row-indexes))]
    (if (== 0 (dtype/ecount row-indexes))
      dataset
      (select dataset :all
              (dtype-proto/set-and-not
               (bitmap/->bitmap (range n-rows))
               row-indexes)))))


(defn remove-rows
  "Same as drop-rows."
  [dataset row-indexes]
  (drop-rows dataset row-indexes))


(defn missing
  [dataset]
  (reduce #(dtype-proto/set-or %1 (ds-col/missing %2))
          (->bitmap)
          dataset))


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


(par-util/export-symbols tech.ml.dataset.readers
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


(defn check-dataset-wrong-position
  [item]
  (when (instance? Dataset item)
    (throw (Exception. "Dataset now must be passed as last option.
This is an interface change and we do apologize!"))))


(defn filter
  "dataset->dataset transformation.  Predicate is passed a map of
  colname->column-value."
  ([predicate column-name-seq dataset]
   (check-dataset-wrong-position column-name-seq)
   (->> (or column-name-seq (column-names dataset))
        (select-columns dataset)
        (mapseq-reader)
        (dfn/argfilter predicate)
        (select dataset :all)))
  ([predicate dataset]
   (filter predicate nil dataset)))


(defn ds-filter
  "Legacy method.  Please see filter"
  [predicate dataset & [column-name-seq]]
  (filter predicate column-name-seq dataset))


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
  ([key-fn column-name-seq dataset]
   (check-dataset-wrong-position column-name-seq)
   (->> (or column-name-seq (column-names dataset))
        (select-columns dataset)
        (mapseq-reader)
        (dfn/arggroup-by key-fn)))
  ([key-fn dataset]
   (group-by->indexes key-fn nil dataset)))


(defn group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  a map of colname->column-value.  Selecting which columns are used in the key-fn
  using column-name-seq is optional but will greatly improve performance."
  ([key-fn column-name-seq dataset]
   (check-dataset-wrong-position column-name-seq)
   (->> (group-by->indexes key-fn column-name-seq dataset)
        (pmap (fn [[k v]] [k (select dataset :all v)]))
        (into {})))
  ([key-fn dataset]
   (group-by key-fn nil dataset)))


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
       (pmap (fn [[k v]]
               [k (-> (select dataset :all v)
                      (set-dataset-name k))]))
       (into {})))

(defn ds-group-by-column
  "Legacy method.  Please see group-by-column"
  [colname dataset]
  (group-by-column colname dataset))


(defn- ->comparator
  [item]
  (when item
    (if (instance? java.util.Comparator item)
      item
      (comparator item))))


(defn sort-by
  "Sort a dataset by a key-fn and compare-fn."
  ([key-fn compare-fn column-name-seq dataset]
   (check-dataset-wrong-position column-name-seq)
   (let [^List data (->> (or column-name-seq (column-names dataset))
                         (select-columns dataset)
                         (mapseq-reader)
                         (dtype/reader-map
                          #(key-fn %)))
         n-elems (.size data)]
     (->> (dfn/argsort data :comparator (->comparator compare-fn))
          (select dataset :all))))
  ([key-fn compare-fn dataset]
   (sort-by key-fn compare-fn :all dataset))
  ([key-fn dataset]
   (sort-by key-fn nil :all dataset)))


(defn ds-sort-by
  "Legacy method.  Please see sort-by"
  ([key-fn compare-fn column-name-seq dataset]
   (check-dataset-wrong-position column-name-seq)
   (sort-by key-fn compare-fn dataset column-name-seq))
  ([key-fn compare-fn dataset]
   (sort-by key-fn compare-fn :all dataset))
  ([key-fn dataset]
   (sort-by key-fn nil :all dataset)))


(defn ->sort-by
  "Version of sort-by used in -> statements common in dataflows"
  ([dataset key-fn compare-fn column-name-seq]
   (sort-by key-fn compare-fn dataset column-name-seq))
  ([dataset key-fn compare-fn]
   (sort-by key-fn compare-fn dataset :all))
  ([dataset key-fn]
   (sort-by key-fn nil dataset :all)))


(defn sort-by-column
  "Sort a dataset by a given column using the given compare fn."
  ([colname compare-fn dataset]
   (let [^List data (dtype/->reader (dataset colname))
         n-elems (.size data)]
     (->> (dfn/argsort data :comparator (->comparator compare-fn))
          (select dataset :all))))
  ([colname dataset]
   (sort-by-column colname nil dataset)))


(defn ds-sort-by-column
  "Legacy method.  Please see sort by column."
  ([colname compare-fn dataset]
   (sort-by-column colname compare-fn dataset))
  ([colname dataset]
   (sort-by-column colname dataset)))


(defn ->sort-by-column
  "sort-by-column used in -> dataflows"
  ([dataset colname compare-fn]
   (sort-by-column colname compare-fn dataset))
  ([dataset colname]
   (sort-by-column colname dataset)))


(defn concat
  "Concatenate datasets.  Respects missing values.  Datasets must all have the same
  columns.  Result column datatypes will be a widening cast of the datatypes."
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
                          final-dtype (if (== 1 (count columns))
                                        (dtype/get-datatype (first columns))
                                        (reduce builtin-op-providers/widest-datatype
                                                (map dtype/get-datatype columns)))
                          column-values (reader-concat/concat-readers
                                         {:datatype final-dtype} columns)
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


(defn- sorted-int32-sequence
  [idx-seq]
  (let [^ints data (dtype/make-container :java-array :int32 idx-seq)]
    (Arrays/sort data)
    data))


(defn unique-by
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key,idx-seq->idx.  Defaults to #(first %2)."
  ([map-fn {:keys [column-name-seq keep-fn]
            :or {keep-fn #(first %2)}
            :as _options}
    dataset]
   (->> (group-by->indexes map-fn column-name-seq dataset)
        (map (fn [[k v]] (keep-fn k v)))
        (sorted-int32-sequence)
        (select dataset :all)))
  ([map-fn dataset]
   (unique-by map-fn {} dataset)))


(defn unique-by-column
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key, idx-seq->idx.  Defaults to #(first %2)."
  ([colname {:keys [keep-fn]
              :or {keep-fn #(first %2)}
              :as _options}
    dataset]
   (->> (group-by-column->indexes colname dataset)
        (map (fn [[k v]] (keep-fn k v)))
        (sorted-int32-sequence)
        (select dataset :all)))
  ([colname dataset]
   (unique-by-column colname {} dataset)))


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
                       (group-by->indexes map-fn column-name-seq dataset)
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
                 (.endsWith file-str ".tsv.gz"))
        xlsx? (or (.endsWith file-str ".xlsx")
                  (.endsWith file-str ".xlsx.gz"))
        xls? (or (.endsWith file-str ".xls")
                 (.endsWith file-str ".xls.gz"))]
    {:gzipped? gzipped?
     :json? json?
     :tsv? tsv?
     :xls? xls?
     :xlsx? xlsx?}))


(defn ->dataset
  "Create a dataset from either csv/tsv or a sequence of maps.
   *  A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
   ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
   is csv or tsv and then engineering around detecting datatypes all of which can
   be overridden.
   *  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.
  Options:
  :table-name - set the name of the dataset (deprecated in favor of :dataset-name).
  :dataset-name - set the name of the dataset.
  :column-whitelist - either sequence of string column names or sequence of column
     indices of columns to whitelist.
  :column-blacklist - either sequence of string column names or sequence of column
     indices of columns to blacklist.
  :num-rows - Number of rows to read
  :header-row? - Defaults to true, indicates the first row is a header.
  :key-fn - function to be applied to column names.  Typical use is:
     `:key-fn keyword`.
  :separator - Add a character separator to the list of separators to auto-detect.
  :csv-parser - Implementation of univocity's AbstractParser to use.  If not provided
     a default permissive parser is used.  This way you parse anything that univocity
     supports (so flat files and such).
  :skip-bad-rows? - For really bad files, some rows will not have the right column
     counts for all rows.  This skips rows that fail this test.
  :max-chars-per-column - Defaults to 4096.  Columns with more characters that this
     will result in an exception.
  :max-num-columns - Defaults to 8192.  CSV,TSV files with more columns than this
     will fail to parse.  For more information on this option, please visit:
     https://github.com/uniVocity/univocity-parsers/issues/301
  :parser-fn -
   - keyword - all columns parsed to this datatype
   - ifn? - called with two arguments: (parser-fn column-name-or-idx column-data)
          - Return value must be implement tech.ml.dataset.parser.PColumnParser in
            which case that is used or can return nil in which case the default
            column parser is used.
   - tuple - pair of [datatype parse-fn] in which case container of type [datatype]
           will be created.
           parse-fn can be one of:
        :relaxed? - data will be parsed such that parse failures of the standard
           parse functions do not stop the parsing process.  :unparsed-values and
           :unparsed-indexes are available in the metadata of the column that tell
           you the values that failed to parse and their respective indexes.
        fn? - function from str-> one of #{:missing :parse-failure value}.
           Exceptions here always kill the parse process.  :missing will get marked
           in the missing indexes, and :parse-failure will result in the index being
           added to missing, the unparsed the column's :unparsed-values and
           :unparsed-indexes will be updated.
        string? - for datetime types, this will turned into a DateTimeFormatter via
           DateTimeFormatter/ofPattern.
        DateTimeFormatter - use with the appropriate temporal parse static function
           to parse the value.
   - map - the header-name-or-idx is used to lookup value.  If not nil, then
           value can be any of the above options.  Else the default column parser
           is used.
  :parser-scan-len - Length of initial column data used for parser-fn's datatype
       detection routine. Defaults to 100.

  Returns a new dataset"
  ([dataset
    {:keys [table-name dataset-name]
     :as options}]
   (let [dataset (if (instance? File dataset)
                   (.toString ^File dataset)
                   dataset)
         ;;Unify table-name and dataset name with dataset-name
         ;;taking precedence.
         dataset-name (or dataset-name
                          table-name)
         options (if dataset-name
                   (assoc options :dataset-name dataset-name)
                   options)
         dataset
         (cond
           (satisfies? ds-proto/PColumnarDataset dataset)
           dataset
           (instance? InputStream dataset)
           (ds-impl/parse-dataset dataset options)

           (string? dataset)
           (let [^String dataset dataset
                 {:keys [gzipped? json? tsv? xls? xlsx?]}
                 (get-file-info dataset)
                 options (if (and tsv? (not (contains? options :separator)))
                           (assoc options :separator \tab)
                           options)
                 options (if (and json? (not (contains? options :key-fn)))
                           (assoc options :key-fn keyword)
                           options)
                 open-fn (cond
                           json?
                           #(-> (apply io/get-json % (apply clojure.core/concat
                                                            options))
                                (ds-parse-mapseq/mapseq->dataset
                                 (merge {:dataset-name dataset}
                                        options)))
                           (or xls? xlsx?)
                           (let [parse-fn (parallel-require/require-resolve
                                           'tech.libs.poi/workbook->datasets)
                                 options (if xls?
                                           (assoc options :poi-file-type :xls)
                                           (assoc options :poi-file-type :xlsx))]
                             (fn [istream]
                               (let [datasets (parse-fn istream options)]
                                 (when-not (== 1 (count datasets))
                                   (log/warnf "Found multiple (%d) worksheets when parsing %s"
                                              (count datasets)
                                              dataset))
                                 (first datasets))))
                           :else
                           #(ds-impl/parse-dataset % (merge {:dataset-name dataset}
                                                            options)))]
             (with-open [istream (if gzipped?
                                   (io/gzip-input-stream dataset)
                                   (io/input-stream dataset))]
               (open-fn istream)))
           :else
           (ds-parse-mapseq/mapseq->dataset dataset options))]
     (if dataset-name
       (ds-proto/set-dataset-name dataset dataset-name)
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
