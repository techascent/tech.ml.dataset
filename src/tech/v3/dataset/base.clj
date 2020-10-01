(ns ^:no-doc tech.v3.dataset.base
  "Base dataset bare bones implementation.  Methods here are used in further
  implementations and they are exposed to users."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.libs.smile.data :as smile-data]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.parse.mapseq-colmap :as parse-mapseq-colmap]
            [tech.v3.dataset.parse :as ds-parse]
            [tech.v3.dataset.parse.univocity :as univocity]
            [tech.io :as io]
            [clojure.tools.logging :as log])
  (:import [java.io InputStream File]
           [tech.v3.datatype Buffer ObjectReader]
           [tech.v3.dataset.impl.dataset Dataset]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset.string_table StringTable]
           [java.util List HashSet LinkedHashMap Map Arrays]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IFn]
           [smile.data DataFrame]
           [smile.io Read])
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


(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (ds-proto/columns dataset))


(defn column
  [dataset colname]
  (if-let [retval (get dataset colname)]
    retval
    (errors/throwf "Unable to find column %s" colname)))


(defn column-names
  "In-order sequence of column names"
  [dataset]
  (->> (ds-proto/columns dataset)
       (map ds-col/column-name)))


(defn has-column?
  [dataset column-name]
  (contains? dataset column-name))


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
  (let [index-seq (if (number? index-seq)
                    [index-seq]
                    index-seq)]
    (ds-proto/select dataset colname-seq index-seq)))


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
              (let [old-name (ds-col/column-name col)]
                (if (contains? colname-map old-name)
                  (ds-col/set-name col (get colname-map old-name))
                  col))))
       (ds-impl/new-dataset (dataset-name dataset)
                            (meta dataset))))


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
          (vals dataset)))


(defn supported-column-stats
  "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."
  [dataset]
  (ds-proto/supported-column-stats dataset))


(defn check-dataset-wrong-position
  [item]
  (when (instance? Dataset item)
    (throw (Exception. "Dataset now must be passed as last option.
This is an interface change and we do apologize!"))))


(defn filter
  "dataset->dataset transformation.  Predicate is passed a map of
  colname->column-value."
  ([dataset column-name-seq predicate]
   (check-dataset-wrong-position column-name-seq)
   (->> (or column-name-seq (column-names dataset))
        (select-columns dataset)
        (ds-readers/mapseq-reader)
        (argops/argfilter predicate)
        (select dataset :all)))
  ([dataset predicate]
   (filter dataset nil predicate)))


(defn filter-column
  "Filter a given column by a predicate.  Predicate is passed column values.
  If predicate is *not* an instance of Ifn it is treated as a value and will
  be used as if the predicate is #(= value %).
  Returns a dataset."
  [dataset colname predicate]
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
    (->> (get dataset colname)
         (argops/argfilter predicate)
         (select dataset :all))))


(defn group-by->indexes
  ([dataset column-name-seq key-fn]
   (check-dataset-wrong-position column-name-seq)
   (->> (or column-name-seq (column-names dataset))
        (select-columns dataset)
        (ds-readers/mapseq-reader)
        (argops/arggroup-by key-fn {:unordered? false})))
  ([dataset key-fn]
   (group-by->indexes dataset nil key-fn)))


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


(defn group-by-column->indexes
  [dataset colname]
  (->> (column dataset colname)
       (dtype/->reader)
       (argops/arggroup {:unordered? false})))


(defn group-by-column
  "Return a map of column-value->dataset."
  [colname dataset]
  (->> (group-by-column->indexes dataset colname)
       (pmap (fn [[k v]]
               [k (-> (select dataset :all v)
                      (set-dataset-name k))]))
       (into {})))


(defn sort-by
  "Sort a dataset by a key-fn and compare-fn."
  ([dataset compare-fn column-name-seq key-fn]
   (check-dataset-wrong-position column-name-seq)
   (->> (or column-name-seq (column-names dataset))
        (select-columns dataset)
        (ds-readers/mapseq-reader)
        (dtype/emap key-fn :object)
        (argops/argsort compare-fn)
        (select dataset :all)))
  ([dataset compare-fn key-fn]
   (sort-by dataset compare-fn :all key-fn))
  ([dataset key-fn]
   (sort-by dataset nil :all key-fn)))


(defn sort-by-column
  "Sort a dataset by a given column using the given compare fn."
  ([colname compare-fn dataset]
   (->> (argops/argsort compare-fn (dataset colname))
        (select dataset :all)))
  ([colname dataset]
   (sort-by-column colname nil dataset)))


(defn ->sort-by-column
  "sort-by-column used in -> dataflows"
  ([dataset colname compare-fn]
   (sort-by-column colname compare-fn dataset))
  ([dataset colname]
   (sort-by-column colname dataset)))


(defn- do-concat
  [reader-concat-fn dataset other-datasets]
  (let [datasets (->> (clojure.core/concat [dataset] (remove nil? other-datasets))
                      (remove nil?)
                      seq)]
    (when-let [dataset (first datasets)]
      (let [column-list
            (->> datasets
                 (mapcat (fn [dataset]
                           (->> (columns dataset)
                                (mapv (fn [col]
                                        (assoc (meta col)
                                               :column
                                               col
                                               :table-name (dataset-name dataset)))))))
                 (clojure.core/group-by :name))
            label-map (->> datasets
                           (map (comp :label-map meta))
                           (apply merge))]
        (when-not (= 1 (count (->> (vals column-list)
                                   (map count)
                                   distinct)))
          (throw (ex-info "Dataset is missing a column" {})))
        (->> column-list
             (pmap (fn [[colname columns]]
                     (let [columns (map :column columns)
                           final-dtype (if (== 1 (count columns))
                                         (dtype/get-datatype (first columns))
                                         (reduce casting/widest-datatype
                                                 (map dtype/get-datatype columns)))
                           column-values (reader-concat-fn final-dtype columns)
                           missing
                           (->> (reduce
                                 (fn [[missing offset] col]
                                   [(dtype-proto/set-or
                                     missing
                                     (dtype-proto/set-offset (ds-col/missing col)
                                                             offset))
                                    (+ offset (dtype/ecount col))])
                                 [(->bitmap) 0]
                                 columns)
                                (first))
                           first-col (first columns)]
                       (ds-col/new-column colname
                                          column-values
                                          (meta first-col)
                                          missing))))
             (ds-impl/new-dataset (dataset-name dataset))
             (#(with-meta % {:label-map label-map})))))))


(defn concat-inplace
  "Concatenate datasets in place.  Respects missing values.  Datasets must all have the
  same columns.  Result column datatypes will be a widening cast of the datatypes."
  [dataset & datasets]
  (do-concat #(dtype/concat-buffers %1 %2)
             dataset datasets))


(defn concat-copying
  "Concatenate datasets into a new dataset copying data.  Respects missing values.
  Datasets must all have the same columns.  Result column datatypes will be a widening
  cast of the datatypes."
  [dataset & datasets]
  (let [datasets (->> (clojure.core/concat [dataset] (remove nil? datasets))
                      (remove nil?)
                      seq)
        n-rows (long (reduce + (map row-count datasets)))]
    (do-concat #(-> (dtype/copy-raw->item! %2
                                           (dtype/make-container
                                            :jvm-heap %1 n-rows))
                    (first))
               (first datasets) (rest datasets))))


(defn concat
  "Concatenate datasets in place.  Respects missing values.  Datasets must all have the
  same columns.  Result column datatypes will be a widening cast of the datatypes.
  Also see concat-copying as this may be faster in many situations."
  [dataset & datasets]
  (apply concat-inplace dataset datasets))


(defn- sorted-int32-sequence
  [idx-seq]
  (let [^ints data (dtype/make-container :java-array :int32 idx-seq)]
    (Arrays/sort data)
    data))


(defn unique-by
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key,idx-seq->idx.  Defaults to #(first %2)."
  ([dataset {:keys [column-name-seq keep-fn]
            :or {keep-fn #(first %2)}
             :as _options}
    map-fn]
   (->> (group-by->indexes map-fn column-name-seq dataset)
        (map (fn [[k v]] (keep-fn k v)))
        (sorted-int32-sequence)
        (select dataset :all)))
  ([dataset map-fn]
   (unique-by map-fn {} dataset)))


(defn unique-by-column
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key, idx-seq->idx.  Defaults to #(first %2)."
  ([dataset
    {:keys [keep-fn]
     :or {keep-fn #(first %2)}
     :as _options}
    colname]
   (->> (group-by-column->indexes colname dataset)
        (map (fn [[k v]] (keep-fn k v)))
        (sorted-int32-sequence)
        (select dataset :all)))
  ([dataset colname]
   (unique-by-column colname {} dataset)))


(defn take-nth
  [dataset n-val]
  (select dataset :all (->> (range (second (dtype/shape dataset)))
                            (clojure.core/take-nth n-val))))


(defn ->dataset
  "Create a dataset from either csv/tsv or a sequence of maps.
   *  A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
   ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
   is csv or tsv and then engineering around detecting datatypes all of which can
   be overridden.
   *  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.

  Options:
  - `:dataset-name` - set the name of the dataset.
  - `:file-type` - Override filetype discovery mechanism for strings or force a particular
      parser for an input stream.  Note that arrow and parquet must have paths on disk
      and cannot currently load from input stream.  Acceptible file types are:
      #{:csv :tsv :xlsx :xls :arrow :parquet}.
  - `:gzipped?` - for file formats that support it, override autodetection and force
     creation of a gzipped input stream as opposed to a normal input stream.
  - `:column-whitelist` - either sequence of string column names or sequence of column
     indices of columns to whitelist.
  - `:column-blacklist` - either sequence of string column names or sequence of column
     indices of columns to blacklist.
  - `:num-rows` - Number of rows to read
  - `:header-row?` - Defaults to true, indicates the first row is a header.
  - `:key-fn` - function to be applied to column names.  Typical use is:
     `:key-fn keyword`.
  - `:separator` - Add a character separator to the list of separators to auto-detect.
  - `:csv-parser` - Implementation of univocity's AbstractParser to use.  If not
     provided a default permissive parser is used.  This way you parse anything that
     univocity supports (so flat files and such).
  - `:bad-row-policy` - One of three options: :skip, :error, :carry-on.  Defaults to
     :carry-on.  Some csv data has ragged rows and in this case we have several
     options. If the option is :carry-on then we either create a new column or add
     missing values for columns that had no data for that row.
  - `:skip-bad-rows?` - Legacy option.  Use :bad-row-policy.
  - `:max-chars-per-column` - Defaults to 4096.  Columns with more characters that this
     will result in an exception.
  - `:max-num-columns` - Defaults to 8192.  CSV,TSV files with more columns than this
     will fail to parse.  For more information on this option, please visit:
     https://github.com/uniVocity/univocity-parsers/issues/301
  - `:n-initial-skip-rows` - Skip N rows initially.  This currently may include the header
     row.  Works across both csv and spreadsheet datasets.
  - `:parser-fn` -
    - `keyword?` - all columns parsed to this datatype
    - tuple - pair of [datatype `parse-data`] in which case container of type
      [datatype] will be created. `parse-data` can be one of:
        - `:relaxed?` - data will be parsed such that parse failures of the standard
           parse functions do not stop the parsing process.  :unparsed-values and
           :unparsed-indexes are available in the metadata of the column that tell
           you the values that failed to parse and their respective indexes.
        - `fn?` - function from str-> one of `:tech.ml.dataset.parser/missing`,
           `:tech.ml.dataset.parser/parse-failure`, or the parsed value.
           Exceptions here always kill the parse process.  :missing will get marked
           in the missing indexes, and :parse-failure will result in the index being
           added to missing, the unparsed the column's :unparsed-values and
           :unparsed-indexes will be updated.
        - `string?` - for datetime types, this will turned into a DateTimeFormatter via
           DateTimeFormatter/ofPattern.  For encoded-text, this has to be a valid
           argument to Charset/forName.
        - `DateTimeFormatter` - use with the appropriate temporal parse static function
           to parse the value.
   - `map?` - the header-name-or-idx is used to lookup value.  If not nil, then
           value can be any of the above options.  Else the default column parser
           is used.

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
                          table-name
                          (when (string? dataset)
                            dataset))
         options (if dataset-name
                   (assoc options :dataset-name dataset-name)
                   options)
         dataset
         (cond
           (satisfies? ds-proto/PColumnarDataset dataset)
           dataset
           (instance? DataFrame dataset)
           (smile-data/dataframe->dataset dataset options)
           (map? dataset)
           (parse-mapseq-colmap/column-map->dataset options dataset)
           (map? (first (seq dataset)))
           (parse-mapseq-colmap/mapseq->dataset options dataset)
           (or (string? dataset) (instance? InputStream dataset))
           (let [options (if (string? dataset)
                           (merge (ds-parse/str->file-info dataset)
                                  options))]
             (ds-parse/data->dataset dataset options)))]
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

  options -
  :unpack? - unpack packed datetime types.  Defaults to true"
  ([ds {:keys [unpack?]
        :or {unpack? true}}]
   (reduce (fn [ds col]
             (let [colname (ds-col/column-name col)
                   col (if unpack?
                         (packing/unpack col)
                         col)]
               (assoc ds colname (dtype-cmc/->array col))))
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


(defn- data->string
  ^String [data-item]
  (if data-item
    (cond
      (string? data-item) data-item
      (keyword? data-item) (name data-item)
      (symbol? data-item) (name data-item)
      :else (.toString ^Object data-item))))


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
   (let [{:keys [gzipped? file-type]}
         (when (string? output)
           (ds-parse/str->file-info output))
         headers (into-array String (map data->string
                                         (column-names ds)))
         ^List str-readers
         (->> (columns ds)
              (mapv #(ds-col/column-map data->string :string %)))
         tsv? (or (= file-type :tsv) (= \tab (:separator options)))
         n-cols (column-count ds)
         n-rows (row-count ds)
         str-rdr (reify ObjectReader
                   (lsize [rdr] n-rows)
                   (readObject [rdr row-idx]
                     (let [^"[Ljava.lang.String;" str-array (make-array
                                                             String n-cols)]
                       (dotimes [col-idx n-cols]
                         (aset str-array col-idx ^String
                               (.readObject ^ObjectReader
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
     (univocity/write! output headers str-rdr output-options)))
  ([ds output]
   (write-csv! ds output {})))
