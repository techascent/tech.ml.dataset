(ns tech.v3.dataset.io
  (:require [tech.io :as io]
            [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.libs.smile.data :as smile-data]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.io.mapseq-colmap :as parse-mapseq-colmap])
  (:import [java.io InputStream File]
           [smile.data DataFrame]))


(defn str->file-info
  [^String file-str]
  (let [file-str (.toLowerCase ^String file-str)
        gzipped? (.endsWith file-str ".gz")
        file-str (if gzipped?
                   (.substring file-str 0 (- (count file-str) 3))
                   file-str)
        last-period (.lastIndexOf file-str ".")
        file-type (if-not (== -1 last-period)
                    (keyword (.substring file-str (inc last-period)))
                    :unknown)]
    {:gzipped? gzipped?
     :file-type file-type}))


(defn wrap-stream-fn
  [dataset gzipped? open-fn]
  (with-open [^InputStream istream (if (instance? InputStream dataset)
                                     dataset
                                     (if gzipped?
                                       (io/gzip-input-stream dataset)
                                       (io/input-stream dataset)))]
    (open-fn istream)))


(defmulti data->dataset
  (fn [data options]
    (:file-type options)))


(defmethod data->dataset :default
  [data options]
  (errors/throwf "Unrecognized read file type: %s"
                 (:file-type options)))


(defmulti dataset->data!
  (fn [ds output options]
    (:file-type options)))


(defmethod dataset->data! :default
  [ds output options]
  (errors/throwf "Unrecognized write file type: %s"
                 (:file-type options)))



(defn ->dataset
  "Create a dataset from either csv/tsv or a sequence of maps.

   * A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
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
                           (merge (str->file-info dataset)
                                  options)
                           options)]
             (data->dataset dataset options))
           (nil? (seq dataset))
           (ds-impl/new-dataset options nil))]
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


(defn write!
  ([dataset output-path options]
   (let [options (merge (when (string? output-path)
                          (str->file-info output-path))
                        options)]
     (dataset->data! dataset output-path options)))
  ([dataset output-path]
   (write! dataset output-path nil)))
