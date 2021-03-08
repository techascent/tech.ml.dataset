(ns tech.v3.dataset.io
  (:require [tech.v3.io :as io]
            [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.io.mapseq-colmap :as parse-mapseq-colmap]
            [tech.v3.dataset.readers :as readers])
  (:import [java.io InputStream File]))


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


(defmethod data->dataset :json
  [data options]
  (->> (apply io/get-json data (apply concat (seq options)))
       (parse-mapseq-colmap/mapseq->dataset options)))


(defmethod data->dataset :edn
  [data options]
  (->> (apply io/get-edn data (apply concat (seq options)))
       (parse-mapseq-colmap/mapseq->dataset options)))


(defmulti dataset->data!
  (fn [ds output options]
    (:file-type options)))


(defmethod dataset->data! :json
  [data options]
  (apply io/put-json! data
         (readers/mapseq-reader data)
         (apply concat (seq options))))


(defmethod dataset->data! :edn
  [data options]
  (apply io/put-edn! data
         (readers/mapseq-reader data)
         (apply concat (seq options))))


(defmethod dataset->data! :default
  [ds output options]
  (errors/throwf "Unrecognized write file type: %s"
                 (:file-type options)))



(defn ->dataset
  "Create a dataset from either csv/tsv or a sequence of maps.

   * A `String` be interpreted as a file (or gzipped file if it
     ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
     is csv or tsv and then engineering around detecting datatypes all of which can
     be overridden.

  * InputStreams have no file type and thus a `file-type` must be provided in the
    options.

  * A sequence of maps may be passed in in which case the first N maps are scanned in
    order to derive the column datatypes before the actual columns are created.

  Parquet, xlsx, and xls formats require that you require the appropriate libraries
  which are `tech.v3.libs.parquet` for parquet, `tech.v3.libs.fastexcel` for xlsx,
  and `tech.v3.libs.poi` for xls.


  Arrow support is provided via the tech.v3.libs.Arrow namespace not via a file-type
  overload as the Arrow project current has 3 different file types and it is not clear
  what their final suffix will be or which of the three file types it will indicate.
  Please see documentation in the `tech.v3.libs.arrow` namespace for further information
  on Arrow file types.

  Options:

  - `:dataset-name` - set the name of the dataset.
  - `:file-type` - Override filetype discovery mechanism for strings or force a particular
      parser for an input stream.  Note that parquet must have paths on disk
      and cannot currently load from input stream.  Acceptible file types are:
      #{:csv :tsv :xlsx :xls :parquet}.
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
  - `:text-temp-dir` - The temporary directory to use for file-backed text.  Setting
     this value to boolean 'false' turns off file backed text.  If a tech.v3.resource
     stack context is opened the file will be deleted when the context closes else it
     will be deleted when the gc cleans up the dataset.  A shutdown hook is added as
     a last resort to ensure the file is cleaned up. Each column's data filefile will
     be created in `(System/getProperty \"java.io.tmpdir\")` by default.
  - `:n-initial-skip-rows` - Skip N rows initially.  This currently may include the
     header row.  Works across both csv and spreadsheet datasets.
  - `:parser-fn` -
      - `keyword?` - all columns parsed to this datatype. For example:
        `{:parser-fn :string}`
      - `map?` - `{column-name parse-method}` parse each column with specified
        `parse-method`.
        The `parse-method` can be:
          - `keyword?` - parse the specified column to this datatype. For example:
            `{:parser-fn {:answer :boolean :id :int32}}`
          - tuple - pair of `[datatype parse-data]` in which case container of type
            `[datatype]` will be created. `parse-data` can be one of:
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
                 DateTimeFormatter/ofPattern.  For `:text` you can specify the backing file
                 to use.
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
           (or (string? dataset) (instance? InputStream dataset))
           (let [options (if (string? dataset)
                           (merge (str->file-info dataset)
                                  options)
                           options)]
             (data->dataset dataset options))
           (map? dataset)
           (parse-mapseq-colmap/column-map->dataset options dataset)
           ;;Not everything has a conversion to seq.
           (map? (try (first (seq dataset))
                      (catch Throwable e nil)))
           (parse-mapseq-colmap/mapseq->dataset options dataset)
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
  "Write a dataset out to a file.  Supported forms are:

```clojure
(ds/write! test-ds \"test.csv\")
(ds/write! test-ds \"test.tsv\")
(ds/write! test-ds \"test.tsv.gz\")
(ds/write! test-ds \"test.nippy\")
(ds/write! test-ds out-stream)
```

Options:

  * `:max-chars-per-column` - csv,tsv specific, defaults to 65536 - values longer than this will
     cause an exception during serialization.
  * `:max-num-columns` - csv,tsv specific, defaults to 8192 - If the dataset has more than this number of
     columns an exception will be thrown during serialization.
  * `:quoted-columns` - csv specific - sequence of columns names that you would like to always have quoted.
  * `:file-type` - Manually specify the file type.  This is usually inferred from the filename but if you
     pass in an output stream then you will need to specify the file type.
  * `:headers?` - if csv headers are written, defaults to true."
  ([dataset output-path options]
   (let [options (merge (when (string? output-path)
                          (str->file-info output-path))
                        options)]
     (dataset->data! dataset output-path options)))
  ([dataset output-path]
   (write! dataset output-path nil)))

