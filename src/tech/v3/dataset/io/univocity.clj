(ns tech.v3.dataset.io.univocity
  "Bindings to univocity.  Transforms csv's, tsv's into sequences
  of string arrays that are then passed into `tech.v3.dataset.io.string-row-parser`
  methods."
  (:require [tech.v3.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.io.string-row-parser :as row-parser]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.utils :as ds-utils])
  (:import  [tech.v3.datatype Buffer ObjectReader]
            [com.univocity.parsers.common AbstractParser AbstractWriter CommonSettings]
            [com.univocity.parsers.csv
             CsvFormat CsvParserSettings CsvParser
             CsvWriterSettings CsvWriter]
            [com.univocity.parsers.tsv
             TsvWriterSettings TsvWriter]
            [com.univocity.parsers.common.processor.core Processor]
            [java.io Reader InputStream Closeable Writer]
            [java.util List]
            [java.lang AutoCloseable]))


(set! *warn-on-reflection* true)


(defn- sequence-type
  [item-seq]
  (cond
    (every? string? item-seq)
    :string
    (every? number? item-seq)
    :number
    :else
    (throw (Exception. "Item seq must of either strings or numbers"))))


(defn- ->character
  [item]
  (cond
    (instance? Character item)
    item
    (string? item)
    (if (== 1 (count item))
      (first item)
      (throw (Exception.
              (format "Multicharacter separators (%s) are not supported." item))))
    :else
    (throw (Exception. (format "'%s' is not a valid separator" item)))))

(defn- default-csv-options
  [options]
  (-> options
      (update :max-chars-per-column #(or % (* 64 1024)))
      (update :max-num-columns #(or % 8192))))


(defn- apply-options-to-settings!
  ^CommonSettings [^CommonSettings settings options]
  (let [{:keys [max-chars-per-column
                max-num-columns]} (default-csv-options options)]
    (doto settings
      (.setMaxCharsPerColumn (long max-chars-per-column))
      (.setMaxColumns (long max-num-columns))))
  settings)



(defn create-csv-parser
  "Create an implementation of univocity csv parser."
  ^AbstractParser [{:keys [header-row?
                           num-rows
                           column-whitelist
                           column-blacklist
                           separator
                           n-initial-skip-rows]
                    :or {header-row? true
                         ;;64K max chars per column.  This is a silly thing to have
                         ;;to set...
                         }
                    :as options}]
  (if-let [csv-parser (:csv-parser options)]
    csv-parser
    (let [settings (CsvParserSettings.)
          num-rows (or num-rows (:n-records options))
          separator-seq (concat [\, \tab]
                                (when separator
                                  [(->character separator)]))]

      (.detectFormatAutomatically settings (into-array Character/TYPE separator-seq))
      (when num-rows
        (.setNumberOfRecordsToRead settings (if header-row?
                                              (inc (int num-rows))
                                              (int num-rows))))
      (apply-options-to-settings! settings options)
      (doto settings
        (.setSkipEmptyLines true)
        (.setIgnoreLeadingWhitespaces true)
        (.setIgnoreTrailingWhitespaces true))
      (when n-initial-skip-rows
        (.setNumberOfRowsToSkip settings (int n-initial-skip-rows)))
      (when (or (seq column-whitelist)
                (seq column-blacklist))
        (when (and (seq column-whitelist)
                   (seq column-blacklist))
          (throw (Exception.
                  "Either whitelist or blacklist can be provided but not both")))
        (let [[string-fn! number-fn!]
              (if (seq column-whitelist)
                [#(.selectFields
                   settings
                   ^"[Ljava.lang.String;" (into-array String %))
                 #(.selectIndexes
                   settings
                   ^"[Ljava.lang.Integer;" (into-array Integer (map int %)))]
                [#(.excludeFields
                   settings
                   ^"[Ljava.lang.String;" (into-array String %))
                 #(.excludeIndexes
                   settings
                   ^"[Ljava.lang.Integer;"(into-array Integer (map int %)))])
              column-data (if (seq column-whitelist)
                            column-whitelist
                            column-blacklist)
              column-type (sequence-type column-data)]
          (case column-type
            :string (string-fn! column-data)
            :number (number-fn! column-data))))
      (CsvParser. settings))))


(deftype RawRowIterator  [^:unsynchronized-mutable closer
                          ^AbstractParser parser
                          ^:unsynchronized-mutable next-val]
  java.util.Iterator
  (hasNext [this] (boolean next-val))
  (next [this]
    (let [retval next-val]
      (set! next-val (.parseNext parser))
      (when-not next-val
        (when closer
          (cond
            (instance? Closeable closer)
            (.close ^Closeable closer)
            (instance? AutoCloseable closer)
            (.close ^AutoCloseable closer))
          (set! closer nil)))
      retval)))


(defn raw-row-iterable
  "Returns an iterable that produces string[]'s"
  (^Iterable [input ^AbstractParser parser]
   (reify Iterable
     (iterator [this]
       (let [^Reader reader (io/reader input)]
         (.beginParsing parser reader)
         (RawRowIterator. reader parser (.parseNext parser))))))
  (^Iterable [input]
   (raw-row-iterable input (create-csv-parser {}))))


(defn csv->rows
  "Given a csv, produces a sequence of rows.  The csv options from ->dataset
  apply here.

  options:

  - `:column-whitelist` - either sequence of string column names or sequence of column
     indices of columns to whitelist.
  - `:column-blacklist` - either sequence of string column names or sequence of column
     indices of columns to blacklist.
  - `:num-rows` - Number of rows to read
  - `:separator` - Add a character separator to the list of separators to auto-detect.
  - `:max-chars-per-column` - Defaults to 4096.  Columns with more characters that this
     will result in an exception.
  - `:max-num-columns` - Defaults to 8192.  CSV,TSV files with more columns than this
     will fail to parse.  For more information on this option, please visit:
     https://github.com/uniVocity/univocity-parsers/issues/301"
  ([input options]
   (raw-row-iterable
    input
    (create-csv-parser options)))
  ([input]
   (csv->rows input {})))


(defn csv->dataset
  "Non-lazily and serially parse the columns.  Returns a vector of maps of
  {
   :name column-name
   :missing long-reader of in-order missing indexes
   :data typed reader/writer of data
   :metadata - optional map with unparsed-indexes and unparsed-values
  }
  Supports a subset of tech.ml.dataset/->dataset options:
  :column-whitelist
  :column-blacklist
  :n-initial-skip-rows
  :num-rows
  :header-row?
  :separator
  :parser-fn
  :parser-scan-len"
  ([input options]
   (->> (csv->rows input options)
        (seq)
        (row-parser/rows->dataset options)))
  ([input]
   (csv->dataset input {})))


(defn- load-csv
  [data options]
  (ds-io/wrap-stream-fn
   data (:gzipped? options)
   #(csv->dataset %1 options))
  )


(defmethod ds-io/data->dataset :csv
  [data options]
  (load-csv data options))


(defmethod ds-io/data->dataset :tsv
  [data options]
  (load-csv data options))


(defmethod ds-io/data->dataset :txt
  [data options]
  (load-csv data options))


(defprotocol PApplyWriteOptions
  (apply-write-options! [settings options]))


(extend-protocol PApplyWriteOptions
  CsvWriterSettings
  (apply-write-options! [settings options]
    (when-let [quoted-fields (seq (:quoted-columns options))]
      (let [^"[Ljava.lang.String;" str-ary
            (into-array String (map ds-utils/column-safe-name quoted-fields))]
        (.quoteFields settings str-ary)))
    settings)
  TsvWriterSettings
  (apply-write-options! [settings options]
    settings))


(defn rows->csv!
  "Given an something convertible to an output stream, an optional set of headers
  as string arrays, and a sequence of string arrows, write a CSV or a TSV file.

  Options:

  * `:separator` - Defaults to \tab.
  * `:quoted-columns` - For csv, specify which columns should always be quoted
    regardless of their data."
  ([output header-string-array row-string-array-seq]
   (rows->csv! output header-string-array row-string-array-seq {}))
  ([output header-string-array row-string-array-seq
    {:keys [separator]
     :or {separator \tab}
     :as options}]
   (let [^Writer writer (io/writer! output)

         ^AbstractWriter csvWriter
         (if (:csv-writer options)
           (:csv-writer options)
           (let [settings (-> (case separator
                                \,
                                (CsvWriterSettings.)
                                \tab
                                (TsvWriterSettings.))
                              (apply-options-to-settings! options)
                              (apply-write-options! options))]
             (cond
               (instance? CsvWriterSettings settings)
               (CsvWriter. writer ^CsvWriterSettings settings)
               (instance? TsvWriterSettings settings)
               (TsvWriter. writer ^TsvWriterSettings settings)
               :else
               (errors/throwf "Unrecognized settings type %s" (type settings))
               )))]
     (when header-string-array
       (.writeHeaders csvWriter ^"[Ljava.lang.String;" header-string-array))
     (try
       (doseq [^"[Ljava.lang.String;" row row-string-array-seq]
         (.writeRow csvWriter row))
       (finally
         (.close csvWriter))))))


(defn- data->string
  ^String [data-item]
  (if-not (nil? data-item)
    (cond
      (string? data-item) data-item
      (keyword? data-item) (name data-item)
      (symbol? data-item) (name data-item)
      :else (.toString ^Object data-item))))


(defn- write-csv!
  "Write a dataset to a tsv or csv output stream.  Closes output if a stream
  is passed in.  File output format will be inferred if output is a string -
    - .csv, .tsv - switches between tsv, csv.  Tsv is the default.
    - *.gz - write to a gzipped stream.
  At this time writing to json is not supported.
  options -
  :separator - in case output isn't a string, you can use either \\, or \\tab to switch
    between csv or tsv output respectively.
  :headers? - if csv headers are written, defaults to true."
  ([ds output options]
   (let [{:keys [gzipped? file-type]}
         (merge
          (when (string? output)
            (ds-io/str->file-info output))
          options)
         {:keys [max-chars-per-column
                 max-num-columns]} (default-csv-options options)
         columns (vals ds)
         _ (errors/when-not-errorf
            (<= (count columns) (long max-num-columns))
            "Too many columns detected (%d) for max-num-columns (%d)"
            (count columns) max-num-columns)

         headers (when (get options :headers? true)
                   (into-array String
                               (map (comp data->string :name meta) columns)))
         column-names (mapv (comp :name meta) columns)
         ^List str-readers
         (mapv (comp dtype/->reader #(ds-col/column-map data->string :string %)) columns)

         tsv? (or (= file-type :tsv) (= \tab (:separator options)))
         [n-cols n-rows] (dtype/shape ds)
         n-cols (long n-cols)
         n-rows (long n-rows)
         max-chars-per-column (long max-chars-per-column)
         ;;Create a reader that produces string arrays of each row.
         str-rdr (reify ObjectReader
                   (lsize [rdr] n-rows)
                   (readObject [rdr row-idx]
                     (let [^"[Ljava.lang.String;" str-array (make-array
                                                             String n-cols)]
                       (dotimes [col-idx n-cols]
                         (let [str-data (.readObject ^ObjectReader
                                                     (.get str-readers col-idx)
                                                     row-idx)]
                           (errors/when-not-errorf
                            (< (count str-data) max-chars-per-column)
                            "Column %s has value whose length (%d) is greater than max-chars-per-column (%d)."
                            (column-names col-idx) (count str-data) max-chars-per-column)
                           (aset str-array col-idx ^String str-data)))
                       str-array)))
         output (if gzipped?
                  (io/gzip-output-stream! output)
                  output)
         output-options (if tsv?
                          (merge options
                                 {:separator \tab})
                          (merge options
                                 {:separator \,}))]
     (rows->csv! output headers str-rdr output-options)))
  ([ds output]
   (write-csv! ds output {})))


(defmethod ds-io/dataset->data! :csv
  [dataset output options]
  (write-csv! dataset output options))


(defmethod ds-io/dataset->data! :tsv
  [dataset output options]
  (write-csv! dataset output (assoc options :separator \tab)))


(defmethod ds-io/dataset->data! :txt
  [dataset output options]
  (write-csv! dataset output (assoc options :separator \tab)))
