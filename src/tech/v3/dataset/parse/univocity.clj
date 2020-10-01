(ns tech.v3.dataset.parse.univocity
  (:require [tech.io :as io]
            [tech.v3.dataset.parse.string-row-parser :as row-parser]
            [tech.v3.dataset.parse :as ds-parse])
  (:import [com.univocity.parsers.common AbstractParser AbstractWriter]
           [com.univocity.parsers.csv
            CsvFormat CsvParserSettings CsvParser
            CsvWriterSettings CsvWriter]
           [com.univocity.parsers.tsv
            TsvWriterSettings TsvWriter]
           [com.univocity.parsers.common.processor.core Processor]
           [java.io Reader InputStream Closeable Writer]
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



(defn create-csv-parser
  "Create an implementation of univocity csv parser."
  ^AbstractParser [{:keys [header-row?
                           num-rows
                           column-whitelist
                           column-blacklist
                           separator
                           n-initial-skip-rows
                           max-chars-per-column
                           max-num-columns]
                    :or {header-row? true
                         ;;64K max chars per column.  This is a silly thing to have
                         ;;to set...
                         max-chars-per-column (* 64 1024)
                         max-num-columns 8192}
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
      (doto settings
        (.setSkipEmptyLines true)
        (.setIgnoreLeadingWhitespaces true)
        (.setIgnoreTrailingWhitespaces true)
        (.setMaxCharsPerColumn (long max-chars-per-column))
        (.setMaxColumns (long max-num-columns)))
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


(defn raw-row-iterable
  "Returns an iterable that produces string[]'s"
  (^Iterable [input ^AbstractParser parser]
   (reify Iterable
     (iterator [this]
       (let [^Reader reader (io/reader input)
             cur-row (atom nil)]
         (.beginParsing parser reader)
         (reset! cur-row (.parseNext parser))
         (reify
           java.util.Iterator
           (hasNext [this]
             (not (nil? @cur-row)))
           (next [this]
             (let [retval @cur-row
                   next-val (.parseNext parser)]
               (reset! cur-row next-val)
               (when-not next-val
                 (cond
                   (instance? Closeable reader)
                   (.close ^Closeable reader)
                   (instance? AutoCloseable reader)
                   (.close ^AutoCloseable reader)))
               retval)))))))
  (^Iterable [input]
   (raw-row-iterable input (create-csv-parser {}))))


(defn csv->rows
  "Given a csv, produces a sequence of rows.  The csv options from ->dataset
  apply here.

  options:
  :column-whitelist - either sequence of string column names or sequence of column
     indices of columns to whitelist.
  :column-blacklist - either sequence of string column names or sequence of column
     indices of columns to blacklist.
  :num-rows - Number of rows to read
  :separator - Add a character separator to the list of separators to auto-detect.
  :max-chars-per-column - Defaults to 4096.  Columns with more characters that this
     will result in an exception.
  :max-num-columns - Defaults to 8192.  CSV,TSV files with more columns than this
     will fail to parse.  For more information on this option, please visit:
     https://github.com/uniVocity/univocity-parsers/issues/301"
  ([input options]
   (let [^Iterable rows (raw-row-iterable
                         input
                         (create-csv-parser options))]
     (iterator-seq (.iterator rows))))
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
        (row-parser/rows->dataset options)))
  ([input]
   (csv->dataset input {})))


(defmethod ds-parse/data->dataset :csv
  [data options]
  (ds-parse/wrap-stream-fn
   data (:gzipped? options)
   #(csv->dataset %1 options)))


(defmethod ds-parse/data->dataset :tsv
  [data options]
  (ds-parse/wrap-stream-fn
   data (:gzipped? options)
   #(csv->dataset %1 options)))


(defn write!
  ([output header-string-array row-string-array-seq]
   (write! output header-string-array row-string-array-seq {}))
  ([output header-string-array row-string-array-seq
    {:keys [separator]
     :or {separator \tab}
     :as options}]
   (let [^Writer writer (io/writer output)
         ^AbstractWriter csvWriter
         (if (:csv-writer options)
           (:csv-writer options)
           (case separator
             \,
             (CsvWriter. writer (CsvWriterSettings.))
             \tab
             (TsvWriter. writer (TsvWriterSettings.))))]
     (when header-string-array
       (.writeHeaders csvWriter ^"[Ljava.lang.String;" header-string-array))
     (try
       (doseq [^"[Ljava.lang.String;" row row-string-array-seq]
         (.writeRow csvWriter row))
       (finally
         (.close csvWriter))))))
