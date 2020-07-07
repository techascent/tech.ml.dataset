(ns tech.ml.dataset.parse
  "This file really should be named univocity.clj.  But it is for parsing and writing
  csv and tsv data."
  (:require [tech.io :as io]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.readers.update :as update-reader]
            [tech.ml.dataset.impl.column :refer [make-container] :as col-impl]
            [tech.ml.dataset.parse.datetime
             :refer [datetime-can-parse?]
             :as parse-dt]
            [tech.ml.dataset.string-table :as str-table]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.text :as ds-text]
            [tech.parallel.next-item-fn :refer [create-next-item-fn]]
            [clojure.tools.logging :as log])
  (:import [com.univocity.parsers.common AbstractParser AbstractWriter]
           [com.univocity.parsers.csv
            CsvFormat CsvParserSettings CsvParser
            CsvWriterSettings CsvWriter]
           [com.univocity.parsers.tsv
            TsvWriterSettings TsvWriter]
           [com.univocity.parsers.common.processor.core Processor]
           [java.io Reader InputStream Closeable Writer]
           [org.roaringbitmap RoaringBitmap]
           [java.lang AutoCloseable]
           [java.lang.reflect Method]
           [java.time LocalDate LocalTime LocalDateTime
            Instant ZonedDateTime OffsetDateTime]
           [java.time.format DateTimeFormatter]
           [tech.v2.datatype.typed_buffer TypedBuffer]
           [tech.v2.datatype ObjectReader]
           [java.util Iterator HashMap ArrayList List Map RandomAccess UUID]
           [java.nio.charset StandardCharsets Charset]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))


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
                                  [separator]))]

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


(def test-file "data/ames-house-prices/train.csv")


(defn raw-row-iterable
  "Returns an iterable that produces
  map of
  {:header-row - string[]
   :rows - iterable producing string[] rows
  }"
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


(defprotocol PSimpleColumnParser
  (make-parser-container [parser])
  (can-parse? [parser str-val])
  (simple-parse! [parser container str-val])
  (simple-missing! [parser container]))


(defmacro dtype->parse-fn
  [datatype val]
  (case datatype
    :boolean `(boolean
               (cond
                 (or (.equalsIgnoreCase "t" ~val)
                     (.equalsIgnoreCase "y" ~val)
                     (.equalsIgnoreCase "yes" ~val)
                     (.equalsIgnoreCase "True" ~val))
                 true
                 (or (.equalsIgnoreCase "f" ~val)
                     (.equalsIgnoreCase "n" ~val)
                     (.equalsIgnoreCase "no" ~val)
                     (.equalsIgnoreCase "false" ~val))
                 false
                 :else
                 (throw (Exception. (format "Boolean parse failure: %s" ~val) ))))
    :int16 `(Short/parseShort ~val)
    :int32 `(Integer/parseInt ~val)
    :int64 `(Long/parseLong ~val)
    :float32 `(Float/parseFloat ~val)
    :float64 `(Double/parseDouble ~val)
    :uuid `(UUID/fromString ~val)
    :keyword `(keyword ~val)
    :symbol `(symbol ~val)))


(defmacro dtype->missing-val
  [datatype]
  (if (= :object (casting/flatten-datatype datatype))
    `(col-impl/datatype->missing-value ~datatype)
    `(casting/datatype->cast-fn :unknown
                                ~datatype
                                (col-impl/datatype->missing-value ~datatype))))


(defmacro simple-col-parser
  [datatype]
  `(reify
     dtype-proto/PDatatype
     (get-datatype [parser#] ~datatype)
     PSimpleColumnParser
     (make-parser-container [this] (make-container ~datatype))
     (can-parse? [parser# str-val#]
       (try
         (dtype->parse-fn ~datatype str-val#)
         true
         (catch Throwable e#
           false)))
     (simple-parse! [parser# container# str-val#]
       (let [str-val# (str str-val#)
             parsed-val# (dtype->parse-fn ~datatype str-val#)]
         (.add (typecast/datatype->list-cast-fn ~datatype container#)
               parsed-val#)))
     (simple-missing! [parser# container#]
       (.add (typecast/datatype->list-cast-fn ~datatype container#)
             (dtype->missing-val ~datatype)))))


;;Of course boolean is just slightly different than then umeric parsers.
(defn simple-boolean-parser
  []
  (reify
    dtype-proto/PDatatype
    (get-datatype [this] :boolean)
    PSimpleColumnParser
    (make-parser-container [this] (make-container :boolean))
    (can-parse? [parser str-val]
      (try
        (dtype->parse-fn :boolean str-val)
        true
        (catch Throwable e
          false)))
    (simple-parse! [parser container str-val]
       (let [str-val (str str-val)
             parsed-val (dtype->parse-fn :boolean str-val)]
         (.add (typecast/datatype->list-cast-fn :boolean container)
               parsed-val)))
     (simple-missing! [parser container]
       (.add (typecast/datatype->list-cast-fn :boolean container)
             (dtype->missing-val :boolean)))))


(defn simple-string-parser
  []
  (reify
    dtype-proto/PDatatype
    (get-datatype [item#] :string)
    PSimpleColumnParser
    (make-parser-container [this] (make-container :string))
    (can-parse? [this# item#] (< (count item#) 1024))
    (simple-parse! [parser# container# str-val#]
      (.add ^List container# str-val#))
    (simple-missing! [parser# container#]
      (.add ^List container# ""))))


(defn simple-text-parser
  []
  (reify
    dtype-proto/PDatatype
    (get-datatype [item#] :text)
    PSimpleColumnParser
    (make-parser-container [this] (make-container :text))
    (can-parse? [this# item#] true)
    (simple-parse! [parser# container# str-val#]
      (.add ^List container# str-val#))
    (simple-missing! [parser# container#]
      (.add ^List container# ""))))


(defn simple-encoded-text-parser
  ([encode-fn decode-fn]
   (reify
     dtype-proto/PDatatype
     (get-datatype [item#] :encoded-text)
     PSimpleColumnParser
     (make-parser-container [this] (ds-text/encoded-text-builder
                                    encode-fn decode-fn))
     (can-parse? [this# item#] true)
     (simple-parse! [parser# container# str-val#]
       (.add ^List container# str-val#))
     (simple-missing! [parser# container#]
       (.add ^List container# ""))))
  ([charset]
   (apply simple-encoded-text-parser
          (ds-text/charset->encode-decode charset)))
  ([]
   (simple-encoded-text-parser ds-text/default-charset)))


(defmacro make-datetime-simple-parser
  [datatype]
  `(let [missing-val# (col-impl/datatype->missing-value ~datatype)]
     (reify
       dtype-proto/PDatatype
       (get-datatype [item#] ~datatype)
       PSimpleColumnParser
       (make-parser-container [this] (make-container ~datatype))
       (can-parse? [this# item#] (datetime-can-parse? ~datatype item#))
       (simple-parse! [parser# container# str-val#]
         (parse-dt/compile-time-add-to-container!
          ~datatype
          container#
          (parse-dt/compile-time-datetime-parse-str ~datatype str-val#)))
       (simple-missing! [parser# container#]
         (parse-dt/compile-time-add-to-container!
          ~datatype
          container#
          missing-val#)))))


(def ^:no-doc all-parsers
  {:boolean (simple-boolean-parser)
   :int16 (simple-col-parser :int16)
   :int32 (simple-col-parser :int32)
   :int64 (simple-col-parser :int64)
   :float64 (simple-col-parser :float64)
   :uuid (simple-col-parser :uuid)
   :packed-duration (make-datetime-simple-parser :packed-duration)
   :packed-local-date (make-datetime-simple-parser :packed-local-date)
   :packed-local-date-time (make-datetime-simple-parser :packed-local-date-time)
   :zoned-date-time (make-datetime-simple-parser :zoned-date-time)
   :string (simple-string-parser)
   :encoded-text (simple-encoded-text-parser)
   :float32 (simple-col-parser :float32)
   :keyword (simple-col-parser :keyword)
   :symbol (simple-col-parser :symbol)
   :text (simple-text-parser)
   :instant (make-datetime-simple-parser :instant)
   :offset-date-time (make-datetime-simple-parser :offset-date-time)
   :local-time (make-datetime-simple-parser :local-time)
   :local-date (make-datetime-simple-parser :local-date)
   :local-date-time (make-datetime-simple-parser :local-date-time)})


(def default-parsers
  [:boolean :int16 :int32 :int64 :float64 :uuid
   :packed-duration :packed-local-date :packed-local-date-time
   :zoned-date-time :string :text])


(def ^:private default-parser-seq
  (->> default-parsers
       (mapv (juxt identity #(get all-parsers %)))))


(defprotocol PColumnParser
  (parse! [parser str-val]
    "Side-effecting parse the value and store it.  Exceptions escaping from here
will stop the parsing system.")
  (missing! [parser]
    "Mark a value as missing.")
  (column-data [parser]
    "Return a map containing
{:data - convertible-to-reader column data.
 :missing - convertible-to-reader array of missing values."))

(defn ^:no-doc convert-reader-to-strings
  "This function has to take into account bad data and just return
  missing values in the case where a reader conversion fails."
  [input-rdr]
  (let [converted-reader (->> (dtype-pp/reader-converter input-rdr)
                              (typecast/datatype->reader :object))
        input-rdr (typecast/datatype->reader :object input-rdr)]
    (reify ObjectReader
      (getDatatype [rdr] :string)
      (lsize [rdr] (.lsize converted-reader))
      (read [rdr idx]
        (try
          (.toString ^Object (.read converted-reader idx))
          (catch Throwable e
            (try (.toString ^Object (.read input-rdr idx))
                 (catch Throwable e
                   ""))))))))


(defn- cheap-missing-value-map
  [^RoaringBitmap keys missing-value]
  (let [n-elems (int (dtype/ecount keys))]
    (reify
      dtype-proto/PToBitmap
      (convertible-to-bitmap? [m] true)
      (as-roaring-bitmap [m] keys)
      java.util.Map
      (size [m] n-elems)
      (containsKey [this k]
        (.contains keys (int k)))
      (get [this k] missing-value)
      (getOrDefault [this k defval]
        (if (.contains keys (int k))
          missing-value
          defval)))))


(defn- attempt-container-promotion
  [missing old-container next-dtype]
  (let [n-elems (dtype/ecount old-container)
        n-bitmap-elems (dtype/ecount missing)
        new-container (make-container next-dtype n-elems)
        missing-val (col-impl/datatype->missing-value next-dtype)]
    (if (= n-elems n-bitmap-elems)
      (dtype/set-constant! new-container 0 missing-val n-elems)
      (let [converted-container
            (if (#{:string :text} next-dtype)
              (convert-reader-to-strings old-container)
              old-container)
            src-reader (if (.isEmpty ^RoaringBitmap missing)
                         converted-container
                         (update-reader/update-reader
                          (dtype/->reader converted-container next-dtype)
                          (cheap-missing-value-map missing missing-val)))]
        (dtype/copy! src-reader new-container)
        new-container))))


(defn- default-column-parser
  []
  (let [initial-parser (first default-parser-seq)
        item-seq* (atom (rest default-parser-seq))
        container* (atom (make-container (first initial-parser)))
        simple-parser* (atom (second initial-parser))
        ^RoaringBitmap missing (bitmap/->bitmap)]
    (reify PColumnParser
      (parse! [this str-val]
        (let [parsed? (try (.simple-parse!
                            ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
                            @container* str-val)
                           true
                           (catch Throwable e
                             false))]
          (when-not parsed?
            (let [parser-seq (drop-while #(not (can-parse? (second %) str-val))
                                         @item-seq*)
                  next-parser (first parser-seq)]
              (reset! item-seq* (rest parser-seq))
              (if next-parser
                (do
                  (reset! simple-parser* (second next-parser))
                  (let [next-dtype (first next-parser)
                        new-container
                        (try
                          (attempt-container-promotion missing
                                                       @container*
                                                       next-dtype)
                          (catch Throwable e
                            (log/warnf  "Error promoting container %s->%s\n
falling back to :string"
                                        (dtype/get-datatype @container*)
                                        next-dtype)
                            (reset! item-seq* (drop-while #(not= :string
                                                                 (first %))
                                                          @item-seq*))
                            (reset! simple-parser* (second (first @item-seq*)))
                            (attempt-container-promotion missing
                                                         @container*
                                                         :string)))]
                    (reset! container* new-container))
                  (.simple-parse!
                   ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
                   @container* str-val)))))))
      (missing! [parser]
        (.add missing (unchecked-int (dtype/ecount @container*)))
        (.simple-missing! ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
                          @container*))
      (column-data [parser]
        {:missing missing
         :data @container*
         :force-datatype? true}))))


(defn- on-parse-failure!
  [str-val cur-idx add-missing-fn ^List unparsed-data ^RoaringBitmap unparsed-indexes]
  (add-missing-fn)
  (.add unparsed-data str-val)
  (.add unparsed-indexes (unchecked-int cur-idx)))


(defn ^:no-doc attempt-simple-parse!
  [parse-add-fn! simple-parser container
   add-missing-fn ^List unparsed-data ^List unparsed-indexes relaxed?
   str-val]
  (if relaxed?
    (try
      (parse-add-fn! simple-parser container str-val)
      (catch Throwable e
        (on-parse-failure! str-val (dtype/ecount container)
                           add-missing-fn
                           unparsed-data unparsed-indexes)))
    (parse-add-fn! simple-parser container str-val)))


(defn ^:no-doc return-parse-data
  ([container missing unparsed-data unparsed-indexes]
   (merge {:data container
           :missing missing
           :force-datatype? true}
          (when-not (== 0 (count unparsed-data))
            {:metadata {:unparsed-data unparsed-data
                        :unparsed-indexes unparsed-indexes}}))))


(defn ^:no-doc simple-parser->parser
  ([parser-kwd-or-simple-parser relaxed?]
   (let [simple-parser (if (keyword? parser-kwd-or-simple-parser)
                         (get all-parsers parser-kwd-or-simple-parser)
                         parser-kwd-or-simple-parser)
         _ (when-not simple-parser
             (throw (Exception. (format "Unsure how to parse datatype %s"
                                        parser-kwd-or-simple-parser))))
         parser-dtype (dtype/get-datatype simple-parser)
         container (make-parser-container simple-parser)
         ^RoaringBitmap missing (bitmap/->bitmap)
         unparsed-data (str-table/make-string-table)
         unparsed-indexes (bitmap/->bitmap)
         add-missing-fn #(do
                           (.add missing (unchecked-int (dtype/ecount container)))
                           (simple-missing! simple-parser container))]
     (reify
       dtype-proto/PDatatype
       (get-datatype [this] parser-dtype)
       PColumnParser
       (parse! [this str-val]
         (attempt-simple-parse! simple-parse! simple-parser container add-missing-fn
                                unparsed-data unparsed-indexes relaxed?
                                str-val))
       (missing! [parser] (add-missing-fn))
       (column-data [parser]
         (return-parse-data container missing unparsed-data unparsed-indexes)))))
  ([parser-kwd-or-simple-parser]
   ;;strict parsing by default.
   (simple-parser->parser parser-kwd-or-simple-parser false)))


(defn ^:no-doc attempt-general-parse!
  [add-fn! parse-fn container add-missing-fn ^List unparsed-data ^RoaringBitmap unparsed-indexes
   str-val]
  (let [parse-val (parse-fn str-val)]
    (cond
      (= parse-val ::missing)
      (add-missing-fn)
      (= parse-val ::parse-failure)
      (let [cur-idx (dtype/ecount container)]
        (add-missing-fn)
        (.add unparsed-data str-val)
        (.add unparsed-indexes cur-idx))
      :else
      (add-fn! parse-val))))


(defn ^:no-doc general-parser
  [datatype parse-fn]
  (when-not (fn? parse-fn)
    (throw (Exception. (format "parse-fn doesn't appear to be a function: %s"
                               parse-fn))))
  (let [container (make-container datatype)
        missing-val (col-impl/datatype->missing-value datatype)
        missing (bitmap/->bitmap)
        add-fn (cond
                 (dtype-dt/packed-datatype? datatype)
                 (let [container (.backing-store (parse-dt/as-typed-buffer container))]
                   (case (casting/un-alias-datatype datatype)
                     :int32 (let [container (parse-dt/as-int-list container)]
                              #(.add container (unchecked-int %)))
                     :int64 (let [container (parse-dt/as-long-list container)]
                              #(.add container (unchecked-long %)))))
                 (casting/numeric-type? datatype)
                 #(.add ^List container (casting/cast % datatype))
                 :else
                 #(.add ^List container %))
        unparsed-data (str-table/make-string-table)
        unparsed-indexes (bitmap/->bitmap)
        add-missing-fn #(do
                          (.add missing (dtype/ecount container))
                          (.add ^List container missing-val))]
    (reify
      dtype-proto/PDatatype
      (get-datatype [this] datatype)
      PColumnParser
      (parse! [this str-val]
        (attempt-general-parse! add-fn parse-fn container add-missing-fn
                                unparsed-data unparsed-indexes
                                str-val))
      (missing! [parser]
        (add-missing-fn))
      (column-data [parser]
        (return-parse-data container missing unparsed-data unparsed-indexes)))))


(defn ^:no-doc datetime-formatter-parser
  [datatype format-string-or-formatter]
  (let [parse-fn (parse-dt/datetime-formatter-or-str->parser-fn
                  datatype format-string-or-formatter)]
    (general-parser datatype parse-fn)))


(defn ^:no-doc make-parser
  ([parser-fn header-row-name scan-rows
    default-column-parser-fn
    simple-parser->parser-fn
    datetime-formatter-fn
    general-parser-fn]
   (cond
     (fn? parser-fn)
     (if-let [parser (parser-fn header-row-name scan-rows)]
       parser
       (default-column-parser-fn))
     (keyword? parser-fn)
     (simple-parser->parser-fn parser-fn)
     (vector? parser-fn)
     (let [[datatype parser-info] parser-fn]
       (cond
         (= parser-info :relaxed?)
         (simple-parser->parser-fn datatype true)
         (parse-dt/datetime-datatype? datatype)
         (datetime-formatter-fn datatype parser-info)
         (= datatype :encoded-text)
         (-> (cond
               (instance? Charset parser-info)
               (simple-encoded-text-parser parser-info)
               (and (:encode-fn parser-info)
                    (:decode-fn parser-info))
               (simple-encoded-text-parser (:encode-fn parser-info)
                                           (:decode-fn parser-info))
               :else
               (throw (Exception.
                       (format "Unrecognized argument to :encoded-text: %s"
                               parser-info))))
             (simple-parser->parser-fn false))
         :else
         (general-parser-fn datatype parser-info)))
     (map? parser-fn)
     (if-let [entry (get parser-fn header-row-name)]
       (make-parser entry header-row-name scan-rows
                    default-column-parser-fn
                    simple-parser->parser-fn
                    datetime-formatter-fn
                    general-parser-fn)
       (default-column-parser-fn))
     :else
     (default-column-parser-fn)))
  ([parser-fn header-row-name scan-rows]
   (make-parser parser-fn header-row-name scan-rows
                default-column-parser
                simple-parser->parser
                datetime-formatter-parser
                general-parser)))


(defn rows->dataset
  "Given a sequence of string[] rows, parse into columnar data.
  See csv->columns.
  This method is useful if you have another way of generating sequences of
  string[] row data."
  [{:keys [header-row?
           parser-fn
           parser-scan-len
           bad-row-policy
           skip-bad-rows?]
    :or {header-row? true
         parser-scan-len 100}
    :as options}
   row-seq]
  (let [initial-row (first row-seq)
        n-cols (count initial-row)
        header-row (when header-row? initial-row)
        bad-row-policy (if (not bad-row-policy)
                         (if skip-bad-rows?
                           :skip
                           :carry-on)
                         bad-row-policy)
        row-seq (if header-row?
                  (rest row-seq)
                  row-seq)
        column-parsers (ArrayList.)
        _ (.addAll column-parsers
                   (if parser-fn
                     (let [scan-rows (take parser-scan-len row-seq)
                           n-rows (count scan-rows)
                           scan-cols (->> (apply interleave scan-rows)
                                          (partition n-rows))]
                       (map (partial make-parser parser-fn)
                            (or header-row (range n-cols))
                            scan-cols))
                     (repeatedly n-cols default-column-parser)))]
    (doseq [^"[Ljava.lang.String;" row row-seq]
      (let [row-len (alength row)
            n-cols (.size column-parsers)
            skip-row?
            (when-not (== (.size column-parsers) row-len)
              (case bad-row-policy
                :error
                (throw (Exception.
                        (format "Row has invalid length: %d\n%s"
                                (count row) (vec row))))
                :skip
                (do
                  (log/warnf "Skipping row (invalid length): %d" row-len)
                  true)
                :carry-on
                (do
                  (if (< row-len n-cols)
                    (dotimes [iter (- n-cols row-len)]
                      (-> (.get column-parsers (+ row-len iter))
                          (missing!)))
                    (dotimes [iter (- row-len n-cols)]
                      (let [new-parser (make-parser parser-fn (+ n-cols iter) [])
                            n-data (dtype/ecount (:data (column-data
                                                         (.get column-parsers 0))))]
                        (dotimes [data-iter n-data]
                          (missing! new-parser))
                        (.add column-parsers new-parser))))
                  false)))]
        (when-not skip-row?
          (loop [col-idx 0]
            (when (< col-idx row-len)
              (let [^String row-data (aget row col-idx)
                    parser (.get column-parsers col-idx)]
                (if (and row-data
                         (> (.length row-data) 0)
                         (not (.equalsIgnoreCase "na" row-data)))
                  (parse! parser row-data)
                  (missing! parser))
                (recur (unchecked-inc col-idx))))))))
    (->> (mapv (fn [init-row-data parser]
                 (assoc (column-data parser)
                        :name init-row-data))
               (if header-row?
                 (concat initial-row
                         (range (count initial-row)
                                (.size column-parsers)))
                 (range (.size column-parsers)))
               column-parsers)
         (ds-impl/new-dataset options))))


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
        (rows->dataset options)))
  ([input]
   (csv->dataset input {})))


(defn rows->n-row-sequences
  "Used for parallizing loading of a csv.  Returns N sequences that fed from a single
  sequence of rows.  Experimental - Not the most effectively way of speeding up
  loading.

  Type-hinting your columns and providing specific parsers for datetime types like:
  (ds/->dataset input {:parser-fn {\"date\" [:packed-local-date \"yyyy-MM-dd\"]}})
  may have a larger effect than parallelization in most cases.

  Loading multiple files in parallel will also have a larger effect than
  single-file parallelization in most cases."
  ([{:keys [header-row?]
     :or {header-row? true}} n row-seq]
   (let [[header-row row-seq] (if header-row?
                                [(first row-seq) (rest row-seq)]
                                [nil row-seq])
         row-fn (create-next-item-fn row-seq)]
     (repeatedly
      n
      (if header-row
        #(concat [header-row]
                 (->> (repeatedly row-fn)
                      (take-while identity)))
        #(->> (repeatedly row-fn)
              (take-while identity))))))
  ([options row-seq]
   (rows->n-row-sequences options (.availableProcessors (Runtime/getRuntime)) row-seq))
  ([row-seq]
   (rows->n-row-sequences {}  row-seq)))


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
