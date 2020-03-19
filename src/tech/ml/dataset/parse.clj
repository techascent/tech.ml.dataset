(ns tech.ml.dataset.parse
  (:require [tech.io :as io]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.impl.column :refer [make-container] :as col-impl]
            [tech.v2.datatype.bitmap :as bitmap]
            [clojure.set :as set])
  (:import [com.univocity.parsers.common AbstractParser]
           [com.univocity.parsers.csv CsvFormat CsvParserSettings CsvParser]
           [java.io Reader InputStream Closeable]
           [org.roaringbitmap RoaringBitmap]
           [java.lang AutoCloseable]
           [java.lang.reflect Method]
           [java.util Iterator HashMap ArrayList List Map RandomAccess]
           [java.util.function Function]
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
  ^AbstractParser [{:keys [header-row?
                           num-rows
                           column-whitelist
                           column-blacklist]
                    :or {headers? true}
                    :as options}]
  (let [settings (CsvParserSettings.)
        num-rows (or num-rows (:n-records options))]
    (.detectFormatAutomatically settings (into-array Character/TYPE [\, \tab]))
    (when header-row?
      (.setHeaderExtractionEnabled settings true))
    (when num-row
      (.setNumberOfRecordsToRead settings (if header-row?
                                            (inc (int num-rows))
                                            (int num-rows))))
    (when (or (seq column-whitelist)
              (seq column-blacklist))
      (when (and (seq column-whitelist)
                 (seq column-blacklist))
        (throw (Exception. "Either whitelist or blacklist can be provided but not both")))
      (let [[string-fn! number-fn!] (if (seq column-whitelist)
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
    (CsvParser. settings)))


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
                 (throw (Exception. "Parse failure"))))
    :int16 `(Short/parseShort ~val)
    :int32 `(Integer/parseInt ~val)
    :int64 `(Long/parseLong ~val)
    :float32 `(Float/parseFloat ~val)
    :float64 `(Double/parseDouble ~val)
    :keyword `(keyword ~val)
    :symbol `(symbol ~val)))


(defmacro dtype->missing-val
  [datatype]
  `(casting/datatype->cast-fn :unknown
                              ~datatype
                              (get @col-impl/dtype->missing-val-map ~datatype)))


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
         (if-not (== parsed-val# (dtype->missing-val ~datatype))
           (.add (typecast/datatype->list-cast-fn ~datatype container#)
                 parsed-val#)
           (throw (Exception. "Parse failure")))))
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
      (when (> (count str-val#) 1024)
        (throw (Exception. "Text data not string data")))
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


(def default-parser-seq (->> [:boolean (simple-boolean-parser)
                              :int16 (simple-col-parser :int16)
                              :int32 (simple-col-parser :int32)
                              :float32 (simple-col-parser :float32)
                              :int64 (simple-col-parser :int64)
                              :float64 (simple-col-parser :float64)
                              :string (simple-string-parser)
                              :text (simple-text-parser)]
                             (partition 2)
                             (mapv vec)))

(def all-parsers
  (assoc (into {} default-parser-seq)
         :keyword (simple-col-parser :keyword)
         :symbol (simple-col-parser :symbol)))


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


(defn default-column-parser
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
                        converted-container (if (#{:string :text} (first next-parser))
                                              (dtype/reader-map #(.toString ^Object %)
                                                                @container*)
                                              @container*)
                        n-elems (dtype/ecount converted-container)
                        new-container (make-container next-dtype n-elems)]
                    (reset! container* (dtype/copy! converted-container
                                                    new-container)))
                  (.simple-parse!
                   ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
                   @container* str-val)))))))
      (missing! [parser]
        (.add missing (unchecked-int (dtype/ecount @container*)))
        (.simple-missing! ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
                          @container*))
      (column-data [parser]
        {:missing missing
         :data @container*}))))


(defn simple-parser->parser
  [parser-kwd-or-simple-parser]
  (let [simple-parser (if (keyword? parser-kwd-or-simple-parser)
                        (get all-parsers parser-kwd-or-simple-parser)
                        parser-kwd-or-simple-parser)
        parser-dtype (dtype/get-datatype simple-parser)
        container (make-parser-container simple-parser)
        ^RoaringBitmap missing (bitmap/->bitmap)]
    (reify
      dtype-proto/PDatatype
      (get-datatype [this] parser-dtype)
      PColumnParser
      (parse! [this str-val]
        (simple-parse! simple-parser container str-val))
      (missing! [parser]
        (.add missing (unchecked-int (dtype/ecount container)))
        (simple-missing! simple-parser container))
      (column-data [parser]
        {:missing missing
         :data container}))))


(defn- make-parser
  [parser-fn header-row-name scan-rows]
  (cond
    (fn? parser-fn)
    (if-let [parser (parser-fn header-row-name scan-rows)]
      parser
      (default-column-parser))
    (keyword? parser-fn)
    (simple-parser->parser parser-fn)
    (map? parser-fn)
    (if-let [entry (get parser-fn header-row-name)]
      (make-parser entry header-row-name scan-rows)
      (default-column-parser))))


(defn rows->columns
  "Given a sequence of string[] rows, parse into columnar data.
  See csv->columns.
  This method is useful if you have another way of generating sequences of
  string[] row data."
  [row-seq {:keys [header-row?
                   parser-fn
                   parser-scan-len]
            :or {header-row? true
                 parser-scan-len 100}
            :as options}]
  (let [initial-row (first row-seq)
        n-cols (count initial-row)
        header-row (when header-row? initial-row)
        row-seq (if header-row?
                  (rest row-seq)
                  row-seq)
        ^List column-parsers (vec (if parser-fn
                                    (let [scan-rows (take parser-scan-len row-seq)
                                          n-rows (count scan-rows)
                                          scan-cols (->> (apply interleave scan-rows)
                                                         (partition n-rows))]
                                      (map (partial make-parser parser-fn)
                                           (or header-row (range n-cols))
                                           scan-cols))
                                    (repeatedly n-cols default-column-parser)))]
    (doseq [^"[Ljava.lang.String;" row row-seq]
      (loop [col-idx 0]
        (when (< col-idx n-cols)
          (let [^String row-data (aget row col-idx)
                parser (.get column-parsers col-idx)]
            (if (and row-data
                     (> (.length row-data) 0)
                     (not (.equalsIgnoreCase "na" row-data)))
              (parse! parser row-data)
              (missing! parser))
            (recur (unchecked-inc col-idx))))))
    (mapv (fn [init-row-data parser]
            (assoc (column-data parser)
                   :name init-row-data))
          (if header-row?
            initial-row
            (range n-cols))
          column-parsers)))


(defn csv->columns
  "Non-lazily and serially parse the columns.  Returns a vector of maps of
  {
   :name column-name
   :missing long-reader of in-order missing indexes
   :data typed reader/writer of data.
  }
  options:
  column-whitelist - either sequence of string column names or sequence of column indices of columns to whitelist.
  column-blacklist - either sequence of string column names or sequence of column indices of columns to blacklist.
  num-rows - Number of rows to read
  header-row? - Defaults to true, indicates the first row is a header.
  parser-fn -
   - keyword - all columns parsed to this datatype
   - ifn? - called with two arguments: (parser-fn column-name-or-idx column-data)
          - Return value must be implement PColumnParser in which case that is used
            or can return nil in which case the default column parser is used.
   - map - the header-name-or-idx is used to lookup value.  If not nil, then
           can be either of the two above.  Else the default column parser is used.
  parser-scan-len - Length of initial column data used for parser-fn.  Defaults to 100.

  If the parser-fn is confusing, just pass in println and the output should be clear"
  ([input {:keys [header-row?
                  parser-fn
                  column-whitelist
                  column-blacklist
                  n-records
                  parser-scan-len]
           :or {header-row? true
                parser-scan-len 100}
           :as options}]
   (let [^Iterable rows (raw-row-iterable input (create-csv-parser options))
         data (iterator-seq (.iterator ^Iterable rows))]
     (rows->columns data options)))
  ([input]
   (csv->columns input {})))
