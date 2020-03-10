(ns tech.ml.dataset.parse
  (:require [tech.io :as io]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.impl.column :refer [make-container] :as col-impl])
  (:import [com.univocity.parsers.common AbstractParser]
           [com.univocity.parsers.csv CsvFormat CsvParserSettings CsvParser]
           [java.io Reader InputStream Closeable]
           [java.lang AutoCloseable]
           [java.util Iterator HashMap ArrayList List Map RandomAccess]
           [java.util.function Function]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))


(set! *warn-on-reflection* true)


;;TODO - Load a subset of columns from a file.
;;TODO - simple way to specific datatypes to use for columns

(defn create-csv-parser
  ^AbstractParser []
  (let [settings (CsvParserSettings.)]
    (.setDelimiterDetectionEnabled settings true (into-array Character/TYPE
                                                             [\, \tab]))
    (CsvParser. settings)))


(def test-file "data/ames-house-prices/train.csv")


(defn raw-row-iterable
  "Returns an iterable that produces string[] rows"
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
   (raw-row-iterable input (create-csv-parser))))


(defprotocol PSimpleColumnParser
  (can-parse? [parser str-val])
  (simple-missing-string? [parser str-val])
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
     (can-parse? [parser# str-val#]
       (try
         (dtype->parse-fn ~datatype str-val#)
         true
         (catch Throwable e#
           false)))
     (simple-missing-string? [parser# str-val#]
       (.equalsIgnoreCase ^String str-val# "na"))
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
    (can-parse? [parser str-val]
      (try
        (dtype->parse-fn :boolean str-val)
        true
        (catch Throwable e
          false)))
    (simple-missing-string? [parser str-val]
      (.equalsIgnoreCase ^String str-val "na"))
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
    (can-parse? [this# item#] (< (count item#) 1024))
    (simple-missing-string? [parser str-val] nil)
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
    (can-parse? [this# item#] true)
    (simple-missing-string? [parser str-val] nil)
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
  (parse! [parser str-val])
  (missing! [parser])
  (missing-string? [parser str-val])
  (column-data [parser]))


(defn default-column-parser
  []
  (let [initial-parser (first default-parser-seq)
        item-seq* (atom (rest default-parser-seq))
        container* (atom (make-container (first initial-parser)))
        simple-parser* (atom (second initial-parser))
        missing (LongArrayList.)]
    (reify PColumnParser
      (missing-string? [parser str-val]
        (.simple-missing-string?
         ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
         str-val))
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
        (.add missing (dtype/ecount @container*))
        (.simple-missing! ^tech.ml.dataset.parse.PSimpleColumnParser @simple-parser*
                          @container*))
      (column-data [parser]
        {:missing missing
         :data @container*}))))


(defn csv->columns
  "Non-lazily and serially parse the columns.  Returns a vector of maps of
  {
   :name column-name
   :missing long-reader of in-order missing indexes
   :data typed reader/writer of data.
  }
  options:
  header-row? - Defaults to true, indicates the first row is a header.
  parser-fn - function taking colname and sequence of column values to decide
        parsing strategy.  Defaults to nil in which case the default parser is used.
        Return value must implement PColumnParser.
  parser-scan-len - Length of initial column data used for parser-fn.  Defaults to 100.

  If the parser-fn is confusing, just pass in println and that may help and let it error out."
  [input & {:keys [header-row?
                   parser-fn
                   parser-scan-len]
            :or {header-row? true
                 parser-scan-len 100}}]
  (let [rows (raw-row-iterable input)
        data (iterator-seq (.iterator rows))
        initial-row (first data)
        data (if header-row?
               (rest data)
               data)
        n-cols (count initial-row)
        ^List column-parsers (vec (if parser-fn
                                    (let [scan-rows (take parser-scan-len data)
                                          scan-cols (->> (apply interleave scan-rows)
                                                         (partition parser-scan-len))]
                                      (map parser-fn initial-row scan-cols))
                                    (repeatedly n-cols default-column-parser)))]
    (doseq [^"[Ljava.lang.String;" row data]
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
