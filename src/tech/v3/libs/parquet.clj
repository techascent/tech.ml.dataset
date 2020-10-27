(ns tech.v3.libs.parquet
  "https://gist.github.com/animeshtrivedi/76de64f9dab1453958e1d4f8eca1605f"
  (:require [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.tools.logging :as log])
  (:import [smile.io HadoopInput]
           [tech.v3.datatype PrimitiveList]
           [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.column.page PageReadStore]
           [org.apache.parquet.column.impl ColumnReadStoreImpl]
           [org.apache.parquet.column ColumnDescriptor ColumnReader]
           [org.apache.parquet.io.api GroupConverter PrimitiveConverter]
           [org.apache.parquet.schema OriginalType
            PrimitiveType$PrimitiveTypeName]
           [java.nio ByteBuffer ByteOrder]
           [java.time Instant]))


(comment
  (do
    ;;silence the obnoxious loggin
    (require '[tech.v3.dataset.utils :as ds-utils])
    (ds-utils/set-slf4j-log-level :info))
  )

(set! *warn-on-reflection* true)


(def ^:private test-file "test/data/parquet/userdata1.parquet")


(declare primitive-converter)

(defn- group-converter
  ^GroupConverter []
  (proxy [GroupConverter] []
    (start [])
    (end [])
    (getConverter [idx]
      (primitive-converter))))


(defn- primitive-converter
  ^PrimitiveConverter []
  (proxy [PrimitiveConverter] []
    (asGroupConverter [] (group-converter))))


(defmulti ^:private parse-column
  (fn [col-rdr ^ColumnDescriptor col-def n-rows]
    (.. col-def getPrimitiveType getPrimitiveTypeName)))


(defn- static-parse-column
  ([^ColumnReader col-rdr ^ColumnDescriptor col-def n-rows
    dtype read-fn missing-value]
   (let [max-def-level (.getMaxDefinitionLevel col-def)
         missing (bitmap/->bitmap)
         n-rows (long n-rows)
         container (col-base/make-container dtype 0)
         _ (.ensureCapacity container n-rows)
         col-name (.. col-def getPrimitiveType getName)]
     (dotimes [row n-rows]
       (if (== max-def-level (.getCurrentDefinitionLevel col-rdr))
         (read-fn container col-rdr)
         (do
           (.add missing row)
           (.addObject container missing-value)))
       (.consume col-rdr))
     (col-impl/new-column col-name container {:parquet-metadata (.toString col-def)}
                          missing)))
  ([^ColumnReader col-rdr ^ColumnDescriptor col-def n-rows
    dtype read-fn]
   (static-parse-column col-rdr col-def n-rows dtype read-fn
                        (col-base/datatype->missing-value dtype))))


(defn- default-parse-column
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :object
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (.addObject container (.. col-rdr getBinary getBytes)))))



(defmethod parse-column :default
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (default-parse-column col-rdr col-def n-rows))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/BINARY
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (if (= (.. col-def getPrimitiveType getOriginalType) OriginalType/UTF8)
    (static-parse-column
     col-rdr col-def n-rows :string
     (fn [^PrimitiveList container ^ColumnReader col-rdr]
       (.addObject container (String. (.. col-rdr getBinary getBytes))))
     "")
    (default-parse-column col-rdr col-def n-rows)))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/INT96
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :instant
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (let [byte-data (.. col-rdr getBinary getBytes)
           byte-buf (-> (ByteBuffer/wrap byte-data)
                        (.order ByteOrder/LITTLE_ENDIAN))
           nano-of-day (.getLong byte-buf)
           julian-day (long (.getInt byte-buf))
           epoch-day (- julian-day 2440588)
           epoch-second (+ (* epoch-day dtype-dt/seconds-in-day)
                           (quot nano-of-day dtype-dt/nanoseconds-in-second))
           nanos (rem nano-of-day dtype-dt/nanoseconds-in-second)]
       (.addObject container (Instant/ofEpochSecond epoch-second nanos))))))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/INT32
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :int32
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (.addLong container (.getInteger col-rdr)))))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/INT64
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :int64
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (.addLong container (.getLong col-rdr)))))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/DOUBLE
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :float64
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (.addDouble container (.getDouble col-rdr)))))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/FLOAT
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :float32
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (.addDouble container (.getFloat col-rdr)))))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/BOOLEAN
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (static-parse-column
   col-rdr col-def n-rows :boolean
   (fn [^PrimitiveList container ^ColumnReader col-rdr]
     (.addBoolean container (.getBoolean col-rdr)))))


(defn- page->ds
  [^PageReadStore page ^ParquetFileReader reader]
  (let [file-metadata (.getFileMetaData reader)
        schema (.getSchema file-metadata)
        col-read-store (ColumnReadStoreImpl. page (group-converter) schema
                                             (.getCreatedBy file-metadata))
        n-rows (.getRowCount page)]
    (->> (.getColumns schema)
         (pmap (fn [^ColumnDescriptor col-def]
                 (let [col-name (.. col-def getPrimitiveType getName)]
                   (try
                     (parse-column (.getColumnReader col-read-store col-def) col-def n-rows)
                     (catch Throwable e
                       (log/warnf e "Failed to parse column %s: %s"
                                  col-name (.toString col-def))
                       nil)))))
         (remove nil?)
         (ds-impl/new-dataset))))


(defn- read-next-dataset
  [^ParquetFileReader reader]
  (if-let [page (.readNextRowGroup reader)]
    (cons (page->ds page reader)
          (lazy-seq (read-next-dataset reader)))
    (.close reader)))

(defn read-parquet-file
  ([^String path options]
   (let [reader (ParquetFileReader/open (HadoopInput/file path))]
     (read-next-dataset reader)))
  ([^String path]
   (read-parquet-file path nil)))
