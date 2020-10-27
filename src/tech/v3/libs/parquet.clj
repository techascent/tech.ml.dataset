(ns tech.v3.libs.parquet
  "https://gist.github.com/animeshtrivedi/76de64f9dab1453958e1d4f8eca1605f"
  (:require [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.tools.logging :as log])
  (:import [smile.io HadoopInput]
           [tech.v3.datatype PrimitiveList]
           [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.hadoop.metadata BlockMetaData ColumnChunkMetaData]
           [org.apache.parquet.column.page PageReadStore]
           [org.apache.parquet.column.impl ColumnReadStoreImpl]
           [org.apache.parquet.column ColumnDescriptor ColumnReader]
           [org.apache.parquet.io.api GroupConverter PrimitiveConverter]
           [org.apache.parquet.schema OriginalType
            PrimitiveType$PrimitiveTypeName]
           [java.nio ByteBuffer ByteOrder]
           [java.time Instant LocalDate]))


(comment
  (do
    ;;silence the obnoxious loggin
    (require '[tech.v3.dataset.utils :as ds-utils])
    (ds-utils/set-slf4j-log-level :info))
  )

(set! *warn-on-reflection* true)


(def ^:private test-file "test/data/parquet/userdata1.parquet")
(def ^:private test-file-2 "../../perf-benchmark/performance-benchmark-data/part-00000-2211c4d6-c1a7-4bbb-9a27-302a2cb9f30f-c000.snappy.parquet")

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


(defn- original-type-equals?
  [^ColumnDescriptor col-def orig-type]
  (= (.. col-def getPrimitiveType getOriginalType) orig-type))


(defmulti ^:private parse-column
  (fn [col-rdr ^ColumnDescriptor col-def n-rows]
    (.. col-def getPrimitiveType getPrimitiveTypeName)))



(defmethod parse-column :default
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (default-parse-column col-rdr col-def n-rows))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/BINARY
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (if (original-type-equals? col-def OriginalType/UTF8)
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
  (if (original-type-equals? col-def OriginalType/DATE)
    (static-parse-column
     col-rdr col-def n-rows :packed-local-date
     (fn [^PrimitiveList container ^ColumnReader col-rdr]
       (.addObject container (LocalDate/ofEpochDay (.getInteger col-rdr)))))
    (static-parse-column
     col-rdr col-def n-rows :int32
     (fn [^PrimitiveList container ^ColumnReader col-rdr]
       (.addLong container (.getInteger col-rdr))))))


(defmethod parse-column PrimitiveType$PrimitiveTypeName/INT64
  [^ColumnReader col-rdr ^ColumnDescriptor col-def ^long n-rows]
  (cond
    (original-type-equals? col-def OriginalType/TIMESTAMP_MILLIS)
    (static-parse-column
     col-rdr col-def n-rows :packed-instant
     (fn [^PrimitiveList container ^ColumnReader col-rdr]
       (.addObject container (dtype-dt/milliseconds-since-epoch->instant
                              (.getLong col-rdr)))))
    (original-type-equals? col-def OriginalType/TIMESTAMP_MICROS)
    (static-parse-column
     col-rdr col-def n-rows :packed-instant
     (fn [^PrimitiveList container ^ColumnReader col-rdr]
       (.addObject container (dtype-dt/microseconds-since-epoch->instant
                              (.getLong col-rdr)))))
    :else
    (static-parse-column
     col-rdr col-def n-rows :int64
     (fn [^PrimitiveList container ^ColumnReader col-rdr]
       (.addLong container (.getLong col-rdr))))))


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
  [^PageReadStore page ^ParquetFileReader reader ^BlockMetaData block-metadata]
  (let [file-metadata (.getFileMetaData reader)
        schema (.getSchema file-metadata)
        col-read-store (ColumnReadStoreImpl. page (group-converter) schema
                                             (.getCreatedBy file-metadata))
        n-rows (.getRowCount page)]
    (->> (map (fn [^ColumnDescriptor col-def ^ColumnChunkMetaData col-metadata]
                (let [col-name (.. col-def getPrimitiveType getName)
                      stats (.getStatistics col-metadata)]
                  (try
                    (let [reader (locking col-read-store
                                   (.getColumnReader col-read-store col-def))
                          retval (parse-column reader col-def n-rows)
                          converter (condp = (dtype-base/elemwise-datatype retval)
                                      :string
                                      (fn [^Binary data] (String. (.getBytes data)))
                                      :packed-local-date
                                      (fn [^long data] (LocalDate/ofEpochDay data))
                                      identity)]
                      (vary-meta
                       retval
                       merge {:min (converter (.genericGetMin stats))
                              :max (converter (.genericGetMax stats))
                              :num-missing (.getNumNulls stats)}))
                    (catch Throwable e
                      (log/warnf e "Failed to parse column %s: %s"
                                 col-name (.toString col-def))
                      nil))))
              (.getColumns schema) (.getColumns block-metadata))
         (remove nil?)
         (ds-impl/new-dataset))))


(defn- read-next-dataset
  [^ParquetFileReader reader block-metadata block-metadata-seq]
  (if-let [page (.readNextRowGroup reader)]
    (cons (page->ds page reader block-metadata)
          (lazy-seq (read-next-dataset reader
                                       (first block-metadata-seq)
                                       (rest block-metadata-seq))))
    (.close reader)))


(defn read-parquet-file
  ([^String path options]
   (let [reader (ParquetFileReader/open (HadoopInput/file path))
         blocks (.getRowGroups reader)]
     (read-next-dataset reader (first blocks) (rest blocks))))
  ([^String path]
   (read-parquet-file path nil)))
