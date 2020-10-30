(ns tech.v3.libs.parquet
  "Support for reading Parquet files.  Please include these dependencies in
  your project:


```clojure
[org.apache.parquet/parquet-hadoop \"1.10.1\"]
[org.apache.hadoop/hadoop-common
  \"3.1.1\"
  :exclusions [org.slf4j/slf4j-log4j12
               log4j
               com.google.guava/guava
               commons-codec
               commons-logging
               com.google.code.findbugs/jsr305
               com.fasterxml.jackson.core/jackson-databind]]
```

Read implementation initialially based off of:
https://gist.github.com/animeshtrivedi/76de64f9dab1453958e1d4f8eca1605f"

  (:require [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.io.context :as io-context]
            [tech.v3.dataset.io.column-parsers :as col-parsers]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.utils :as ds-utils]
            [tech.v3.io :as io]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.emap :as emap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [clojure.set :as set]
            [clojure.tools.logging :as log])
  (:import [smile.io HadoopInput]
           [tech.v3.datatype PrimitiveList Buffer]
           [org.apache.hadoop.conf Configuration]
           [org.apache.parquet.hadoop ParquetFileReader ParquetWriter
            ParquetFileWriter ParquetFileWriter$Mode CodecFactory
            FilePageWriteStore]
           [org.apache.parquet.hadoop.util HadoopOutputFile]
           [org.apache.parquet.hadoop.metadata BlockMetaData ColumnChunkMetaData
            CompressionCodecName]
           [org.apache.parquet.column ColumnDescriptor ColumnReader Encoding
            ParquetProperties ColumnWriter]
           [org.apache.parquet.column.page PageReadStore]
           [org.apache.parquet.column.impl ColumnReadStoreImpl]
           [org.apache.parquet.io OutputFile PositionOutputStream InputFile
            ColumnIOFactory PrimitiveColumnIO]
           [org.apache.parquet.io.api GroupConverter PrimitiveConverter Binary]
           [org.apache.parquet.schema OriginalType MessageType
            PrimitiveType$PrimitiveTypeName Type$Repetition Type PrimitiveType]
           [org.roaringbitmap RoaringBitmap]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels FileChannel]
           [java.nio.file Paths StandardOpenOption OpenOption]
           [java.io OutputStream]
           [java.time Instant LocalDate]
           [java.util Iterator List HashMap]))


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


(deftype ^:private ColumnIterator [^ColumnReader col-rdr
                                   read-fn
                                   ^long max-def-level
                                   ^:unsynchronized-mutable n-rows]
  Iterator
  (hasNext [it] (> n-rows 0))
  (next [it]
    (if (> n-rows 0)
      (let [retval (if (== max-def-level (.getCurrentDefinitionLevel col-rdr))
                     (read-fn col-rdr)
                     :tech.ml.dataset.parse/missing)]
        (set! n-rows (dec n-rows))
        (.consume col-rdr)
        retval)
      (throw (java.util.NoSuchElementException.)))))


(defn- read-byte-array
  ^bytes [^ColumnReader rdr]
  (.. rdr getBinary getBytes))

(defn- read-str
  ^String [^ColumnReader rdr]
  (String. (read-byte-array rdr)))


(defn- read-instant
  ^Instant [^ColumnReader rdr]
  (let [byte-buf (-> (ByteBuffer/wrap (read-byte-array rdr))
                     (.order ByteOrder/LITTLE_ENDIAN))
        nano-of-day (.getLong byte-buf)
        julian-day (long (.getInt byte-buf))
        epoch-day (- julian-day 2440588)
        epoch-second (+ (* epoch-day dtype-dt/seconds-in-day)
                        (quot nano-of-day dtype-dt/nanoseconds-in-second))
        nanos (rem nano-of-day dtype-dt/nanoseconds-in-second)]
    (Instant/ofEpochSecond epoch-second nanos)))


(defn- read-boolean
  [^ColumnReader rdr]
  (.getBoolean rdr))


(defn- read-local-date
  ^LocalDate [^ColumnReader rdr]
  (LocalDate/ofEpochDay (.getInteger rdr)))


(defn- read-int
  [^ColumnReader rdr]
  (.getInteger rdr))

(defn- read-long
  ^long [^ColumnReader rdr]
  (.getLong rdr))

(defn- read-float
  [^ColumnReader rdr]
  (.getFloat rdr))

(defn- read-double
  ^double [^ColumnReader rdr]
  (.getDouble rdr))

(defn- byte-range?
  [^long cmin ^long cmax]
  (and (>= cmin Byte/MIN_VALUE)
       (<= cmax Byte/MAX_VALUE)))

(defn- ubyte-range?
  [^long cmin ^long cmax]
  (and (>= cmin 0)
       (<= cmax 255)))

(defn- short-range?
  [^long cmin ^long cmax]
  (and (>= cmin Short/MIN_VALUE)
       (<= cmax Short/MAX_VALUE)))

(defn- ushort-range?
  [^long cmin ^long cmax]
  (and (>= cmin 0)
       (<= cmax 65535)))


(defn- col-reader->iterable
  ^Iterable [^ColumnReader col-rdr ^ColumnDescriptor col-def n-rows metadata]
  (let [n-rows (long n-rows)
        pf (.getPrimitiveType col-def)
        prim-type (.getPrimitiveTypeName pf)
        org-type (.getOriginalType pf)
        max-def-level (.getMaxDefinitionLevel col-def)
        col-min (get-in metadata [:statistics :min])
        col-max (get-in metadata [:statistics :max])
        ;;TODO - get dictionaries working.  Columns readers throw exceptions
        ;;no matter what if the dictionary id is queried.
        dictionary? (or (contains? (:encodings metadata) :plain-dictionary)
                        (contains? (:encodings metadata) :rle-dictionary))]
    (condp = prim-type
      PrimitiveType$PrimitiveTypeName/BOOLEAN
      (reify
        dtype-proto/PElemwiseDatatype
        (elemwise-datatype [rdr] :boolean)
        Iterable
        (iterator [rdr] (ColumnIterator. col-rdr read-boolean
                                         max-def-level n-rows)))
      PrimitiveType$PrimitiveTypeName/BINARY
      (if (= org-type OriginalType/UTF8)
        (if false
          (let [dict-data (HashMap.)
                compute-fn (reify java.util.function.Function
                             (apply [this value]
                               (read-str col-rdr)))
                read-fn (fn [^ColumnReader rdr]
                          ;;This always throws an exception independent of if the column
                          ;;has a dictionary encoding
                          (let [dict-id (.getCurrentValueDictionaryID rdr)]
                            (.computeIfAbsent dict-data dict-id compute-fn)))]
            (reify
              dtype-proto/PElemwiseDatatype
              (elemwise-datatype [rdr] :string)
              Iterable
              (iterator [rdr] (ColumnIterator. col-rdr read-fn
                                               max-def-level n-rows))))
          (reify
            dtype-proto/PElemwiseDatatype
            (elemwise-datatype [rdr] :string)
            Iterable
            (iterator [rdr] (ColumnIterator. col-rdr read-str
                                             max-def-level n-rows))))
        (reify
          dtype-proto/PElemwiseDatatype
          (elemwise-datatype [rdr] :object)
          Iterable
          (iterator [rdr] (ColumnIterator. col-rdr read-byte-array
                                           max-def-level n-rows))))
      PrimitiveType$PrimitiveTypeName/INT96
      (reify
        dtype-proto/PElemwiseDatatype
        (elemwise-datatype [rdr] :instant)
        Iterable
        (iterator [rdr] (ColumnIterator. col-rdr read-instant
                                         max-def-level n-rows)))
      PrimitiveType$PrimitiveTypeName/INT32
      (if (= org-type OriginalType/DATE)
        (reify
          dtype-proto/PElemwiseDatatype
          (elemwise-datatype [rdr] :local-date)
          Iterable
          (iterator [rdr] (ColumnIterator. col-rdr read-local-date
                                           max-def-level n-rows)))
        (reify
          dtype-proto/PElemwiseDatatype
          (elemwise-datatype [rdr]
            (cond
              (and col-min col-max)
              (let [col-min (long col-min)
                    col-max (long col-max)]
                (cond
                  (byte-range? col-min col-max) :int8
                  (ubyte-range? col-min col-max) :uint8
                  (short-range? col-min col-max) :int16
                  (ushort-range? col-min col-max) :uint16
                  :else :int32))
              (= org-type OriginalType/INT_8)
              :int8
              (= org-type OriginalType/UINT_8)
              :uint8
              (= org-type OriginalType/INT_16)
              :int16
              (= org-type OriginalType/UINT_16)
              :uint16
              :else
              :int32))
          Iterable
          (iterator [rdr] (ColumnIterator. col-rdr read-int
                                           max-def-level n-rows))))
      PrimitiveType$PrimitiveTypeName/INT64
      (condp = org-type
        OriginalType/TIMESTAMP_MILLIS
        (reify
          dtype-proto/PElemwiseDatatype
          (elemwise-datatype [rdr] :instant)
          Iterable
          (iterator [rdr] (ColumnIterator. col-rdr #(dtype-dt/milliseconds-since-epoch->instant
                                                     (read-long %))
                                           max-def-level n-rows)))
        OriginalType/TIMESTAMP_MICROS
        (reify
          dtype-proto/PElemwiseDatatype
          (elemwise-datatype [rdr] :instant)
          Iterable
          (iterator [rdr] (ColumnIterator. col-rdr #(dtype-dt/microseconds-since-epoch->instant
                                                     (read-long %))
                                           max-def-level n-rows)))
        (reify
          dtype-proto/PElemwiseDatatype
          (elemwise-datatype [rdr]
            (cond
              (and col-min col-max)
              (let [col-min (long col-min)
                    col-max (long col-max)]
                (cond
                  (byte-range? col-min col-max) :int8
                  (ubyte-range? col-min col-max) :uint8
                  (short-range? col-min col-max) :int16
                  (ushort-range? col-min col-max) :uint16
                  :else :int64))
              (= org-type OriginalType/UINT_32)
              :uint32
              :else
              :int64))
          Iterable
          (iterator [rdr] (ColumnIterator. col-rdr read-long
                                           max-def-level n-rows))))
      PrimitiveType$PrimitiveTypeName/DOUBLE
      (reify
        dtype-proto/PElemwiseDatatype
        (elemwise-datatype [rdr] :float64)
        Iterable
        (iterator [rdr] (ColumnIterator. col-rdr read-double
                                         max-def-level n-rows)))
      PrimitiveType$PrimitiveTypeName/FLOAT
      (reify
        dtype-proto/PElemwiseDatatype
        (elemwise-datatype [rdr] :float32)
        Iterable
        (iterator [rdr] (ColumnIterator. col-rdr read-float
                                         max-def-level n-rows)))
      ;;Default case, just return the byte arrays
      (reify
        dtype-proto/PElemwiseDatatype
        (elemwise-datatype [rdr] :object)
        Iterable
        (iterator [rdr] (ColumnIterator. col-rdr read-byte-array
                                         max-def-level n-rows))))))


(defn- parse-column
  [col-rdr ^ColumnDescriptor col-def n-rows parse-context key-fn metadata]
  (let [n-rows (long n-rows)
        col-name (.. col-def getPrimitiveType getName)
        iterable (col-reader->iterable col-rdr col-def n-rows metadata)
        iterator (.iterator iterable)
        col-parser (parse-context col-name)
        {:keys [data missing metadata]}
        (if col-parser
          ;;The user specified a specific parser for this datatype
          (loop [continue? (.hasNext iterator)
                 row 0]
            (if continue?
              (let [col-data (.next iterator)]
                (when-not (= col-data :tech.ml.dataset.parse/missing)
                  (col-parsers/add-value! col-parser row col-data))
                (recur (.hasNext iterator) (unchecked-inc row)))
              (col-parsers/finalize! col-parser n-rows)))
            (let [container-dtype (packing/pack-datatype
                                   (dtype-base/elemwise-datatype iterable))
                  container (col-base/make-container container-dtype)
                  missing-value (col-base/datatype->missing-value container-dtype)
                  missing (bitmap/->bitmap)]
              ;;Faster to not use the generic system because we know what the type
              ;;will be.
              (loop [continue? (.hasNext iterator)
                     row 0]
              (if continue?
                (let [col-data (.next iterator)]
                  (if (.equals ^Object col-data :tech.ml.dataset.parse/missing)
                    (do
                      (.add missing row)
                      (.addObject container missing-value))
                    (.addObject container col-data))
                  (recur (.hasNext iterator) (unchecked-inc row)))
                {:data container
                 :missing missing}))))]
    (col-impl/new-column (key-fn col-name) data metadata missing)))


(defn- row-group->ds
  [^PageReadStore page ^ParquetFileReader reader options block-metadata]
  (let [file-metadata (.getFileMetaData reader)
        schema (.getSchema file-metadata)
        col-read-store (ColumnReadStoreImpl. page (group-converter) schema
                                             (.getCreatedBy file-metadata))
        n-rows (.getRowCount page)
        parse-context (io-context/options->parser-fn options nil)
        key-fn (or (:key-fn options) identity)
        column-whitelist (when (seq (:column-whitelist options))
                           (set (:column-whitelist options)))
        column-blacklist (when (seq (:column-blacklist options))
                           (set (:column-blacklist options)))
        retval
        (->> (map (fn [^ColumnDescriptor col-def col-metadata]
                    (let [cname (key-fn (.. col-def getPrimitiveType getName))
                          whitelisted? (or (not column-whitelist)
                                           (column-whitelist cname))
                          blacklisted? (and column-blacklist
                                            (column-blacklist cname))]
                      (when (or whitelisted?
                                (not blacklisted?))
                        (try
                          (let [reader (.getColumnReader col-read-store col-def)
                                retval (parse-column reader col-def n-rows parse-context key-fn col-metadata)
                                converter (condp = (dtype-base/elemwise-datatype retval)
                                            :string
                                            (fn [^Binary data] (String. (.getBytes data)))
                                            :text
                                            (fn [^Binary data] (String. (.getBytes data)))
                                            :packed-local-date
                                            (fn [^long data] (LocalDate/ofEpochDay data))
                                            ;;Strip non-numeric representations else we risk not being able to
                                            ;;save to nippy
                                            (fn [data] (if (number? data) data nil)))]
                            (vary-meta
                             retval
                             assoc
                             :statistics (-> (:statistics col-metadata)
                                             (update :min #(when % (converter %)))
                                             (update :max #(when % (converter %))))
                             :parquet-metadata (dissoc col-metadata :statistics :primitive-type)))
                          (catch Throwable e
                            (log/warnf e "Failed to parse column %s: %s"
                                       cname (.toString col-def))
                            nil)))))
                  (.getColumns schema) (:columns block-metadata))
             (remove nil?)
             (ds-impl/new-dataset))]
    (vary-meta retval assoc :parquet-metadata (dissoc block-metadata :columns))))


(defn- read-next-dataset
  [^ParquetFileReader reader options block-metadata block-metadata-seq]
  (if-let [row-group (.readNextRowGroup reader)]
    (try
      (cons (row-group->ds row-group reader options block-metadata)
            (lazy-seq (read-next-dataset reader options
                                         (first block-metadata-seq)
                                         (rest block-metadata-seq))))
      (catch Throwable e
        (.close reader)
        (throw e)))
    (.close reader)))


(def ^:private comp-code-map
  {CompressionCodecName/BROTLI :brotli
   CompressionCodecName/GZIP :gzip
   CompressionCodecName/LZ4 :lz4
   CompressionCodecName/LZO :lzo
   CompressionCodecName/SNAPPY :snappy
   CompressionCodecName/UNCOMPRESSED :uncompressed
   CompressionCodecName/ZSTD :zstd})


(def ^:private encoding-map
  {Encoding/BIT_PACKED :bit-packed
   Encoding/DELTA_BINARY_PACKED :delta-binary-packed
   Encoding/DELTA_BYTE_ARRAY :delta-byte-array
   Encoding/DELTA_LENGTH_BYTE_ARRAY :delta-length-byte-array
   Encoding/PLAIN :plain
   Encoding/PLAIN_DICTIONARY :plain-dictionary
   Encoding/RLE :rle
   Encoding/RLE_DICTIONARY :rle-dictionary})


(def ^:private repetition-map
  {Type$Repetition/REQUIRED :required
   Type$Repetition/REPEATED :repeated
   Type$Repetition/OPTIONAL :optional})


(def ^:private original-type-map
  {OriginalType/BSON :bson
   OriginalType/DATE :date
   OriginalType/DECIMAL :decimal
   OriginalType/ENUM :enum
   OriginalType/INTERVAL :interval
   OriginalType/INT_8 :int8
   OriginalType/INT_16 :int16
   OriginalType/INT_32 :int32
   OriginalType/INT_64 :int64
   OriginalType/JSON :json
   OriginalType/LIST :list
   OriginalType/MAP :map
   OriginalType/MAP_KEY_VALUE :map-key-value
   OriginalType/TIMESTAMP_MICROS :timestamp-micros
   OriginalType/TIMESTAMP_MILLIS :timestamp-millis
   OriginalType/TIME_MICROS :time-micros
   OriginalType/TIME_MILLIS :time-millis
   OriginalType/UINT_8 :uint8
   OriginalType/UINT_16 :uint16
   OriginalType/UINT_32 :uint32
   OriginalType/UINT_64 :uint64
   OriginalType/UTF8 :utf8})



(defn- parquet-reader->metadata
  [^ParquetFileReader reader]
  (->> (.getRowGroups reader)
       (map (fn [^BlockMetaData block]
              {:path (.getPath block)
               :num-rows (.getRowCount block)
               :total-byte-size (.getTotalByteSize block)
               :columns
               (->> (.getColumns ^BlockMetaData block)
                    (mapv (fn [^ColumnChunkMetaData metadata]
                            (let [ptype (.getPrimitiveType metadata)]
                              {:starting-pos (.getStartingPos metadata)
                               :compression-codec (get comp-code-map (.getCodec metadata)
                                                       :unrecognized-compression-codec)
                               :path (.toArray (.getPath metadata))
                               :primitive-type ptype
                               :repetition (get repetition-map (.getRepetition ptype)
                                                :unrecognized-repetition)
                               :original-type (get original-type-map (.getOriginalType ptype)
                                                   (.getOriginalType ptype))
                               :first-data-page-offset (.getFirstDataPageOffset metadata)
                               :dictionary-page-offset (.getDictionaryPageOffset metadata)
                               :num-values (.getValueCount metadata)
                               :uncompressed-size (.getTotalUncompressedSize metadata)
                               :total-size (.getTotalSize metadata)
                               :statistics (let [stats (.getStatistics metadata)]
                                             {:min (.genericGetMin stats)
                                              :max (.genericGetMax stats)
                                              :num-missing (.getNumNulls stats)})
                               :encodings (->> (.getEncodings metadata)
                                               (map #(get encoding-map % :unrecognized-encoding))
                                               set)}))))}))))


(defn- ->file-reader
  ^ParquetFileReader [data]
  (cond
    (instance? ParquetFileReader data)
    data
    (string? data)
    (ParquetFileReader/open (HadoopInput/file data))
    (instance? org.apache.hadoop.fs.Path data)
    (ParquetFileReader/open ^org.apache.hadoop.fs.Path data)
    (instance? InputFile data)
    (ParquetFileReader/open ^InputFile data)
    :else
    (errors/throwf "Unrecognized parquet input type: %s" (type data))))


(defn parquet->metadata-seq
  "Given a local parquet file, return a sequence of metadata, one for each row-group.
  A row-group maps directly to a dataset."
  [path]
  (with-open [reader (->file-reader path)]
    (parquet-reader->metadata reader)))


(defn parquet->ds-seq
  "Given a string, hadoop path, or a parquet InputFile, return a sequence of datasets.
  Column will have parquet metadata merged into their normal metadata.
  Reader will be closed upon termination of the sequence."
  ([path options]
   (let [reader (->file-reader path)
         metadata (parquet-reader->metadata reader)]
     (read-next-dataset reader options (first metadata) (rest metadata))))
  ([^String path]
   (parquet->ds-seq path nil)))



(defmethod ds-io/data->dataset :parquet
  [input options]
  (let [data-file (io/file input)
        _ (errors/when-not-errorf
           (.exists data-file)
           "Only on-disk files work with parquet.  %s does not resolve to a file"
           input)
        dataset-seq (parquet->ds-seq (.getCanonicalPath data-file) options)]
    (errors/when-not-errorf
     (== 1 (count dataset-seq))
     "Zero or multiple datasets found in parquet file %s" input)
    (first dataset-seq)))


;;https://github.com/apache/parquet-mr/blob/72738f59920cb8a875757d5fbb0a70bd7115fdcf/parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnWriteStoreV1.java
;;https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileWriter.java
;;https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageWriteStore.java


(defn- pos-out-stream
  [str-path]
  (let [fchannel (FileChannel/open
                  (Paths/get str-path (into-array String []))
                  (into-array OpenOption [StandardOpenOption/WRITE
                                          StandardOpenOption/CREATE
                                          StandardOpenOption/TRUNCATE_EXISTING]))]
    (proxy [PositionOutputStream] []
      (getPos [] (.position fchannel))
      (close [] (.close fchannel))
      (flush [] (.force fchannel false))
      (write
        ([data]
         (if (bytes? data)
           (let [^bytes data data]
             (.write fchannel (ByteBuffer/wrap data)))
           (let [data (unchecked-byte data)
                 data-ary (byte-array [data])
                 buf (ByteBuffer/wrap data-ary)]
             (.write fchannel buf))))
        ([data off len]
         (let [byte-buf (ByteBuffer/wrap data)
               _ (.position byte-buf off)
               _ (.limit byte-buf (+ off len))]
           (assert (== (.remaining byte-buf) len))
           (.write fchannel byte-buf)))))))


(defn- local-file-output-file
  ^OutputFile [^String str-path]
  (let [file (io/file str-path)
        _ (errors/when-not-error
           file
           "Only writing to filesystem files is supported")
        str-path (.getCanonicalPath file)]
    (reify OutputFile
      (create [f block-size-hint]
        (pos-out-stream str-path))
      (createOrOverwrite [f block-size-hint]
        (pos-out-stream str-path))
      (supportsBlockSize [f] false)
      ;;Fair roll of the dice
      (defaultBlockSize [f] 4096))))


(defn- ->file-writer
  ^ParquetFileWriter [path ^Configuration config ^MessageType schema
                      ^ParquetProperties properties]
  (let [^OutputFile of (cond
                         (instance? OutputFile path)
                         path
                         (instance? org.apache.hadoop.fs.Path path)
                         (HadoopOutputFile/fromPath path config)
                         (string? path)
                         (local-file-output-file path)
                         :else
                         (errors/throwf "Unrecognized parquet file writer input: %s" (type path)))]
    (ParquetFileWriter. of schema ParquetFileWriter$Mode/CREATE
                        ParquetWriter/DEFAULT_BLOCK_SIZE
                        ParquetWriter/MAX_PADDING_SIZE_DEFAULT
                        (.getColumnIndexTruncateLength properties)
                        (.getStatisticsTruncateLength properties)
                        (.getPageWriteChecksumEnabled properties))))


(def ^:private int32-set #{:int8 :uint8 :int16 :uint16 :int32 :local-date})
(def ^:private int64-set #{:uint32 :int64 :uint64 :instant})
(def ^:private str-set #{:string :text})


(defn- column->field
  ^Type [col]
  (let [colmeta (meta col)
        datatype (-> (:datatype colmeta)
                     (packing/unpack-datatype)
                     (casting/un-alias-datatype))
        colname (:name colmeta)
        missing (ds-col/missing col)]
    (PrimitiveType.
     ^Type$Repetition
     (if (== 0 (dtype-base/ecount missing))
       Type$Repetition/REQUIRED
       Type$Repetition/OPTIONAL)

     ^PrimitiveType$PrimitiveTypeName
     (cond
       (= :boolean datatype) PrimitiveType$PrimitiveTypeName/BOOLEAN
       (int32-set datatype) PrimitiveType$PrimitiveTypeName/INT32
       (int64-set datatype) PrimitiveType$PrimitiveTypeName/INT64
       (str-set datatype) PrimitiveType$PrimitiveTypeName/BINARY
       (= :float32 datatype) PrimitiveType$PrimitiveTypeName/FLOAT
       (= :float64 datatype) PrimitiveType$PrimitiveTypeName/DOUBLE
       :else
       (errors/throwf "Unsupported datatype for parquet writing: %s" datatype))

     (int 0)
     ^String (ds-utils/column-safe-name colname)
     ^OriginalType
     (case datatype
       :int8 OriginalType/INT_8
       :int16 OriginalType/INT_16
       :int32 OriginalType/INT_32
       :int64 OriginalType/INT_64
       :uint8 OriginalType/UINT_8
       :uint16 OriginalType/UINT_16
       :uint32 OriginalType/UINT_32
       :uint64 OriginalType/UINT_64
       :string OriginalType/UTF8
       :text OriginalType/UTF8
       :local-date OriginalType/DATE
       :instant OriginalType/TIMESTAMP_MICROS
       nil)
     nil nil)))


(defn- ds->schema
  ^MessageType [ds]
  (let [^List fields (->> (vals ds)
                          (mapv column->field))]
    (MessageType. (ds-utils/column-safe-name (:name (meta ds)))
                  fields)))


(defn- compression-codec
  ^CompressionCodecName [kwd]
  (get (set/map-invert comp-code-map) kwd CompressionCodecName/SNAPPY))


(defn- consume-boolean!
  [buffer ^RoaringBitmap missing ^ColumnWriter col-writer n-rows]
  (let [buffer (dtype-base/->reader buffer)]
    (fn [^long idx]
      (if (.contains missing idx)
        (.writeNull col-writer 0 0)
        (.write col-writer (.readBoolean buffer idx) (int 0) (int 1))))))


(defn- consume-int32!
  [buffer ^RoaringBitmap missing ^ColumnWriter col-writer n-rows]
  (let [buffer (-> (if (= :local-date (packing/unpack-datatype
                                       (dtype-base/elemwise-datatype buffer)))
                     (dtype-dt/long-temporal-field :epoch-days buffer)
                     buffer)
                   (dtype-base/->buffer))]
    (fn [^long idx]
        (if (.contains missing idx)
          (.writeNull col-writer 0 0)
          (.write col-writer (.readInt buffer idx) (int 0) (int 1))))))


(defn- consume-int64!
  [buffer ^RoaringBitmap missing ^ColumnWriter col-writer n-rows]
  (let [buffer (-> (if (= :instant
                          (packing/unpack-datatype
                           (dtype-base/elemwise-datatype buffer)))
                     (emap/emap dtype-dt/instant->microseconds-since-epoch :int64 buffer)
                     buffer)
                   (dtype-base/->buffer))]
    (fn [^long idx]
      (if (.contains missing idx)
        (.writeNull col-writer 0 0)
        (.write col-writer (.readLong buffer idx) (int 0) (int 1))))))


(defn- consume-string!
  [buffer ^RoaringBitmap missing ^ColumnWriter col-writer n-rows]
  (let [buffer (dtype-base/->reader buffer)]
    (fn [^long idx]
      (if (.contains missing idx)
        (.writeNull col-writer 0 0)
        (.write col-writer (Binary/fromString (str (.readObject buffer idx)))
                (int 0) (int 1))))))


(defn- consume-float32!
  [buffer ^RoaringBitmap missing ^ColumnWriter col-writer n-rows]
  (let [buffer (dtype-base/->reader buffer)]
    (fn [^long idx]
      (if (.contains missing idx)
        (.writeNull col-writer 0 0)
        (.write col-writer (.readFloat buffer idx)
                (int 0) (int 1))))))


(defn- consume-float64!
  [buffer ^RoaringBitmap missing ^ColumnWriter col-writer n-rows]
  (let [buffer (dtype-base/->reader buffer)]
    (fn [^long idx]
      (if (.contains missing idx)
        (.writeNull col-writer 0 0)
        (.write col-writer (.readDouble buffer idx)
                (int 0) (int 1))))))


(defn- write-column!
  [n-rows ds-col ^ColumnWriter col-writer ^ColumnDescriptor col-desc]
  (let [missing (ds-col/missing ds-col)
        datatype (-> (:datatype (meta ds-col))
                     (packing/unpack-datatype)
                     (casting/un-alias-datatype))]
    (cond
      (= :boolean datatype) (consume-boolean! ds-col missing col-writer n-rows)
      (int32-set datatype) (consume-int32! ds-col missing col-writer n-rows)
      (int64-set datatype) (consume-int64! ds-col missing col-writer n-rows)
      (str-set datatype) (consume-string! ds-col missing col-writer n-rows)
      (= :float32 datatype) (consume-float32! ds-col missing col-writer n-rows)
      (= :float64 datatype) (consume-float64! ds-col missing col-writer n-rows)
      :else
      (errors/throwf "Unsupported datatype for parquet writing: %s" datatype))))


(defn ds-seq->parquet
  "Write a sequence of datasets to a parquet file.  EAch dataset will be stored
  in a parquet row group in a single file.

 Options:

  * `:hadoop-configuration` - Either nil or an instance of `org.apache.hadoop.conf.Configuration`.
  * `:parquet-properties` - Either nil or an instance of `org.apache.parquet.column.ParquetProperties`.
  * `:parquet-compression-codec` - Either nil or an instance of `org.apache.parquet.hadoop.metadata.CompressionCodecName`.
  * `:parquet-validating?` - true if the columns should be validated after being written.  Defauts to true."
  ([path options ds-seq]
   (let [first-ds (first ds-seq)
         schema (ds->schema first-ds)
         ^Configuration configuration
         (or (:hadoop-configuration options)
             (Configuration.))
         ^ParquetProperties properties
         (or (:parquet-properties options)
             (-> (ParquetProperties/builder)
                 (.build)))
         ^CodecFactory codec-factory
         (or (:parquet-codec-factory options)
             (CodecFactory. configuration
                            (.getPageSizeThreshold properties)))
         compressor (or (:parquet-compressor options)
                        (.getCompressor codec-factory
                                        (compression-codec (:parquet-compression-codec
                                                            options))))
         file-writer (->file-writer path configuration schema properties)
         validating? (boolean (get options :parquet-validating? true))
         message-column-io (.getColumnIO (ColumnIOFactory. validating?) schema)
         col-descriptors (->> (.getLeaves message-column-io)
                              (map (fn [^PrimitiveColumnIO prim-io]
                                     (.getColumnDescriptor prim-io))))
         colnames (map (comp :name meta) (vals first-ds))]
     (.start file-writer)
     (doseq [[_group-ordinal ds] (map-indexed vector ds-seq)]
       (let [ds-cols (ds-base/columns (ds-base/select-columns ds colnames))
             page-write-store (FilePageWriteStore.
                               compressor
                               schema
                               (.getAllocator properties)
                               (.getColumnIndexTruncateLength properties)
                               (.getPageWriteChecksumEnabled properties))
             column-store (.newColumnWriteStore properties schema page-write-store)
             colwriters (->> col-descriptors
                             (map (fn [^ColumnDescriptor col-desc]
                                    (.getColumnWriter column-store col-desc))))
             n-rows (ds-base/row-count ds)
             ^List col-write-fns (mapv (partial write-column! n-rows) ds-cols colwriters col-descriptors)
             n-col-write-fns (.size col-write-fns)]
         (.startBlock file-writer n-rows)
         (dotimes [idx n-rows]
           (dotimes [col-idx n-col-write-fns]
             ((.get col-write-fns col-idx) idx))
           (.endRecord column-store))
         (.flush column-store)
         (.flushToFileWriter page-write-store file-writer)
         (.endBlock file-writer)))
     (.end file-writer {"created-by" "tech.v3.dataset"})
     :ok))
  ([path ds-seq]
   (ds-seq->parquet path nil ds-seq)))


(defn ds->parquet
  "Write a dataset to a parquet file.  Many parquet options are possible;
  these can also be passed in via ds/->write!

  Options:

  * `:hadoop-configuration` - Either nil or an instance of `org.apache.hadoop.conf.Configuration`.
  * `:parquet-properties` - Either nil or an instance of `org.apache.parquet.column.ParquetProperties`.
  * `:parquet-compression-codec` - Either nil or an instance of `org.apache.parquet.hadoop.metadata.CompressionCodecName`.
  * `:parquet-validating?` - true if the columns should be validated after being written.  Defauts to true."
  ([ds path options]
   (ds-seq->parquet path options [ds]))
  ([ds path]
   (ds->parquet ds path nil)))


(defmethod ds-io/dataset->data! :parquet
  [ds output options]
  (ds->parquet ds output options))


(comment
  (do (require '[tech.v3.dataset :as ds])
      (def stocks (ds/->dataset "test/data/stocks.csv")))


  )
