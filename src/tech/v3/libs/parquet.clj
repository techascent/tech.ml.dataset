(ns tech.v3.libs.parquet
  "Support for reading Parquet files. You must require this namespace to
  enable parquet read/write support.

  Supported datatypes:

  * all numeric types
  * strings
  * java.time LocalDate, Instant
  * UUIDs (get read/written as strings in accordance to R's write_parquet function)


  Parsing parquet file options include more general io/->dataset options:

  * `:key-fn`
  * `:column-whitelist`
  * `:column-blacklist`
  * `:parser-fn`


  Please include these dependencies in your project:

```clojure
[org.apache.parquet/parquet-hadoop \"1.11.0\"]
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
            [tech.v3.io.url :as io-url]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.emap :as emap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.parallel.for :as parallel-for]
            [clojure.string :as s]
            [clojure.set :as set]
            [clojure.tools.logging :as log])

  ;; Behold my Kindom of Nouns...And Tremble!!!!
  (:import [smile.io HadoopInput]
           [tech.v3.datatype PrimitiveList Buffer]
           [tech.v3.dataset Text ParquetRowWriter]
           [org.apache.hadoop.conf Configuration]
           [java.time.temporal TemporalAccessor TemporalField ChronoField]
           [org.apache.parquet.hadoop ParquetFileReader ParquetWriter
            ParquetFileWriter$Mode CodecFactory]
           [org.apache.parquet.hadoop.util HadoopOutputFile]
           [org.apache.parquet.hadoop.metadata BlockMetaData ColumnChunkMetaData
            CompressionCodecName]
           [org.apache.parquet.column ColumnDescriptor ColumnReader Encoding
            ParquetProperties ParquetProperties$WriterVersion ColumnWriter]
           [org.apache.parquet.column.page PageReadStore]
           [org.apache.parquet.column.impl ColumnReadStoreImpl]
           [org.apache.parquet.io OutputFile PositionOutputStream InputFile
            ColumnIOFactory PrimitiveColumnIO]
           [org.apache.parquet.io.api GroupConverter PrimitiveConverter Binary
            RecordConsumer]
           [org.apache.parquet.schema OriginalType MessageType
            PrimitiveType$PrimitiveTypeName Type$Repetition Type PrimitiveType]
           [org.apache.hadoop.fs Path]
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
(def ^:private test-file-2 "test/data/part-00000-74d3eb51-bc9c-4ba5-9d13-9e0d71eea31f.c000.snappy.parquet")
(def ^:private perf-benchmark "/home/chrisn/dev/geni-performance-benchmark/geni/data/performance-benchmark-data/part-00000-021194b8-aa1b-4cd8-b641-e9902dea79a6-c000.snappy.parquet")

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

(defn- col-def->col-type
  [^ColumnDescriptor col-def]
  (-> col-def
      (.getPrimitiveType)
      (.getPrimitiveTypeName)))


(defn- col-def->col-orig-type
  ^OriginalType [^ColumnDescriptor col-def]
  (-> col-def
      (.getPrimitiveType)
      (.getOriginalType)))


(defn- col-metadata->min-max
  [metadata]
  (when (and (get-in metadata [:statistics :min])
             (get-in metadata [:statistics :max]))
    [(get-in metadata [:statistics :min]) (get-in metadata [:statistics :max])]))


(defn- make-column-iterator
  [^ColumnReader col-rdr ^ColumnDescriptor col-def read-fn]
  (ColumnIterator. col-rdr read-fn
                   (.getMaxDefinitionLevel col-def)
                   (.getTotalValueCount col-rdr)))


(defn- make-column-iterable
  [^ColumnReader col-rdr ^ColumnDescriptor col-def dtype read-fn]
  (reify
    dtype-proto/PElemwiseDatatype
    (elemwise-datatype [rdr] dtype)
    Iterable
    (iterator [rdr] (make-column-iterator col-rdr col-def read-fn))))


(defmulti ^:private typed-col->iterable
  (fn [col-rdr col-def metadata]
    (col-def->col-type col-def)))


(defmethod typed-col->iterable :default
  [col-rdr col-def metadata]
  (make-column-iterable col-rdr col-def :object read-byte-array))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/BOOLEAN
  [col-rdr col-def metadata]
  (make-column-iterable col-rdr col-def :boolean read-boolean))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/BINARY
  [col-rdr col-def metadata]
  (let [org-type (col-def->col-orig-type col-def)]
    (if (= org-type OriginalType/UTF8)
      (make-column-iterable col-rdr col-def :string read-str)
      (make-column-iterable col-rdr col-def :object read-byte-array))))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/INT96
  [col-rdr col-def metadata]
  (make-column-iterable col-rdr col-def :instant read-instant))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/INT32
  [col-rdr col-def metadata]
  (let [org-type (col-def->col-orig-type col-def)]
    (if (= org-type OriginalType/DATE)
      (make-column-iterable col-rdr col-def :epoch-days read-int)
      (let [[col-min col-max] (col-metadata->min-max metadata)
            dtype (cond
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
                    :int32)]
        (make-column-iterable col-rdr col-def dtype read-int)))))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/INT64
  [col-rdr col-def metadata]
  (let [org-type (col-def->col-orig-type col-def)
        [col-min col-max] (col-metadata->min-max metadata)]
    (condp = org-type
      OriginalType/TIMESTAMP_MILLIS
      (make-column-iterable col-rdr col-def :instant
                            #(-> (read-long %)
                                 (dtype-dt/milliseconds-since-epoch->instant)))
      OriginalType/TIMESTAMP_MICROS
      (make-column-iterable col-rdr col-def :instant
                            #(-> (read-long %)
                                 (dtype-dt/microseconds-since-epoch->instant)))
      (let [dtype (cond
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
                    :int64)]
        (make-column-iterable col-rdr col-def dtype read-long)))))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/DOUBLE
  [col-rdr col-def metadata]
  (make-column-iterable col-rdr col-def :float64 read-double))


(defmethod typed-col->iterable PrimitiveType$PrimitiveTypeName/FLOAT
  [col-rdr col-def metadata]
  (make-column-iterable col-rdr col-def :float32 read-float))


(defn- dot-notation
  [str-ary]
  (s/join "." str-ary))


(defn- fixed-datatype-parser
  [dtype]
  (let [container-dtype (packing/pack-datatype dtype)
        container (col-base/make-container container-dtype)
        missing-value (col-base/datatype->missing-value
                       (packing/unpack-datatype container-dtype))
        missing (bitmap/->bitmap)]
    (reify col-parsers/PParser
      (add-value! [p idx value]
        (if (= value :tech.ml.dataset.parse/missing)
          (do
            (.addObject container missing-value)
            (.add missing (long idx)))
          (.addObject container value)))
      (finalize! [p n-rows]
        (col-parsers/finalize-parser-data! container missing nil nil
                                           missing-value n-rows)))))


(defn- parse-column-data
  [^ColumnReader col-rdr ^ColumnDescriptor col-def n-rows
   parse-context key-fn metadata]
  (let [n-rows (long n-rows)
        col-name (dot-notation (.getPath col-def))
        ^Iterable iterable (typed-col->iterable col-rdr col-def metadata)
        iterator (.iterator iterable)
        col-parser (or (parse-context col-name)
                       (fixed-datatype-parser (dtype/elemwise-datatype iterable)))
        ^Buffer row-rep-counts (when-not (== n-rows (.getTotalValueCount col-rdr))
                                 (-> (int-array n-rows)
                                     (dtype/->buffer)))
        {:keys [data missing metadata]}
        ;;The user specified a specific parser for this datatype
        (loop [continue? (.hasNext iterator)
               row-idx -1
               row 0]
          (if continue?
            (let [rep-level (.getCurrentRepetitionLevel col-rdr)
                  col-data (.next iterator)
                  row-idx (if (== 0 rep-level) (unchecked-inc row-idx) row-idx)]
              (col-parsers/add-value! col-parser row col-data)
              (when row-rep-counts
                (.accumPlusLong row-rep-counts row-idx 1))
              (recur (.hasNext iterator) row-idx (unchecked-inc row)))
            (col-parsers/finalize! col-parser n-rows)))]
    (col-impl/new-column (key-fn col-name) data
                         (merge metadata (when row-rep-counts
                                           {:row-rep-counts row-rep-counts}))
                         missing)))


(defn- parse-parquet-column
  [column-whitelist column-blacklist ^ColumnReadStoreImpl col-read-store
   n-rows parse-context key-fn
   ^ColumnDescriptor col-def col-metadata]
  (let [n-rows (long n-rows)
        cname (key-fn (.. col-def getPrimitiveType getName))
        whitelisted? (or (not column-whitelist)
                         (column-whitelist cname))
        blacklisted? (and column-blacklist
                          (column-blacklist cname))]
    (when (and whitelisted? (not blacklisted?))
      (try
        (let [reader (.getColumnReader col-read-store col-def)
              retval (parse-column-data reader col-def n-rows
                                        parse-context key-fn col-metadata)
              converter (condp = (dtype-base/elemwise-datatype retval)
                          :string
                          (fn [^Binary data] (String. (.getBytes data)))
                          :text
                          (fn [^Binary data] (String. (.getBytes data)))
                          :packed-local-date
                          (fn [^long data] (LocalDate/ofEpochDay data))
                          ;;Strip non-numeric representations else we risk not being
                          ;;able to save to nippy
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


(defn- rep-count-indexes
  (^Buffer [^ints max-rep-counts ^RoaringBitmap original-missing]
   (let [n-rows (alength max-rep-counts)
         index-data (dtype/make-list :int32)
         missing (RoaringBitmap.)]
     (dotimes [row-idx n-rows]
       (let [missing? (.contains original-missing row-idx)]
         (dotimes [repeat-idx (aget max-rep-counts row-idx)]
           (when (or missing? (not= repeat-idx 0)) (.add missing (.size index-data)))
           (.addLong index-data row-idx))))
     [(dtype/as-array-buffer index-data) missing]))
  (^Buffer [^ints max-rep-counts ^RoaringBitmap original-missing col-rep-counts]
   (let [n-rows (alength max-rep-counts)
         index-data (dtype/make-list :int32)
         col-rep-counts (dtype/->buffer col-rep-counts)
         missing (RoaringBitmap.)]
     (loop [row-idx 0
            col-original-idx 0]
       (if (< row-idx n-rows)
         (let [col-n-repeat (.readLong col-rep-counts row-idx)
               end-col-idx (unchecked-dec (+ col-n-repeat col-original-idx))]
           (dotimes [col-idx col-n-repeat]
             (let [rel-missing (+ col-idx col-original-idx)]
               (when (.contains original-missing rel-missing)
                 (.add missing (.size index-data)))
               (.addLong index-data rel-missing)))
           (dotimes [extra-idx (- (aget max-rep-counts row-idx)
                                  col-n-repeat)]
             (.add missing (.size index-data))
             (.addLong index-data end-col-idx))
           (recur (unchecked-inc row-idx) (unchecked-inc end-col-idx)))))
     [(dtype/as-array-buffer index-data) missing])))


(defn- scatter-rows
  [columns row-rep-counts]
  (let [n-rows (count (first row-rep-counts))
        max-rep-counts (int-array n-rows)
        row-rep-counts (vec row-rep-counts)
        n-rep-cols (count row-rep-counts)]
    ;;Go row by row and for each row take the find the max repetition count
    (parallel-for/parallel-for
     row-idx n-rows
     (loop [col-idx 0
            max-val 0]
       (if (< col-idx n-rep-cols)
         (recur (unchecked-inc col-idx) (max max-val (long ((row-rep-counts col-idx)
                                                            row-idx))))
         (aset max-rep-counts (int row-idx) (int max-val)))))
    (->>
     columns
     (pmap (fn [column]
             (let [original-missing (ds-col/missing column)
                   [col-indexes new-missing]
                   (if-let [col-rep-counts (:row-rep-counts (meta column))]
                     (rep-count-indexes max-rep-counts original-missing col-rep-counts)
                     (rep-count-indexes max-rep-counts original-missing))
                   ;;We clear the old missing set because that was taken care of above.
                   ;;and select, when it has a missing set, has to go index by index and
                   ;;make a new missing set.
                   new-col (-> (ds-col/set-missing column nil)
                               (ds-col/select col-indexes))]
               (ds-col/set-missing new-col new-missing))))
     (vec))))


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
        col-parser (partial parse-parquet-column column-whitelist column-blacklist
                            col-read-store n-rows parse-context key-fn)
        initial-columns (->> (map col-parser
                                  (.getColumns schema)
                                  (:columns block-metadata))
                             (remove nil?)
                             (vec))
        rep-counts (->> (map (comp :row-rep-counts meta) initial-columns)
                        (remove nil?)
                        (vec))
        columns (if (seq rep-counts)
                  (scatter-rows initial-columns rep-counts)
                  ;;handle repetitions
                  initial-columns)
        retval (ds-impl/new-dataset options columns)]
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


(def ^:private int32-set #{:int8 :uint8 :int16 :uint16 :int32
                           :local-date :epoch-days})
(def ^:private int64-set #{:uint32 :int64 :uint64 :instant})
(def ^:private str-set #{:string :text})


(defn- colname->fieldname
  ^String [col]
  (ds-utils/column-safe-name (:name (meta col))))


(defn- column->field
  ^Type [col]
  (let [colmeta (meta col)
        datatype (-> (:datatype colmeta)
                     (packing/unpack-datatype))
        datatype (if (not= datatype :epoch-days)
                   (casting/un-alias-datatype datatype)
                   datatype)
        colname (colname->fieldname col)
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
     ^String colname
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
       :epoch-days OriginalType/DATE
       :instant OriginalType/TIMESTAMP_MICROS
       nil)
     nil nil)))


(defn- ds->schema
  ^MessageType [ds]
  (let [^List fields (->> (vals ds)
                          (mapv column->field))]
    (MessageType. (colname->fieldname ds) fields)))


(defn- compression-codec
  ^CompressionCodecName [kwd]
  (get (set/map-invert comp-code-map) kwd CompressionCodecName/SNAPPY))


(defn- prepare-for-parquet
  "In preparation for parquet so far we need to only map uuid columns
  to text columns."
  [ds]
  (reduce (fn [ds col]
            (let [colmeta (meta col)]
              (if (= :uuid (:datatype colmeta))
                ;;Column-map respects missing
                (ds-base/update-column
                 ds (:name colmeta)
                 (fn [col]
                   (ds-col/column-map #(Text. (str %))
                                      :text
                                      col)))
               ds)))
          ds
          (vals ds)))

(defn- make-record-writer
  [datatype]
  (cond
    (identical? :boolean datatype)
    (fn [^RecordConsumer consumer col-val]
      (.addBoolean consumer (boolean col-val)))

    (int32-set datatype)
    (fn [^RecordConsumer consumer col-val]
      (.addInteger consumer
                   (if (= :local-date datatype)
                     (unchecked-int
                      (.getLong ^TemporalAccessor col-val
                                ChronoField/EPOCH_DAY))
                     (unchecked-int col-val))))

    (int64-set datatype)
    (fn [^RecordConsumer consumer col-val]
      (.addLong consumer
                (if (= :instant datatype)
                  (dtype-dt/instant->microseconds-since-epoch col-val)
                  (unchecked-long col-val))))
    (str-set datatype)
    (fn [^RecordConsumer consumer col-val]
      (.addBinary consumer (Binary/fromString col-val)))

    (identical? :float32 datatype)
    (fn [^RecordConsumer consumer col-val]
      (.addFloat consumer (unchecked-float col-val)))

    (identical? :float64 datatype)
    (fn [^RecordConsumer consumer col-val]
      (.addDouble consumer (unchecked-double col-val)))
    :else
    (errors/throwf "Unsupported datatype for parquet writing: %s" datatype)))


(defn- ds-row->parquet
  [^List columns ^long row-idx ^RecordConsumer consumer]
  (.startMessage consumer)
  (dotimes [col-idx (.size columns)]
    (let [col-data (.get columns col-idx)
          colname (col-data 0)
          writer (col-data 1)
          missing (col-data 2)
          ^Buffer col-rdr (col-data 3)]
      (when-not (.contains ^RoaringBitmap missing (unchecked-int row-idx))
        (.startField consumer colname col-idx)
        (let [col-val (.readObject col-rdr row-idx)]
          (writer consumer col-val))
        (.endField consumer colname col-idx))))
  (.endMessage consumer))


(defn- ->hadoop-path
  ^Path [fpath]
  (if (instance? Path fpath)
    fpath
    (let [path-url (if (io-url/url? fpath)
                     fpath
                     ;;Allow local files to load.
                     (let [nio-path (Paths/get fpath (into-array String []))
                           nio-path (if (.isAbsolute nio-path)
                                      nio-path
                                      (Paths/get (System/getProperty "user.dir")
                                                 (into-array String [fpath])))]
                       (.toUri ^java.nio.file.Path nio-path)))]
      (Path. (str path-url)))))


(defn ds-seq->parquet
  "Write a sequence of datasets to a parquet file.  Parquet will break the data
  stream up according to parquet file properties.

 Options:

  * `:hadoop-configuration` - Either nil or an instance of
  `org.apache.hadoop.conf.Configuration`.
  * `:compression-codec` - Either nil or an instance of
  `org.apache.parquet.hadoop.metadata.CompressionCodecName`.  Defaults to
  `org.apache.parquet.column.ParquetProperties.WriterVersion`.
  * `:block-size` - Defaults to `ParquetWriter/DEFAULT_BLOCK_SIZE`.
  * `:page-size` - Defaults to `ParquetWriter/DEFAULT_PAGE_SIZE`.
  * `:dictionary-page-size` - Defaults to `ParquetWriter/DEFAULT_PAGE_SIZE`.
  * `:dictionary-enabled?` - Defaults to
     `ParquetWriter/DEFAULT_IS_DICTIONARY_ENABLED`.
  * `:validating?` - Defaults to `ParquetWriter/DEFAULT_IS_VALIDATING_ENABLED`.
  * `:writer-version` - Defaults to `ParquetWriter/DEFAULT_WRITER_VERSION`."
  ([path {:keys [block-size page-size dictionary-page-size
                 dictionary-enabled? validating?
                 writer-version]
          :or {block-size ParquetWriter/DEFAULT_BLOCK_SIZE
               page-size ParquetWriter/DEFAULT_PAGE_SIZE
               dictionary-page-size ParquetWriter/DEFAULT_PAGE_SIZE
               dictionary-enabled? ParquetWriter/DEFAULT_IS_DICTIONARY_ENABLED
               validating? ParquetWriter/DEFAULT_IS_VALIDATING_ENABLED
               writer-version ParquetWriter/DEFAULT_WRITER_VERSION
               } :as options}
    ds-seq]
   (let [ds-seq (map prepare-for-parquet ds-seq)
         first-ds (first ds-seq)
         ;;message type
         schema (ds->schema first-ds)
         ^Configuration configuration
         (or (:hadoop-configuration options)
             (Configuration.))
         row-writer (ParquetRowWriter. ds-row->parquet schema {})]
     (when (.exists (io/file path))
       (.delete (io/file path)))
     (with-open [writer (ParquetWriter.
                         (->hadoop-path path)
                         ^ParquetFileWriter$Mode
                         (if (io/exists? path)
                           ParquetFileWriter$Mode/OVERWRITE
                           ParquetFileWriter$Mode/CREATE)
                         row-writer
                         (compression-codec (:parquet-compression-codec options))
                         (int block-size)
                         (int page-size)
                         (int dictionary-page-size)
                         (boolean dictionary-enabled?)
                         (boolean validating?)
                         writer-version
                         configuration)]
       (doseq [ds ds-seq]
         (set! (.dataset row-writer) (mapv (fn [col]
                                             ;;Deconstruct the column
                                             [(colname->fieldname col)
                                              (-> (dtype/elemwise-datatype col)
                                                  (packing/unpack-datatype)
                                                  (casting/un-alias-datatype)
                                                  (make-record-writer))
                                              (ds-col/missing col)
                                              (dtype-base/->reader col)])
                                           (ds-base/columns ds)))
         (dotimes [idx (ds-base/row-count ds)]
           (.write writer idx))))
     :ok))
  ([path ds-seq]
   (ds-seq->parquet path nil ds-seq)))


(defn ds->parquet
  "Write a dataset to a parquet file.  Many parquet options are possible;
  these can also be passed in via ds/->write!

  Options are the same as ds-seq->parquet."
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
