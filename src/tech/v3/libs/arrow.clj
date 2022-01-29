(ns tech.v3.libs.arrow
  "Support for reading/writing apache arrow datasets.  Datasets may be memory mapped
  but default to being read via an input stream.

  Supported datatypes:

  * All numeric types - `:uint8`, `:int8`, `:uint16`, `:int16`, `:uint32`, `:int32`,
  `:uint64`, `:int64`, `:float32`, `:float64`, `:boolean`.
  * String types - `:string`, `:text`.  During write you have the option to always write
  data as text which can be more efficient in the memory-mapped read case as it doesnt'
  require the creation of string tables at load time.
  * Datetime Types - `:local-date`, `:local-time`, `:instant`.  During read you have the
  option to keep these types in their source numeric format e.g. 32 bit `:epoch-days`
  for `:local-date` datatypes.  This format can make some types of processing, such as
  set creation, more efficient.


  When writing a dataset an arrow file with a single record set is created.  When
  writing a sequence of datasets downstream schemas must be compatible with the schema
  of the initial dataset so for instance a conversion of int32 to double is fine but
  double to int32 is not.

  mmap support on systems running JDK-17 requires the foreign or memory module to be
  loaded.  Appropriate JVM arguments can be found
  [here](https://github.com/techascent/tech.ml.dataset/blob/0524ddd5bbcb9421a0f11290ec8a01b7795dcff9/project.clj#L69).


  ## Required Dependencies

  In order to support both memory mapping and JDK-17, we only rely on the Arrow SDK's
  flatbuffer and schema definitions:

```clojure
  [org.apache.arrow/arrow-vector \"6.0.0\"]

  ;;Compression codecs
  [org.lz4/lz4-java \"1.8.0\"]
  [com.github.luben/zstd-jni \"1.5.1-1\"]
```"
  (:require [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.utils :as ml-utils]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.resource :as resource]
            [tech.v3.io :as io]
            [clojure.tools.logging :as log]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.core.protocols :as clj-proto]
            [clojure.datafy :refer [datafy]])
  (:import [org.apache.arrow.vector.ipc.message MessageSerializer]
           [org.apache.arrow.flatbuf Message DictionaryBatch RecordBatch
            FieldNode Buffer BodyCompression BodyCompressionMethod]
           [org.roaringbitmap RoaringBitmap]
           [com.google.flatbuffers FlatBufferBuilder]
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.vector.types.pojo Field Schema ArrowType$Int
            ArrowType$Utf8 ArrowType$Timestamp ArrowType$Time DictionaryEncoding FieldType
            ArrowType$FloatingPoint ArrowType$Bool ArrowType$Date ArrowType$Duration
            ArrowType$LargeUtf8]
           [org.apache.arrow.flatbuf CompressionType]
           [org.apache.arrow.vector.types MetadataVersion]
           [org.apache.arrow.vector.ipc WriteChannel]
           [tech.v3.dataset.string_table StringTable]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset Text]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype ObjectReader ArrayHelpers ByteConversions BooleanBuffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [java.io OutputStream InputStream ByteArrayOutputStream ByteArrayInputStream]
           [java.nio ByteBuffer]
           [java.util List ArrayList Map]
           [java.time ZoneId]
           [java.nio.channels WritableByteChannel]
           ;;Compression codecs
           [com.github.luben.zstd Zstd]
           [org.apache.commons.compress.compressors.lz4 FramedLZ4CompressorInputStream
            FramedLZ4CompressorOutputStream]
           [net.jpountz.lz4 LZ4Factory]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Arrow stream adapters
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- create-in-memory-byte-channel
  ^WritableByteChannel []
  (let [storage (dtype/make-list :int8)]
    (reify
      WritableByteChannel
      (isOpen [this] true)
      (close [this])
      (write [this bb]
        (let [retval (.remaining bb)]
          (.addAll storage (dtype/->buffer bb))
          (.position bb (+ (.position bb) (.remaining bb)))
          retval))
      dtype-proto/PToBuffer
      (convertible-to-buffer? [this] true)
      (->buffer [this] (dtype-proto/->buffer storage)))))


(defn- arrow-in-memory-writer
  ^WriteChannel []
  (let [storage (create-in-memory-byte-channel)]
    {:channel (WriteChannel. storage)
     :storage storage}))


(defn- arrow-output-stream-writer
  ^WriteChannel [^OutputStream ostream]
  (let [closed?* (atom false)]
    (-> (reify WritableByteChannel
          (isOpen [this] @closed?*)
          (close [this]
            (when (compare-and-set! closed?* false true)
              (.close ostream)))
          (write [this bb]
            (let [written (.remaining bb)]
              (.write ostream (dtype/->byte-array bb))
              (.position bb (+ (.position bb) written))
              written)))
        (WriteChannel.))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Compression -
;; Compression happens in jvm-heap land.
;; Decompression happens in native-heap land.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- ensure-bytes-array-buffer
  ^ArrayBuffer [data]
  (if-let [ary-buf (dtype/as-array-buffer data)]
    (if (= :int8 (dtype/elemwise-datatype ary-buf))
      ary-buf
      (dtype/make-container :int8 data))))


(defn- create-zstd-compressor
  [comp-map]
  (assert (= :zstd (get comp-map :compression-type)))
  (let [comp-level (int (get comp-map :level 3))]
    (fn [compbuf dstbuf]
      (let [ninput (dtype/ecount compbuf)
            maxlen (Zstd/compressBound ninput)
            ^bytes dstbuf (-> (if (< (dtype/ecount dstbuf) maxlen)
                                (byte-array maxlen)
                                dstbuf))
            srcbuf (ensure-bytes-array-buffer compbuf)
            ^bytes src-data (.ary-data srcbuf)
            n-written (Zstd/compressByteArray
                       dstbuf 0 maxlen
                       src-data
                       (unchecked-int (.offset srcbuf))
                       (unchecked-int (.n-elems srcbuf))
                       comp-level)]
        {:writer-cache dstbuf
         :dst-buffer (dtype/sub-buffer dstbuf 0 n-written)}))))


(defn- create-zstd-decompressor
  []
  (fn [compbuf resbuf]
    (Zstd/decompress ^ByteBuffer (nio-buffer/as-nio-buffer resbuf)
                     ^ByteBuffer (nio-buffer/as-nio-buffer compbuf))
    resbuf))


(defn- create-apache-lz4-frame-compressor
  [comp-map]
  (assert (= :lz4 (get comp-map :compression-type)))
  (fn [compbuf dstbuf]
    (let [^ByteArrayOutputStream dstbuf (or dstbuf (ByteArrayOutputStream.))
          os (FramedLZ4CompressorOutputStream. dstbuf)
          srcbuf (ensure-bytes-array-buffer compbuf)
          ^bytes src-data (.ary-data srcbuf)]
      (.write os src-data (unchecked-int (.offset srcbuf)) (unchecked-int (.n-elems srcbuf)))
      (.finish os)
      (let [final-bytes (.toByteArray dstbuf)]
        (.reset dstbuf)
        {:writer-cache dstbuf
         :dst-buffer final-bytes}))))


(defn- create-lz4-frame-compressor
  [comp-map]
  (assert (= :lz4 (get comp-map :compression-type)))
  (let [fact (LZ4Factory/fastestInstance)
        compressor (.fastCompressor fact)]
    (fn [srcbuf dstbuf]
      (let [maxlen (.maxCompressedLength compressor (dtype/ecount srcbuf))
            ^bytes dstbuf (-> (if (< (dtype/ecount dstbuf) maxlen)
                                (byte-array maxlen)
                                dstbuf))
            srcbuf (ensure-bytes-array-buffer srcbuf)
            ^bytes src-data (.ary-data srcbuf)
            n-written
            (.compress compressor
                       src-data
                       (unchecked-int (.offset srcbuf))
                       (unchecked-int (.n-elems srcbuf))
                       dstbuf 0 maxlen)]
        {:writer-cache dstbuf
         :dst-buffer (dtype/sub-buffer dstbuf 0 n-written)}))))


(defn- create-lz4-decompressor
  []
  (let [fact (LZ4Factory/fastestInstance)
        decompressor (.fastDecompressor fact)]
    (fn [compbuf dstbuf]
      (.decompress decompressor
                   ^ByteBuffer (nio-buffer/as-nio-buffer compbuf)
                   ^ByteBuffer (nio-buffer/as-nio-buffer dstbuf))
      dstbuf)))


(def ^:private compression-info
  {:lz4 {:file-type CompressionType/LZ4_FRAME
         :compressor-fn create-lz4-frame-compressor
         :decompressor-fn create-lz4-decompressor}
   :zstd {:file-type CompressionType/ZSTD
          :compressor-fn create-zstd-compressor
          :decompressor-fn create-zstd-decompressor}})


(defn- create-compressor
  "Returns a function that takes two arguments, first is the buffer to be compressed
  and second is a cache argument that is returned upon use of the compressor to allow
  using the same buffer to compress data to.  Compression happens in jvm-heap land
  so the buffer to be compressed will be copied to a byte array."
  [comp-map]
  (if-let [comp-data (get compression-info (comp-map :compression-type))]
    ((comp-data :compressor-fn) comp-map)
    (throw (Exception. (format "Unrecognized compressor map %s" comp-map)))))


(def ^:private file-type->compression-kwd
  (->> compression-info
       (map (fn [[k data]]
              [(data :file-type) k]))
       (into {})))

(def ^:private compression-kwd->file-type
  (->> compression-info
       (map (fn [[k data]]
              [k (data :file-type)]))
       (into {})))


(defn- create-decompressor
  "Returns a function that takes a native-heap compressed buffer and a native-heap
  buffer to hold the decompressed data and performs the decompression."
  [^long comp-type]
  (if-let [kwd (file-type->compression-kwd comp-type)]
    ((get-in compression-info [kwd :decompressor-fn]))
    (throw (Exception. (format "Unrecognized file compression enum: %s" comp-type)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocol extensions for arrow schema types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol clj-proto/Datafiable
  ArrowType$Int
  (datafy [this]
    (let [signed? (.getIsSigned this)
          bit-width (.getBitWidth this)]
      {:datatype (if signed?
                   (case bit-width
                     8 :int8
                     16 :int16
                     32 :int32
                     64 :int64)
                   (case bit-width
                     8 :uint8
                     16 :uint16
                     32 :uint32
                     64 :uint64))}))
  ArrowType$Utf8
  (datafy [this]
    {:datatype :string
     :encoding :utf-8
     :offset-buffer-datatype :uint32})
  ArrowType$LargeUtf8
  (datafy [this]
    {:datatype :string
     :encoding :utf-8
     :offset-buffer-datatype :int64})
  ArrowType$FloatingPoint
  (datafy [this]
    {:datatype (condp = (.getPrecision this)
                 FloatingPointPrecision/HALF :float16
                 FloatingPointPrecision/SINGLE :float32
                 FloatingPointPrecision/DOUBLE :float64)})
  ArrowType$Timestamp
  (datafy [this]
    (merge
     (condp = (.getUnit this)
       TimeUnit/MILLISECOND {:datatype :epoch-milliseconds}
       TimeUnit/MICROSECOND {:datatype :epoch-microseconds}
       TimeUnit/SECOND {:datatype :epoch-second}
       TimeUnit/NANOSECOND {:datatype :int64 :time-unit :epoch-nanoseconds})
     (when (and (.getTimezone this)
                (not= 0 (count (.getTimezone this))))
       {:timezone (.getTimezone this)})))
  ArrowType$Time
  (datafy [this]
    (condp = (.getUnit this)
      TimeUnit/MILLISECOND {:datatype :time-milliseconds}
      TimeUnit/MICROSECOND {:datatype :time-microseconds}
      TimeUnit/SECOND {:datatype :time-second}
      TimeUnit/NANOSECOND {:datatype :time-nanosecond}))
  ArrowType$Date
  (datafy [this]
    (condp = (.getUnit this)
      DateUnit/MILLISECOND {:datatype :epoch-milliseconds}
      DateUnit/DAY {:datatype :epoch-days}))
  ArrowType$Bool
  (datafy [this] {:datatype :boolean})
  DictionaryEncoding
  (datafy [this] {:id (.getId this)
                  :ordered? (.isOrdered this)
                  :index-type (datafy (.getIndexType this))}))


(defn- pad
  ^long [^long data]
  (let [padding (rem data 8)]
    (if-not (== 0 padding)
      (+ data (- 8 padding))
      data)))


(defn- message-id->message-type
  [^long message-id]
  (case message-id
    1 :schema
    2 :dictionary-batch
    3 :record-batch
    {:unexpected-message-type message-id}))


(defn- message-type->message-id
  ^long [message-type]
  (case message-type
    :schema 1
    :dictionary-batch 2
    :record-batch 3))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Readings messages from native buffers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- read-message
  "returns a pair of offset-data and message."
  [data]
  (when-not (== 0 (dtype/ecount data))
    (let [msg-size (native-buffer/read-int data)
          [msg-size offset] (if (== -1 msg-size)
                              [(native-buffer/read-int data 4) 8]
                              [msg-size 4])
          offset (long offset)
          msg-size (long msg-size)]
      (when (> msg-size 0)
        (let [new-msg (Message/getRootAsMessage
                       (-> (dtype/sub-buffer data offset msg-size)
                           (nio-buffer/native-buf->nio-buf)))
              next-buf (dtype/sub-buffer data (+ offset msg-size))
              body-length (.bodyLength new-msg)
              aligned-offset (pad (+ offset msg-size body-length))]
          (merge
           {:next-data (dtype/sub-buffer data aligned-offset)
            :message new-msg
            :message-type (message-id->message-type (.headerType new-msg))}
           (when-not (== 0 body-length)
             {:body (dtype/sub-buffer next-buf 0 (.bodyLength new-msg))})))))))


(defn- message-seq
  "Given a native buffer of arrow stream data, produce a sequence of flatbuf messages"
  [^NativeBuffer data]
  (when-let [msg (read-message data)]
    (cons msg (lazy-seq (message-seq (:next-data msg))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reading messages from streams
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- read-int-LE
  (^long [^InputStream is byte-buf]
   (let [^bytes byte-buf (or byte-buf (byte-array 4))]
     (.read is byte-buf)
     (ByteConversions/intFromBytesLE (aget byte-buf 0) (aget byte-buf 1)
                                     (aget byte-buf 2) (aget byte-buf 3))))
  (^long [^InputStream is]
   (read-int-LE is nil)))


(defn- read-stream-message
  [^InputStream is]
  (let [msg-size (read-int-LE is)
        [msg-size offset] (if (== -1 msg-size)
                            [(read-int-LE is) 8]
                            [msg-size 4])
        msg-size (long msg-size)]
    (when (> msg-size 0)
      (let [pad-msg-size (pad msg-size)
            bytes (byte-array pad-msg-size)
            _ (.read is bytes)
            new-msg (Message/getRootAsMessage (nio-buffer/as-nio-buffer
                                               (dtype/sub-buffer bytes 0 msg-size)))
            body-len (.bodyLength new-msg)
            pad-body-len (pad body-len)
            nbuf (when-not (== 0 body-len)
                   (dtype/make-container :native-heap :int8 pad-body-len))
            read-chunk-size (* 1024 1024 100)
            read-buffer (byte-array (min body-len read-chunk-size))
            bytes-read (when nbuf
                         (loop [offset 0]
                           (let [amount-to-read (min (- pad-body-len offset) read-chunk-size)
                                 n-read (long (if-not (== 0 amount-to-read)
                                                (.read is read-buffer 0 amount-to-read)
                                                0))]
                             (if-not (== 0 n-read)
                               (do (dtype/copy! (dtype/sub-buffer read-buffer 0 n-read)
                                                (dtype/sub-buffer nbuf offset n-read))
                                   (recur (+ offset n-read)))
                               offset))))]
        (when (and nbuf (not= bytes-read pad-body-len))
          (throw (Exception. (format "Unable to read entire buffer - Expected %d got %d"
                                     pad-body-len bytes-read))))
        (merge {:message new-msg
                :message-type (message-id->message-type (.headerType new-msg))}
               (when nbuf
                 {:body (dtype/sub-buffer nbuf 0 body-len)}))))))

(defn- stream-message-seq
  [is]
  (if-let [msg (try (read-stream-message is)
                    (catch Throwable e
                      (.close ^InputStream is)
                      (throw e)))]
    (cons msg (lazy-seq (stream-message-seq is)))
    (do (.close ^InputStream is) nil)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datatype to schema mapping
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Create a dictionary and dictionary encoding from a string column
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- string-column->dict
  [col hash-salt]
  (let [str-t (ds-base/ensure-column-string-table col)
        int->str (str-table/int->string str-t)
        indices (dtype-proto/->array-buffer (str-table/indices str-t))
        n-elems (.size int->str)
        bit-width (casting/int-width (dtype/elemwise-datatype indices))
        metadata (meta col)
        colname (:name metadata)
        dict-id (.hashCode ^Object colname)
        dict-id (unchecked-int (if hash-salt
                                 (bit-xor (unchecked-int hash-salt) dict-id)
                                 dict-id))
        arrow-indices-type (ArrowType$Int. bit-width true)
        encoding (DictionaryEncoding. dict-id false arrow-indices-type)
        byte-data (dtype/make-list :int8)
        ;;offsets are int32 apparently
        offsets (dtype/make-list :int32)]
    (dotimes [str-idx (count int->str)]
      (let [strdata (int->str str-idx)
            _ (when (= strdata :failure)
                (throw (Exception. "Invalid string table - missing entries.")))
            str-bytes (.getBytes (str strdata))
            soff (dtype/ecount byte-data)]
        (.addAll byte-data (dtype/->reader str-bytes))
        (.add offsets soff)))
    ;;Make everyone's life easier by adding an extra offset.
    (.add offsets (dtype/ecount byte-data))
    {:encoding encoding
     :byte-data byte-data
     :offsets (dtype-proto/->array-buffer offsets)}))


(defn- string-col->encoding
  "Given a string column return a map of :dict-id :table-width.  The dictionary
  id is the hashcode of the column mame."
  [^List dictionaries col hash-salt]
  (let [dict (string-column->dict col hash-salt)]
    (.add dictionaries dict)
    {:encoding (:encoding dict)}))


(defn- make-field
  ^Field [^String name ^FieldType field-type]
  (Field. name field-type nil))


(defn- field-type
  ^FieldType
  ([nullable? ^FieldType datatype ^DictionaryEncoding dict-encoding ^Map str-str-meta]
   (FieldType. (boolean nullable?) datatype dict-encoding str-str-meta))
  ([nullable? datatype str-meta]
   (field-type nullable? datatype nil str-meta))
  ([nullable? datatype]
   (field-type nullable? datatype nil)))


(defn- ->str-str-meta
  [metadata]
  (->> metadata
       (map (fn [[k v]] [(pr-str k) (pr-str v)]))
       (into {})))


(defonce ^:private uuid-warn-counter (atom 0))


(defn- datatype->field-type
  (^FieldType [datatype & [nullable? metadata extra-data]]
   (let [nullable? (or nullable? (= :object (casting/flatten-datatype datatype)))
         metadata (->str-str-meta (dissoc metadata :name :datatype :categorical?) )
         ft-fn (fn [arrow-type & [dict-encoding]]
                 (field-type nullable? arrow-type dict-encoding metadata))
         datatype (packing/unpack-datatype datatype)]
     (case (if #{:epoch-microseconds :epoch-milliseconds
                 :epoch-days}
             datatype
             (casting/un-alias-datatype datatype))
       :boolean (ft-fn (ArrowType$Bool.))
       :uint8 (ft-fn (ArrowType$Int. 8 false))
       :int8 (ft-fn (ArrowType$Int. 8 true))
       :uint16 (ft-fn (ArrowType$Int. 16 false))
       :int16 (ft-fn (ArrowType$Int. 16 true))
       :uint32 (ft-fn (ArrowType$Int. 32 false))
       :int32 (ft-fn (ArrowType$Int. 32 true))
       :uint64 (ft-fn (ArrowType$Int. 64 false))
       :int64 (ft-fn (ArrowType$Int. 64 true))
       :float32 (ft-fn (ArrowType$FloatingPoint. FloatingPointPrecision/SINGLE))
       :float64 (ft-fn (ArrowType$FloatingPoint. FloatingPointPrecision/DOUBLE))
       :epoch-milliseconds (ft-fn (ArrowType$Timestamp. TimeUnit/MILLISECOND
                                                        (str (:timezone extra-data))))
       :instant (ft-fn (ArrowType$Timestamp. TimeUnit/MICROSECOND
                                             (str (:timezone extra-data))))
       :epoch-microsecond (ft-fn (ArrowType$Timestamp. TimeUnit/MICROSECOND
                                                        (str (:timezone extra-data))))
       :epoch-days (ft-fn (ArrowType$Date. DateUnit/DAY))
       :local-date (ft-fn (ArrowType$Date. DateUnit/DAY))
       ;;packed local time is 64bit microseconds since midnight
       :local-time (ft-fn (ArrowType$Time. TimeUnit/MICROSECOND (int 8)))
       :time-nanosecond (ft-fn (ArrowType$Time. TimeUnit/NANOSECOND (int 8)))
       :time-microsecond (ft-fn (ArrowType$Time. TimeUnit/MICROSECOND (int 8)))
       :time-millisecond (ft-fn (ArrowType$Time. TimeUnit/MILLISECOND (int 4)))
       :time-second (ft-fn (ArrowType$Time. TimeUnit/SECOND (int 4)))
       :duration (ft-fn (ArrowType$Duration. TimeUnit/MICROSECOND))
       :string (if-let [^DictionaryEncoding encoding (:encoding extra-data)]
                 (ft-fn (ArrowType$Utf8.) encoding)
                 ;;If no encoding is provided then just save the string as text
                 (ft-fn (ArrowType$Utf8.)))
       :uuid (do
               (when (== 1 (long (swap! uuid-warn-counter inc)))
                 (log/warn "Columns of type UUID are converted to type text when serializing to Arrow"))
               (ft-fn (ArrowType$Utf8.)))
       :text (ft-fn (ArrowType$Utf8.))
       :encoded-text (ft-fn (ArrowType$Utf8.))))))


(defn- col->field
  ^Field [dictionaries {:keys [strings-as-text?
                               ::hash-salt]}
          col]
  (let [colmeta (meta col)
        nullable? (boolean
                   (or (:nullable? colmeta)
                       (not (.isEmpty
                             (col-proto/missing col)))))
        col-dtype (:datatype colmeta)
        colname (:name colmeta)
        extra-data (merge (select-keys (meta col) [:timezone])
                          (when (and (not strings-as-text?)
                                     (= :string col-dtype))
                            (string-col->encoding dictionaries col hash-salt)))]
    (try
      (make-field
       (ml-utils/column-safe-name colname)
       (datatype->field-type col-dtype nullable? colmeta extra-data))
      (catch Throwable e
        (throw (Exception. (format "Column %s metadata conversion failure:\n%s"
                                   colname e)
                           e))))))

(defn- ->timezone
  (^ZoneId [& [item]]
   (cond
     (instance? ZoneId item)
     item
     (string? item)
     (ZoneId/of ^String item)
     :else
     (dtype-dt/utc-zone-id))))


(defn ^:no-doc prepare-dataset-for-write
  "Convert dataset column datatypes to datatypes appropriate for arrow
  serialization.

  Options:

  * `:strings-as-text` - Serialize strings as text.  This leads to the most efficient
  format for mmap operations."
  ([ds options]
   (reduce
    (fn [ds col]
      (cond
        (= :uuid (dtype/elemwise-datatype col))
        (let [missing (col-proto/missing col)
              metadata (meta col)]
          (assoc ds (metadata :name)
                 #:tech.v3.dataset{:data (mapv (comp #(Text. %) str) col)
                                   :missing missing
                                   :metadata metadata
                                   :name (metadata :name)}))
        (and (= :string (dtype/elemwise-datatype col))
             (not (:strings-as-text? options)))
        (if (instance? StringTable (.data ^Column col))
          ds
          (let [missing (col-proto/missing col)
                metadata (meta col)]
            (assoc ds (metadata :name)
                   #:tech.v3.dataset{:data (str-table/string-table-from-strings col)
                                     :missing missing
                                     :metadata metadata
                                     :name (metadata :name)})))
        :else
        ds))
    ds
    (ds-base/columns ds)))
  ([ds]
   (prepare-dataset-for-write ds nil)))


(defn ^:no-doc ds->schema
  "Return a map of :schema, :dictionaries where :schema is an Arrow schema
  and dictionaries is a list of {:byte-data :offsets}.  Dataset must be prepared first
  - [[prepare-dataset-for-write]] to ensure that columns have the appropriate datatypes."
  ([ds options]
   (let [dictionaries (ArrayList.)
         hash-salt (get options ::hash-salt)
         hash-salt (when hash-salt (.hashCode ^Object hash-salt))
         options (assoc options ::hash-salt hash-salt)]
     {:schema
      (Schema. ^Iterable
               (->> (ds-base/columns ds)
                    (map (partial col->field dictionaries options))))
      :dictionaries dictionaries}))
  ([ds]
   (ds->schema ds {})))


(defn- read-schema
  "returns a pair of offset-data and schema"
  [{:keys [message _body _message-type]}]
  (let [schema (MessageSerializer/deserializeSchema ^Message message)
        fields
        (->> (.getFields schema)
             (mapv (fn [^Field field]
                     (let [arrow-type (.getType (.getFieldType field))
                           datafied-data (datafy arrow-type)]
                       (when-not (map? datafied-data)
                         (throw (Exception.
                                 (format "Failed to datafy datatype %s"
                                         (type arrow-type)))))
                       (merge
                        {:name (.getName field)
                         :nullable? (.isNullable field)
                         :field-type datafied-data
                         :metadata (.getMetadata field)}
                        (when-let [encoding (.getDictionary field)]
                          {:dictionary-encoding (datafy encoding)}))))))]
    {:fields fields
     :encodings (->> (map :dictionary-encoding fields)
                     (remove nil?)
                     (map (juxt :id identity))
                     (into {}))
     :metadata (.getCustomMetadata schema)}))


(defn- write-message-header
  [^WriteChannel writer msg-header-data]
  ;;continuation so we have 8 byte lengths
  (let [start (.getCurrentPosition writer)
        meta-len (dtype/ecount msg-header-data)
        padding (rem (+ start meta-len 8) 8)
        ;;pad so that start + meta-len ends on an 8 byte boundary
        meta-len (long (if-not (== 0 padding)
                         (+ meta-len (- 8 padding))
                         meta-len))]
    (.writeIntLittleEndian writer -1)
    (.writeIntLittleEndian writer meta-len)
    (.write writer (dtype/->byte-array msg-header-data))
    (.align writer)))


(defn- write-schema
  "Writes a schema to a message header."
  [writer schema]
  (write-message-header writer (MessageSerializer/serializeMetadata ^Schema schema)))


(defn- decompress-buffers
  [^BodyCompression compression buffers]
  (when-not (== 0 (.method compression))
    (throw (Exception. (format "Only buffer batch compression supported - got %d"
                               (.method compression)))))
  (let [decompressor (create-decompressor (.codec compression))
        buffers (mapv (fn [buffer]
                        (let [orig-len (native-buffer/read-long buffer)]
                          {:orig-len orig-len
                           :buffer (dtype/sub-buffer buffer 8)}))
                      buffers)
        decomp-buf-len (->> (map :orig-len buffers)
                            (remove #(== -1 (long %)))
                            (apply +)
                            (long))
        decomp-buf (dtype/make-container :native-heap :int8 decomp-buf-len)]
    (->> buffers
         (reduce (fn [[res decomp-buf] {:keys [orig-len buffer]}]
                   ;;-1 indicates the buffer isn't actually compressed.
                   (if (== -1 (long orig-len))
                     [(conj res buffer) decomp-buf]
                     [(conj res (decompressor buffer (dtype/sub-buffer decomp-buf 0 orig-len)))
                      (dtype/sub-buffer decomp-buf orig-len)]))
                 [[] decomp-buf])
         (first))))


(defn- read-record-batch
  ([^RecordBatch record-batch ^NativeBuffer data]
   (let [compression (.compression record-batch)
         buffers (->> (range (.buffersLength record-batch))
                      (mapv #(let [buffer (.buffers record-batch (int %))]
                               (dtype/sub-buffer data (.offset buffer)
                                                 (.length buffer)))))
         buffers (if compression
                   (decompress-buffers compression buffers)
                   buffers)]
     {:nodes (->> (range (.nodesLength record-batch))
                  (mapv #(let [node (.nodes record-batch (int %))]
                           {:n-elems (.length node)
                            :n-null-entries (.nullCount node)})))
      :buffers buffers}))
  ([{:keys [message body _message-type]}]
   (read-record-batch (.header ^Message message (RecordBatch.)) body)))


(defn- write-record-batch-header
  [^FlatBufferBuilder builder n-rows nodes buffers compression-type]
  (let [_ (RecordBatch/startNodesVector builder (count nodes))
        ;;Apparently you have to reverse structs when writing to a vector...
        _ (doseq [node (reverse nodes)]
            (FieldNode/createFieldNode builder
                                       (long (node :n-elems))
                                       (long (node :n-null-entries))))
        node-offset (.endVector builder)
        _ (RecordBatch/startBuffersVector builder (count buffers))
        _ (doseq [buffer (reverse buffers)]
            (Buffer/createBuffer builder
                                 (long (buffer :offset))
                                 (long (buffer :length))))
        buffers-offset (.endVector builder)
        comp-offset (when compression-type
                      (BodyCompression/createBodyCompression
                       builder
                       (unchecked-byte compression-type)
                       (BodyCompressionMethod/BUFFER)))]
    (RecordBatch/startRecordBatch builder)
    (RecordBatch/addLength builder (long n-rows))
    (when comp-offset (RecordBatch/addCompression builder (unchecked-int comp-offset)))
    (RecordBatch/addNodes builder node-offset)
    (RecordBatch/addBuffers builder buffers-offset)
    (RecordBatch/endRecordBatch builder)))


(defn- len->bitwise-len
  "Given a length of a boolean vector, return the length if
  represented by a bitwise vector."
  ^long [^long n-elems]
  (quot (+ n-elems 7) 8))


(defn- no-missing
  [^long n-elems]
  (let [n-bytes (len->bitwise-len n-elems)
        c (dtype/make-container :int8 n-bytes)]
    (dtype/set-constant! c -1)
    c))

(defn- byte-length
  ^long [container]
  (* (dtype/ecount container) (casting/numeric-byte-width
                               (dtype/elemwise-datatype container))))


(defn- serialize-to-bytes
  "Serialize a numeric buffer to a write channel."
  ^java.nio.Buffer [num-data]
  (let [data-dt (casting/un-alias-datatype (dtype/elemwise-datatype num-data))
        n-bytes (byte-length num-data)
        backing-buf (byte-array n-bytes)
        bbuf (-> (java.nio.ByteBuffer/wrap backing-buf)
                 (.order java.nio.ByteOrder/LITTLE_ENDIAN))
        ary-data (dtype/->array (if (= data-dt :boolean)
                                  :int8
                                  data-dt)
                                num-data)]
    (if (instance? java.nio.Buffer num-data)
      (case (dtype/elemwise-datatype ary-data)
        :int8 (.put bbuf ^java.nio.ByteBuffer num-data)
        :int16 (-> (.asShortBuffer bbuf)
                   (.put ^java.nio.ShortBuffer num-data))
        :int32 (-> (.asIntBuffer bbuf)
                   (.put ^java.nio.IntBuffer num-data))
        :int64 (-> (.asLongBuffer bbuf)
                   (.put ^java.nio.LongBuffer num-data))
        :float32 (-> (.asFloatBuffer bbuf)
                     (.put ^java.nio.FloatBuffer num-data))
        :float64 (-> (.asDoubleBuffer bbuf)
                     (.put ^java.nio.DoubleBuffer num-data)))
      (case (dtype/elemwise-datatype ary-data)
        :int8 (.put bbuf ^bytes ary-data)
        :int16 (-> (.asShortBuffer bbuf)
                   (.put ^shorts ary-data))
        :int32 (-> (.asIntBuffer bbuf)
                   (.put ^ints ary-data))
        :int64 (-> (.asLongBuffer bbuf)
                   (.put ^longs ary-data))
        :float32 (-> (.asFloatBuffer bbuf)
                     (.put ^floats ary-data))
        :float64 (-> (.asDoubleBuffer bbuf)
                     (.put ^doubles ary-data))))
    backing-buf))


(defn- finish-builder
  "Finish the flatbuffer builder returning a ByteBuffer"
  ^java.nio.ByteBuffer [^FlatBufferBuilder builder message-type header-off body-len]
  (.finish builder
           (Message/createMessage builder
                                  (.toFlatbufID (MetadataVersion/DEFAULT))
                                  message-type ;;dict message type
                                  header-off
                                  body-len
                                  0 ;;custom metadata offset
                                  ))
  (.dataBuffer builder))


(defn- compress-record-batch-buffers
  [buffers options]
  (if-let [comp-map (get options :compression)]
    (let [comp-fn (create-compressor comp-map)
          os (ByteArrayOutputStream.)
          data-len (byte-array 8)
          ^ByteBuffer nio-buf (nio-buffer/as-nio-buffer data-len)
          data-len-buf (dtype/->buffer data-len)]
      {:compression-type (compression-kwd->file-type (comp-map :compression-type))
       :buffers
       (first
        (reduce (fn [[res writer-cache] buffer]
                  (let [buffer (serialize-to-bytes buffer)
                        uncomp-len (dtype/ecount buffer)
                        _ (ByteConversions/longToWriterLE uncomp-len data-len-buf 0)
                        {:keys [writer-cache dst-buffer]} (comp-fn buffer writer-cache)
                        dst-bytes (dtype/as-array-buffer dst-buffer)
                        _ (.write os data-len)
                        _ (.write os ^bytes (.ary-data dst-bytes)
                                  (unchecked-int (.offset dst-bytes))
                                  (unchecked-int (.n-elems dst-bytes)))
                        dst-buffer (.toByteArray os)]
                    (.reset os)
                    [(conj res (nio-buffer/as-nio-buffer dst-buffer))
                     writer-cache]))
                [[] nil]
                buffers))})
    {:buffers (mapv #(-> (serialize-to-bytes %)
                         (nio-buffer/as-nio-buffer))
                    buffers)}))

(defn- buffers->buf-entries
  [buffers]
  (->
   (reduce (fn [[res offset] buffer]
             [(conj res {:offset offset
                         :length (dtype/ecount buffer)})
              (pad (+ (long offset) (dtype/ecount buffer)))])
           [[] 0]
           buffers)
   (first)))


(defn- write-dictionary
  "Write a dictionary to a dictionary batch"
  [^WriteChannel writer {:keys [byte-data offsets encoding]} options]
  (let [n-elems (dec (count offsets))
        missing (no-missing n-elems)
        {:keys [compression-type buffers]}
        (compress-record-batch-buffers [missing offsets byte-data] options)
        buffer-entries (buffers->buf-entries buffers)
        enc-id (.getId ^DictionaryEncoding encoding)
        offset-len (pad (byte-length offsets))
        data-len (pad (count byte-data))
        builder (FlatBufferBuilder.)
        rbatch-off (write-record-batch-header
                    builder
                    n-elems
                    [{:n-elems n-elems
                      :n-null-entries 0}]
                    buffer-entries
                    compression-type)
        last-entry (last buffer-entries)
        body-len (pad (+ (long (last-entry :offset))
                         (long (last-entry :length))))]
    (write-message-header writer
                          (finish-builder builder
                                          (message-type->message-id :dictionary-batch)
                                          (DictionaryBatch/createDictionaryBatch
                                           builder enc-id rbatch-off false)
                                          body-len))
    (doseq [buf buffers]
      (.write writer ^ByteBuffer buf)
      (.align writer))))


(defn- toggle-bit
  ^bytes [^bytes data ^long bit]
  (let [idx (quot bit 8)
        bit (rem bit 8)
        existing (unchecked-int (aget data idx))]
    (ArrayHelpers/aset data idx (unchecked-byte (bit-xor existing (bit-shift-left 1 bit)))))
  data)


(defn- missing-bytes
  ^bytes [^RoaringBitmap bmp ^bytes all-valid-buf]
  (let [nbuf (-> (dtype/clone all-valid-buf)
                 (dtype/->byte-array))]
    ;;Roaring bitmap's recommended way of iterating through all its values.
    (.forEach bmp (reify org.roaringbitmap.IntConsumer
                    (accept [this data]
                      (toggle-bit nbuf (Integer/toUnsignedLong data)))))
    nbuf))

(defn ^:no-doc boolean-bytes
  ^bytes [data]
  (let [rdr (dtype/->reader data)
        len (.lsize rdr)
        retval (byte-array (len->bitwise-len len))]
    (dotimes [idx len]
      (when (.readBoolean rdr idx)
        (toggle-bit retval idx)))
    retval))


(defn- col->buffers
  [col options]
  (let [col-dt (casting/un-alias-datatype (dtype/elemwise-datatype col))
        col-dt (if (and (:strings-as-text? options) (= col-dt :string))
                 :text
                 col-dt)
        cbuf (dtype/->buffer col)]
    ;;Get the data as a datatype safe for conversion into a
    ;;nio buffer.
    (if (casting/numeric-type? col-dt)
      (let [cbuf (if (dtype/as-concrete-buffer cbuf)
                   cbuf
                   ;;make buffer concrete
                   (dtype/clone cbuf))]
        [(if-let [ary-buf (dtype/as-array-buffer cbuf)]
           (-> (dtype/sub-buffer (.ary-data ary-buf)
                                 (.offset ary-buf)
                                 (.n-elems ary-buf))
               (nio-buffer/as-nio-buffer))
           (if-let [nbuf (dtype/as-native-buffer cbuf)]
             (->
              (native-buffer/set-native-datatype
               nbuf (casting/datatype->host-datatype col-dt))
              (nio-buffer/as-nio-buffer))
             ;;else data is not represented
             (throw (Exception. "Numeric buffer missing concrete representation"))))])
      (case col-dt
        :boolean [(boolean-bytes cbuf)]
        :string (let [str-t (ds-base/ensure-column-string-table col)
                      indices (dtype-proto/->array-buffer (str-table/indices str-t))]
                  [(nio-buffer/as-nio-buffer indices)])
        :text
        (let [byte-data (dtype/make-list :int8)
              offsets (dtype/make-list :int32)]
          (pfor/doiter
           strdata cbuf
           (let [strdata (str (or strdata ""))]
             (.add offsets (.lsize byte-data))
             (.addAll byte-data (dtype/->buffer (.getBytes strdata)))))
          (.add offsets (.lsize byte-data))
          [(nio-buffer/as-nio-buffer offsets)
           (nio-buffer/as-nio-buffer byte-data)])))))


(defn- write-dataset
  "Write a dataset-batch to a channel."
  [^WriteChannel writer dataset options]
  (let [n-rows (ds-base/row-count dataset)
        ;;Length of the byte valid buffer
        valid-len (len->bitwise-len n-rows)
        all-valid-buf (-> (doto (dtype/make-container :int8 valid-len)
                            (dtype/set-constant! -1))
                          (dtype/->byte-array))
        nodes-buffs-lens
        (->> (ds-base/columns dataset)
             (map (fn [col]
                    (let [col-missing (col-proto/missing col)
                          n-missing (dtype/ecount col-missing)
                          valid-buf (if (== 0 n-missing)
                                      all-valid-buf
                                      (missing-bytes col-missing all-valid-buf))
                          buffers (vec (concat [valid-buf]
                                               (col->buffers col options)))
                          lengths (map (comp pad byte-length) buffers)
                          col-len (long (apply + lengths))]
                      {:node {:n-elems n-rows
                              :n-null-entries n-missing}
                       :buffers buffers
                       :length col-len}))))
        nodes (map :node nodes-buffs-lens)
        {:keys [compression-type buffers]}
        (compress-record-batch-buffers
         (mapcat :buffers nodes-buffs-lens)
         options)

        buf-entries (buffers->buf-entries buffers)
        last-entry (last buf-entries)
        body-len (pad (+ (long (last-entry :offset)) (long (last-entry :length))))
        builder (FlatBufferBuilder.)]
    (write-message-header writer
                          (finish-builder
                           ;;record-batch message type
                           builder (message-type->message-id :record-batch)
                           (write-record-batch-header builder n-rows nodes buf-entries
                                                      compression-type)
                           body-len))
    (doseq [buf buffers]
      (.write writer ^ByteBuffer buf)
      (.align writer))))


(defn- check-message-type
  [expected-type actual-type]
  (when-not (= actual-type expected-type)
    (throw (Exception.
            (format "Expected message type %s, got %s"
                    expected-type actual-type)))))


(defn- read-dictionary-batch
  [{:keys [message body message-type]}]
  (check-message-type :dictionary-batch message-type)
  (let [^DictionaryBatch db (.header ^Message message (DictionaryBatch.))]
    {:id (.id db)
     :delta? (.isDelta db)
     :records (read-record-batch (.data db) body)}))


(defmulti ^:private parse-message
  "Given a message, parse it just a bit into a more interpretable datastructure."
  :message-type)


(defmethod ^:private parse-message :schema
  [msg]
  (assoc (read-schema msg)
         :message-type (:message-type msg)))


(defmethod ^:private parse-message :dictionary-batch
  [msg]
  (assoc (read-dictionary-batch msg)
         :message-type (:message-type msg)))


(defmethod ^:private parse-message :record-batch
  [msg]
  (assoc (read-record-batch msg)
         :message-type (:message-type msg)))


(defn ^:no-doc parse-message-printable
  "Parse the message and return something that you can look at in the repl."
  [msg]
  (let [retval (parse-message msg)]
    (cond
      (contains? retval :records)
      (update-in retval [:records :buffers]
                 #(mapv native-buffer/native-buffer->map %))
      (contains? retval :buffers)
      (update-in retval [:buffers]
                 #(mapv native-buffer/native-buffer->map %))
      :else
      retval)))


(defn- offsets-data->string-reader
  ^List [offsets data n-elems]
  (let [n-elems (long n-elems)
        offsets (dtype/->reader offsets)]
    (reify ObjectReader
      (elemwiseDatatype [rdr] :string)
      (lsize [rdr] n-elems)
      (readObject [rdr idx]
        (let [start-off (long (offsets idx))
              end-off (long (offsets (inc idx)))]
          (-> (dtype/sub-buffer data start-off
                                (- end-off start-off))
              (dtype/->byte-array)
              (String.)))))))


(defn- dictionary->strings
  "Returns a map of {:id :strings}"
  [{:keys [id _delta? records]}]
  (let [nodes (:nodes records)
        buffers (:buffers records)
        _ (assert (== 1 (count nodes)))
        _ (assert (== 3 (count buffers)))
        node (first nodes)
        [_bitwise offsets databuf] buffers
        n-elems (long (:n-elems node))
        offsets (-> (native-buffer/set-native-datatype offsets :int32)
                    (dtype/sub-buffer 0 (inc n-elems)))
        data (native-buffer/set-native-datatype databuf :int8)
        str-data (dtype/make-list :string
                                  (-> (offsets-data->string-reader offsets data n-elems)
                                      (dtype/clone)))]
    {:id id
     :strings str-data}))


(defn- string-reader->text-reader
  ^Buffer [item]
  (let [data-buf (dtype/->reader item)]
    (reify ObjectReader
      (elemwiseDatatype [rdr] :text)
      (lsize [rdr] (.lsize data-buf))
      (readObject [rdr idx]
        (when-let [data (.readObject data-buf idx)]
          (Text. (str data)))))))


(defn- string-data->column-data
  [dict-map encoding offset-buf-dtype buffers n-elems]
  (if encoding
    (let [str-list (get-in dict-map [(:id encoding) :strings])
          index-data (-> (first buffers)
                         (native-buffer/set-native-datatype
                          (get-in encoding [:index-type :datatype]))
                         (dtype/sub-buffer 0 n-elems))
          retval (StringTable. str-list nil (dyn-int-list/make-from-container
                                             index-data))]
      retval)
    (let [[offsets varchar-data] buffers]
      (-> (offsets-data->string-reader (native-buffer/set-native-datatype
                                        offsets offset-buf-dtype)
                                       varchar-data n-elems)
          (string-reader->text-reader)))))


(defn- int8-buf->missing
  ^RoaringBitmap [data-buf ^long n-elems]
  (let [data-buf (dtype/->reader data-buf)
        ^RoaringBitmap missing (bitmap/->bitmap)
        n-bytes (len->bitwise-len n-elems)]
    (dotimes [idx n-bytes]
      (let [offset (pmath/* 8 idx)
            data (unchecked-int (.readByte data-buf idx))]
        (when-not (== data -1)
          ;;TODO - find more elegant way of pulling this off
          (when (== 0 (pmath/bit-and data 1))
            (.add missing offset))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 1)))
            (.add missing (pmath/+ offset 1)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 2)))
            (.add missing (pmath/+ offset 2)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 3)))
            (.add missing (pmath/+ offset 3)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 4)))
            (.add missing (pmath/+ offset 4)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 5)))
            (.add missing (pmath/+ offset 5)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 6)))
            (.add missing (pmath/+ offset 6)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 7)))
            (.add missing (pmath/+ offset 7))))))
    (dtype-proto/set-and missing (range n-elems))))


(defn ^:no-doc byte-buffer->bitwise-boolean-buffer
  ^Buffer[bitbuffer ^long n-elems]
  (let [buf (dtype/->buffer bitbuffer)]
    (dtype/make-reader :boolean n-elems
                       (let [data (.readByte buf (quot idx 8))
                             shift-val (pmath/bit-shift-left 1 (rem idx 8))]
                         (if (pmath/== shift-val (pmath/bit-and data shift-val))
                           true
                           false)))))


(defn- records->ds
  [schema dict-map record-batch options]
  (let [{:keys [fields]} schema
        {:keys [nodes buffers]} record-batch]
    (assert (= (count fields) (count nodes)))
    (->> (map vector fields nodes)
         (reduce
          (fn [[retval ^long buf-idx] [field node]]
            (let [field-dtype (get-in field [:field-type :datatype])
                  field-dtype (if-not (:integer-datetime-types? options)
                                (case field-dtype
                                  :time-microseconds :packed-local-time
                                  :epoch-microseconds :packed-instant
                                  :epoch-days :packed-local-date
                                  field-dtype)
                                field-dtype)
                  col-metadata (dissoc (:field-type field) :datatype)
                  encoding (get field :dictionary-encoding)
                  n-buffers (long (if (and (= :string field-dtype)
                                           (not encoding))
                                    3
                                    2))
                  specific-bufs (subvec buffers buf-idx (+ buf-idx n-buffers))
                  n-elems (long (:n-elems node))
                  missing (if (== 0 (long (:n-null-entries node)))
                            (bitmap/->bitmap)
                            (int8-buf->missing
                             (first specific-bufs)
                             n-elems))
                  metadata (into col-metadata (:metadata field))]
              [(conj retval
                     (col-impl/new-column
                      (:name field)
                      (cond
                        (= field-dtype :string)
                        (string-data->column-data
                         dict-map encoding
                         (get-in field [:field-type :offset-buffer-datatype])
                         (drop 1 specific-bufs)
                         n-elems)
                        (= field-dtype :boolean)
                        (byte-buffer->bitwise-boolean-buffer
                         (second specific-bufs) n-elems)
                        :else
                        (-> (native-buffer/set-native-datatype
                             (second specific-bufs) field-dtype)
                            (dtype/sub-buffer 0 n-elems)))
                      metadata
                      missing))
               (+ buf-idx n-buffers)]))
          [[] 0])
         (first)
         (ds-impl/new-dataset options))))


(defn- parse-next-dataset
  [schema messages fname idx dict-map options]
  (when (seq messages)
    (let [dict-messages (take-while #(= (:message-type %) :dictionary-batch)
                                    messages)
          rest-messages (drop (count dict-messages) messages)
          dict-map (merge dict-map
                          (->> dict-messages
                               (map dictionary->strings)
                               (map (juxt :id identity))
                               (into {})))
          data-record (first rest-messages)]
      (cons
       (-> (records->ds schema dict-map data-record options)
           (ds-base/set-dataset-name (format "%s-%03d" fname idx)))
       (lazy-seq (parse-next-dataset schema (rest rest-messages)
                                     fname (inc (long idx)) dict-map options))))))


(defn stream->dataset-seq
  "Loads data up to and including the first data record.  Returns the a lazy
  sequence of datasets.  Datasets use mmapped data, however, so realizing the
  entire sequence is usually safe, even for datasets that are larger than
  available RAM.
  This method is expected to be called from within a stack resource context
  unless options include {:resource-type :gc}.  See documentation for
  tech.v3.datatype.mmap/mmap-file.

  Options:

  * `:open-type` - Either `:mmap` or `:input-stream` defaulting to the slower but more robust
  `:input-stream` pathway.  When using `:mmap` resources will be released when the resource
  system dictates - see documentation for [tech.v3.resource](https://techascent.github.io/tech.resource/tech.v3.resource.html).
  When using `:input-stream` the stream will be closed when the lazy sequence is either
  fully realized or an exception is thrown.

  * `close-input-stream?` - When using `:input-stream` `:open-type`, close the input stream upon
  exception or when stream is fully realized.  Defaults to true.

  * `:integer-datetime-types?` - when true arrow columns in the appropriate packed
  datatypes will be represented as their integer types as opposed to their respective
  packed types.  For example columns of type `:epoch-days` will be returned to the user
  as datatype `:epoch-days` as opposed to `:packed-local-date`.  This means reading values
  will return integers as opposed to `java.time.LocalDate`s."
  [fname & [options]]
  (->> (let [messages (case (get options :open-type :input-stream)
                        :mmap (->> (mmap/mmap-file fname options)
                                   (message-seq))
                        :input-stream (->> (apply io/input-stream fname
                                                  (apply concat (seq options)))
                                           (stream-message-seq)))
             messages (sequence (map parse-message) messages)
             schema (first messages)
             _ (when-not (= :schema (:message-type schema))
                 (throw (Exception. "Initial message is not a schema message.")))]
         (parse-next-dataset schema (rest messages) fname 0 nil options))))


(defn- message-seq->dataset
  [fname options message-seq]
  (let [messages (mapv parse-message message-seq)
        schema (first messages)
        _ (when-not (= :schema (:message-type schema))
            (throw (Exception. "Initial message is not a schema message.")))
        messages (rest messages)
        dict-messages (take-while #(= (:message-type %) :dictionary-batch)
                                  messages)
        rest-messages (drop (count dict-messages) messages)
        dict-map (->> dict-messages
                      (map dictionary->strings)
                      (map (juxt :id identity))
                      (into {}))
        data-record (first rest-messages)]
    (when-not (= :record-batch (:message-type data-record))
      (throw (Exception. "No data records detected")))
    (when (seq (rest rest-messages))
      (throw (Exception. "File contains multiple record batches.
Please use stream->dataset-seq-inplace.")))
    (-> (records->ds schema dict-map data-record options)
        (ds-base/set-dataset-name fname))))


(defn stream->dataset
  "Reads data non-lazily in arrow streaming format expecting to find a single dataset.

  Options:

  * `:open-type` - Either `:mmap` or `:input-stream` defaulting to the slower but more robust
  `:input-stream` pathway.  When using `:mmap` resources will be released when the resource
  system dictates - see documentation for [tech.v3.resource](https://techascent.github.io/tech.resource/tech.v3.resource.html).
  When using `:input-stream` the stream will be closed when the lazy sequence is either fully realized or an
  exception is thrown.  Memory mapping is not supported on m-1 macs unless you are using JDK-17.

  * `close-input-stream?` - When using `:input-stream` `:open-type`, close the input stream upon
  exception or when stream is fully realized.  Defaults to true.

  * `:integer-datatime-types?` - when true arrow columns in the appropriate packed
  datatypes will be represented as their integer types as opposed to their respective
  packed types.  For example columns of type `:epoch-days` will be returned to the user
  as datatype `:epoch-days` as opposed to `:packed-local-date`.  This means reading values
  will return integers as opposed to `java.time.LocalDate`s."
  [fname & [options]]
  (->> (case (get options :open-type :input-stream)
         :mmap
         (->> (mmap/mmap-file fname options)
              (message-seq))
         :input-stream
         (->> (apply io/input-stream fname (apply concat (seq options)))
              (stream-message-seq)))
       (message-seq->dataset fname options)))


(def ^:private compression-type
  {:zstd CompressionType/ZSTD
   :lz4 CompressionType/LZ4_FRAME})


(defn- ensure-valid-compression-keyword
  [kwd]
  (if (contains? compression-type kwd)
    kwd
    (throw (Exception. (format "Unrecognized compression type %s" kwd)))))


(defn- validate-compression-for-write
  [compression]
  (when compression
    (->
     (cond
       (map? compression)
       (do
         (when-not (get compression :compression-type)
           (throw (Exception. (format "Invalid compression map %s" compression))))
         (update compression :compression-type ensure-valid-compression-keyword))
       (keyword? compression)
       {:compression-type (ensure-valid-compression-keyword compression)}
       :else
       (throw (Exception. "Unrecognized compression type")))
     #_(create-compressor))))


(defn dataset->stream!
  "Write a dataset as an arrow stream file.  File will contain one record set.

  Options:

  * `strings-as-text?`: - defaults to false - Save out strings into arrow files without
     dictionaries.  This works well if you want to load an arrow file in-place or if
     you know the strings in your dataset are either really large or should not be in
     string tables.

  * `:compression` - Either `:zstd` or `:lz4`,  defaults to no compression (nil).
     Per-column compression of the data can result in some significant size savings
     (2x+) and thus some significant time savings when loading over the network.
     Using compression makes loading via mmap non-lazy - If you are going to use
     compression mmap probably doesn't make sense on load and most likely will
     result on slower loading times."
  ([ds path options]
   (let [ds (prepare-dataset-for-write ds options)
         options (update options :compression validate-compression-for-write)
         {:keys [schema dictionaries]} (ds->schema ds options)]
     ;;Any native mem allocated in this context will be released
     (resource/stack-resource-context
      (with-open [ostream (io/output-stream! path)]
        (let [writer (arrow-output-stream-writer ostream)]
          (write-schema writer schema)
          (doseq [dict dictionaries] (write-dictionary writer dict options))
          (write-dataset writer ds options))))))
  ([ds path]
   (dataset->stream! ds path {})))


(defn- simplify-datatype
  [datatype options]
  (let [datatype (packing/unpack-datatype datatype)]
    (if (and (= :string datatype) (get options :strings-as-text?))
      :text
      datatype)))


(defn dataset-seq->stream!
  "Write a sequence of datasets as an arrow stream file.  File will contain one record set
  per dataset.  Datasets in the sequence must have matching schemas or downstream schema
  must be able to be safely widened to the first schema.

  Options:

  * `strings-as-text?`: - defaults to false - Save out strings into arrow files without
     dictionaries.  This works well if you want to load an arrow file in-place or if
     you know the strings in your dataset are either really large or should not be in
     string tables.

  * `:compression` - Either `:zstd` or `:lz4`,  defaults to no compression (nil).
     Per-column compression of the data can result in some significant size savings
     (2x+) and thus some significant time savings when loading over the network.
     Using compression makes loading via mmap non-lazy - If you are going to use
     compression mmap probably doesn't make sense and most likely will result in
     slower loading times."
  ([path options ds-seq]
   ;;We use the first dataset to setup schema information the rest of the datasets
   ;;must follow.  So the serialization of the first dataset differs from the serialization
   ;;of the subsequent datasets
   (let [ds (first ds-seq)
         ds-schema (map meta (ds-base/columns ds))
         cnames (map :name ds-schema)
         ds-map-fn
         (fn [ds]
           ;;Ensure they have at least the same columns, in order, as the first dataset
           ;;and do a quick datatype check.
           (let [ds (ds-base/select-columns ds cnames)
                 ds-meta (map meta (ds-base/columns ds))]
             (->
              (reduce (fn [ds col-meta]
                        (ds-base/update-column
                         ds (:name col-meta)
                         (fn [col]
                           (let [col-dt (simplify-datatype (dtype/elemwise-datatype col)
                                                           options)
                                 src-dt (simplify-datatype (:datatype col-meta) options)
                                 merged-dt (casting/widest-datatype src-dt col-dt)]
                             ;;Do a datatype check to see if things match
                             (when-not (= src-dt merged-dt)
                               (throw (Exception.
                                       (format "Datatypes differ in a downstream dataset.  Expected '%s' got '%s'" src-dt col-dt))))
                             (if (= col-dt src-dt)
                               col
                               (dtype/elemwise-cast col src-dt))))))
                      ds
                      ds-schema)
              (prepare-dataset-for-write options))))
         ds-seq (sequence (map ds-map-fn) (rest ds-seq))]
     ;;Any native mem allocated in this context will be released
     (with-open [ostream (io/output-stream! path)]
       (let [writer (arrow-output-stream-writer ostream)
             {:keys [schema dictionaries]} (ds->schema ds (assoc options ::hash-salt -1))]
         (write-schema writer schema)
         (doseq [dict dictionaries] (write-dictionary writer dict options))
         ;;Release any native mem created during this step just after step completes
         (resource/stack-resource-context
          (write-dataset writer ds options))
         (doseq [[idx ds] (map-indexed vector ds-seq)]
           ;;Have to use different hashing of dictionaries else we get overwriting of
           ;;dictionaries which may not be ideal for other engines without adding meaningful
           ;;support for isDelta.
           (let [{:keys [dictionaries]} (ds->schema ds (assoc options ::hash-salt idx))]
             (doseq [dict dictionaries] (write-dictionary writer dict options))
             ;;Release any native mem created during this step just after this step
             ;;completes.
             (resource/stack-resource-context
              (write-dataset writer ds options))))))))
  ([path ds-seq]
   (dataset-seq->stream! path nil ds-seq)))


(defn ^:no-doc read-stream-dataset-copying
  "This method has been deprecated.  Please use stream->dataset"
  [path & [options]]
  (stream->dataset path (assoc options :open-type :input-stream)))


(defn ^:no-doc read-stream-dataset-inplace
  "This method has been deprecated.  Please use stream->dataset"
  [path & [options]]
  (stream->dataset path (assoc options :open-type :mmap)))


(defn ^:no-doc write-dataset-to-stream!
  "This method has been deprecated.  Please use dataset->stream!"
  [ds path & [options]]
  (dataset->stream! ds path options))


(comment
  (require '[tech.v3.dataset :as ds])
  (def test-ds (ds/->dataset {:a ["one" "two" "three"]}))
  (def schema-data (ds->schema test-ds))
  (defn write-to-bytes
    ^bytes [write-fn]
    (let [{:keys [channel storage]} (arrow-in-memory-writer)]
      (write-fn channel)
      (-> (dtype/->buffer storage)
          (dtype/->byte-array))))
  (def data-bytes (write-to-bytes
                   (fn [writer]
                     (write-schema writer (:schema schema-data)))))
  (def dict (first (:dictionaries schema-data)))
  (def dict-bytes (write-to-bytes
                   (fn [writer]
                     (write-dictionary-batch writer dict))))
  (def dict-msg (-> (dtype/make-container :native-heap :int8 dict-bytes)
                    (read-message)
                    (read-dictionary-batch)))
  (def rehydrated-dict (dictionary->strings dict-msg))

  (def ds-bytes (write-to-bytes
                 (fn [writer]
                   (write-dataset-record-batch writer test-ds))))

  (write-dataset-to-stream! test-ds "testa.arrow" {:strings-as-text? true})
  (def read-data (read-stream-dataset-inplace "testa.arrow"))

  )
