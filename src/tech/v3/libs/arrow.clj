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

  Example (with zstd compression):

```clojure
  ;; Writing
  (arrow/dataset->stream! ds fname {:compression :zstd})
  ;; Reading
  (arrow/stream->dataset path)
```

  ## Required Dependencies

  In order to support both memory mapping and JDK-17, we only rely on the Arrow SDK's
  flatbuffer and schema definitions:

```clojure
  ;; netty isn't required and will inevitably conflict with some more recent version
  [org.apache.arrow/arrow-vector \"6.0.0\":exclusions [netty/netty io.netty/netty-common]]
  [com.cnuernber/jarrow \"1.000\"]
  [org.apache.commons/commons-compress \"1.21\"]

  ;;Compression codecs
  [org.lz4/lz4-java \"1.8.0\"]
  ;;Required for decompressing lz4 streams with dependent blocks.
  [net.java.dev.jna/jna \"5.10.0\"]
  [com.github.luben/zstd-jni \"1.5.4-1\"]
```
  The lz4 decompression system will fallback to lz4-java if liblz4 isn't installed or if
  jna isn't loaded.  The lz4-java java library will fail for arrow files that have dependent
  block compression which are sometimes saved by python or R arrow implementations.
  On current ubuntu, in order to install the lz4 library you need to do:

```console
  sudo apt install liblz4-1
  ```  

  ## Performance 

  Arrow has hands down highest performance of any of the formats although nippy comes very close when using
  any compression.  The highest performance pathway is to save out data with :strings-as-text? true and zero
  compression then read them in using mmap - optionally with :text-as-strings? if you never want to see 
  tech.v3.datatype.Text objects in your dataset.  This avoids the creation of string dictionaries during 
  deserialization as these have to be done greedily.  It can dramatically increase many dataset sizes but
  when mmap is used the overall size is irrelevant aside from iteration which can be heavily parallelized.

  Example:

```clojure
  ;; Writing
  (arrow/dataset->stream! ds fname {:strings-as-text? true})
  ;; Reading
  (arrow/stream->dataset path {:text-as-strings? true :open-type :mmap})
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
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.sparse-column :as sparse-col]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.utils :as ml-utils]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.resource :as resource]
            [tech.v3.io :as io]
            [clojure.tools.logging :as log]
            [clj-commons.primitive-math :as pmath]
            [clojure.core.protocols :as clj-proto]
            [clojure.datafy :refer [datafy]]
            [charred.api :as json]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.set :as set]
            [ham-fisted.protocols :as hamf-proto])
  (:import [ham_fisted ArrayLists ArrayLists$ArrayOwner IMutList]
           [org.apache.arrow.vector.ipc.message MessageSerializer]
           [org.apache.arrow.flatbuf Message DictionaryBatch RecordBatch
            FieldNode Buffer BodyCompression BodyCompressionMethod Footer Block]
           [org.roaringbitmap RoaringBitmap]
           [com.google.flatbuffers FlatBufferBuilder]
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.vector.types.pojo Field Schema ArrowType$Int
            ArrowType$Utf8 ArrowType$Timestamp ArrowType$Time DictionaryEncoding FieldType
            ArrowType$FloatingPoint ArrowType$Bool ArrowType$Date ArrowType$Duration
            ArrowType$LargeUtf8 ArrowType$Null ArrowType$List ArrowType$Binary ArrowType$FixedSizeBinary
            ArrowType$Decimal]
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
           [java.nio ByteBuffer ByteOrder ShortBuffer IntBuffer LongBuffer DoubleBuffer
            FloatBuffer]
           [java.util List ArrayList Map HashMap Map$Entry Iterator Set UUID Arrays]
           [java.util.concurrent ForkJoinTask]
           [java.time ZoneId]
           [java.nio.channels WritableByteChannel]
           ;;Compression codecs
           [com.github.luben.zstd Zstd]
           [org.apache.commons.compress.compressors.lz4 FramedLZ4CompressorInputStream
            FramedLZ4CompressorOutputStream]
           [net.jpountz.lz4 LZ4Factory]
           ;;feather support
           [java.io RandomAccessFile BufferedInputStream ByteArrayInputStream
            ByteArrayOutputStream]
           [uk.ac.bristol.star.feather FeatherTable BufUtils
            FeatherType]
           [uk.ac.bristol.star.fbs.feather Type]
           [uk.ac.bristol.star.fbs.feather CTable]))


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
    (if (instance? NativeBuffer compbuf)      
      (Zstd/decompress ^ByteBuffer (nio-buffer/as-nio-buffer resbuf)
                       ^ByteBuffer (nio-buffer/as-nio-buffer compbuf))
      (let [dst (byte-array (dtype/ecount resbuf))]
        (Zstd/decompress dst (dtype/->byte-array compbuf))
        (dtype/copy! dst resbuf)))))


#_(defn- create-apache-lz4-frame-compressor
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


(defn- create-jpnz-lz4-frame-compressor
  [comp-map]
  (assert (= :lz4 (get comp-map :compression-type)))
  (fn [compbuf dstbuf]
    (let [^ByteArrayOutputStream dstbuf (or dstbuf (ByteArrayOutputStream.))
          os (net.jpountz.lz4.LZ4FrameOutputStream. dstbuf)
          srcbuf (ensure-bytes-array-buffer compbuf)
          ^bytes src-data (.ary-data srcbuf)]
      (.write os src-data (unchecked-int (.offset srcbuf)) (unchecked-int (.n-elems srcbuf)))
      (.close os)
      (let [final-bytes (.toByteArray dstbuf)]
        (.reset dstbuf)
        {:writer-cache dstbuf
         :dst-buffer final-bytes}))))


(defonce ^:private init-liblz4* (delay ((requiring-resolve 'tech.v3.libs.arrow.liblz4/initialize!))))


(defn- ensure-native-buffer
  ^NativeBuffer [nbuf]
  (if-let [retval (dtype/as-native-buffer nbuf)]
    nbuf
    (dtype/make-container :native-heap :int8 {:resource-type :stack} nbuf)))

(defn- create-jpnz-lz4-frame-decompressor
  []
  (try
    @init-liblz4*
    (let [ctx-fn (requiring-resolve 'tech.v3.libs.arrow.liblz4/create-decomp-ctx)
          decomp-fn (requiring-resolve 'tech.v3.libs.arrow.liblz4/LZ4F_decompress)
          is-err-int (requiring-resolve 'tech.v3.libs.arrow.liblz4/LZ4F_isError)
          err-str (requiring-resolve 'tech.v3.libs.arrow.liblz4/LZ4F_getErrorName)
          decomp-ctx (ctx-fn)]
      (fn [srcbuf dstbuf]
        (resource/stack-resource-context
         (let [srcbuf (ensure-native-buffer srcbuf)
               n-dstbuf (ensure-native-buffer dstbuf)
               srcsize (dt-ffi/make-ptr :int64 (dtype/ecount srcbuf))
               dstsize (dt-ffi/make-ptr :int64 (dtype/ecount n-dstbuf))
               errcode (decomp-fn decomp-ctx
                                  n-dstbuf dstsize
                                  srcbuf srcsize
                                  nil)]
           (when-not (== 0 (long (is-err-int errcode)))
             (throw (Exception. (str (err-str errcode)))))
           (when-not (identical? n-dstbuf dstbuf)
             (dtype/copy! n-dstbuf dstbuf))
           dstbuf))))
    (catch Exception e
      (log/warn "Unable to load native lz4 library, falling back to jpountz.
Dependent block frames are not supported!!")
      (fn [srcbuf dstbuf]
        (let [src-byte-data (dtype/->byte-array srcbuf)
              bis (ByteArrayInputStream. src-byte-data)
              is (net.jpountz.lz4.LZ4FrameInputStream. bis)
              temp-dstbuf (byte-array (dtype/ecount dstbuf))]
          (.read is temp-dstbuf)
          (dtype/copy! temp-dstbuf dstbuf))))))


(def ^:private compression-info
  {:lz4 {:file-type CompressionType/LZ4_FRAME
         :compressor-fn create-jpnz-lz4-frame-compressor
         :decompressor-fn create-jpnz-lz4-frame-decompressor}
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
  ArrowType$Null
  (datafy [this]
    {:datatype :boolean
     :subtype :null})
  ArrowType$Utf8
  (datafy [this]
    {:datatype :string
     :encoding :utf-8
     :offset-buffer-datatype :uint32})
  ArrowType$Binary
  (datafy [this]
    {:datatype :binary})
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
       TimeUnit/NANOSECOND {:datatype :epoch-nanoseconds})
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
                  :index-type (datafy (.getIndexType this))})
  ArrowType$FixedSizeBinary
  (datafy [this] {:datatype :fixed-size-binary
                  :byte-width (.getByteWidth this)})
  ArrowType$Decimal
  (datafy [this] {:datatype :decimal
                  :scale (.getScale this)
                  :precision (.getPrecision this)
                  :bit-width (.getBitWidth this)})
  ArrowType$List
  (datafy [this]
    {:datatype :list}))


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

(defn- read-msg-size
  [data]
  (let [msg-size (native-buffer/read-int data)]
    (if (== -1 msg-size)
      [(native-buffer/read-int data 4) 8]
      [msg-size 4])))

(deftype MessageIter [^:unsynchronized-mutable data]
  java.util.Iterator
  (hasNext [this]
    (and (> (dtype/ecount data) 4)
         (not (== 0 (long (nth (read-msg-size data) 0))))))
  (next [this]
    (let [[msg-size offset] (read-msg-size data)
          offset (long offset)
          msg-size (long msg-size)]
      (let [new-msg (Message/getRootAsMessage
                     (-> (dtype/sub-buffer data offset msg-size)
                         (nio-buffer/native-buf->nio-buf)))
            next-buf (dtype/sub-buffer data (+ offset msg-size))
            body-length (.bodyLength new-msg)
            aligned-offset (pad (+ offset msg-size body-length))
            rv {:message new-msg
                :message-type (message-id->message-type (.headerType new-msg))}]
        (set! data (dtype/sub-buffer data aligned-offset))
        (if-not (== 0 body-length)
          (assoc rv :body (dtype/sub-buffer next-buf 0 (.bodyLength new-msg)))
          rv)))))


(defn- message-iterable
  "Given a native buffer of arrow stream data, produce a sequence of flatbuf messages"
  ^Iterable [^NativeBuffer data]
  (reify Iterable
    (iterator [this] (MessageIter. data))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reading messages from streams
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- read-int-LE
  (^long [^InputStream is byte-buf]
   (let [^bytes byte-buf (or byte-buf (byte-array 4))]
     (if (== 4 (.read is byte-buf))       
       (ByteConversions/intFromBytesLE (aget byte-buf 0) (aget byte-buf 1)
                                       (aget byte-buf 2) (aget byte-buf 3))
       0)))
  (^long [^InputStream is]
   (read-int-LE is nil)))

(defn- read-stream-msg-size
  ^long [^InputStream is]
  (let [msg-size (read-int-LE is)]
    (if (== -1 msg-size)
      (read-int-LE is)
      msg-size)))

(deftype StreamMessageIter [^InputStream is ^{:unsynchronized-mutable true
                                              :tag long} msg-size
                            close-input-stream?]
  Iterator
  (hasNext [this]    
    (if (== 0 msg-size)
      (do 
        (when close-input-stream? (.close is))
        false)
      true))
  (next [this]
    (let [pad-msg-size (pad msg-size)
          bytes (byte-array pad-msg-size)
          _ (.read is bytes)
          new-msg (Message/getRootAsMessage (nio-buffer/as-nio-buffer
                                             (dtype/sub-buffer bytes 0 msg-size)))
          body-len (.bodyLength new-msg)
          pad-body-len (pad body-len)
          nbuf (when-not (== 0 body-len)
                 (dtype/make-container :int8 pad-body-len))
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
        (when close-input-stream? (.close is))
        (throw (Exception. (format "Unable to read entire buffer - Expected %d got %d"
                                   pad-body-len bytes-read))))
      (set! msg-size (read-stream-msg-size is))
      (merge {:message new-msg
              :message-type (message-id->message-type (.headerType new-msg))}
             (when nbuf
               {:body (dtype/sub-buffer nbuf 0 body-len)})))))

(defn- stream-message-iterable
  ^Iterable [is options]
  (reify Iterable
    (iterator [this] (StreamMessageIter. is (read-stream-msg-size is)
                                         (get options :close-input-stream? true)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datatype to schema mapping
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Create a dictionary and dictionary encoding from a string column
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- string-column->dict
  [col]
  (let [metadata (meta col)
        colname (:name metadata)
        dict-id (.hashCode ^Object colname)
        str-t (ds-base/ensure-column-string-table col)
        ^StringTable prev-str-t (::previous-string-table metadata)
        int->str (str-table/int->string str-t)
        indices (dtype-proto/->array-buffer (str-table/indices str-t))
        n-elems (.size int->str)
        bit-width (casting/int-width (dtype/elemwise-datatype indices))
        arrow-indices-type (ArrowType$Int. bit-width true)
        encoding (DictionaryEncoding. dict-id false arrow-indices-type)
        byte-data (dtype/make-list :int8)
        ;;offsets are int32 apparently
        offsets (dtype/make-list :int32)]
    (if (nil? prev-str-t)
      (dotimes [str-idx (count int->str)]
        (let [strdata (.get int->str str-idx)
              _ (when (= strdata :failure)
                  (throw (Exception. "Invalid string table - missing entries.")))
              str-bytes (.getBytes (str strdata))
              soff (dtype/ecount byte-data)]
          (.addAllReducible byte-data (ArrayLists/toList str-bytes))
          (.add offsets soff)))
      (let [prev-int->str (str-table/int->string prev-str-t)
            start-offset (dtype/ecount prev-int->str)
            n-extra (- (dtype/ecount int->str) (dtype/ecount prev-int->str))
            _ (assert (>= n-extra 0) "Later string tables should only be larger")]
        (dotimes [str-idx n-extra]
          (let [strdata (int->str (+ str-idx start-offset))
                _ (when (= strdata :failure)
                    (throw (Exception. "Invalid string table - missing entries.")))
                str-bytes (.getBytes (str strdata))
                soff (dtype/ecount byte-data)]
            (.addAllReducible byte-data (ArrayLists/toList str-bytes))
            (.add offsets soff)))))
    ;;Make everyone's life easier by adding an extra offset.
    (.add offsets (dtype/ecount byte-data))
    {:encoding encoding
     :string-table str-t
     :byte-data byte-data
     :offsets (dtype-proto/->array-buffer offsets)
     :is-delta? (boolean prev-str-t)}))


(defn- string-col->encoding
  "Given a string column return a map of :dict-id :table-width.  The dictionary
  id is the hashcode of the column mame."
  [^List dictionaries col]
  (let [dict (string-column->dict col)]
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
       (map (fn [[k v]] [(json/write-json-str k) (json/write-json-str v)]))
       (into {})))

(def ^{:private true
       :tag String} ARROW_EXTENSION_NAME "ARROW:extension:name")
(def ^{:private true
       :tag String} ARROW_UUID_NAME "arrow.uuid")

(defn- datatype->field-type
  (^FieldType [datatype & [nullable? metadata extra-data]]
   (let [nullable? (or nullable? (= :object (casting/flatten-datatype datatype)))
         metadata (->str-str-meta (dissoc metadata
                                          :name :datatype :categorical?
                                          ::previous-string-table
                                          ::complex-datatype))
         ft-fn (fn [arrow-type & [dict-encoding]]
                 (field-type nullable? arrow-type dict-encoding metadata))
         complex-datatype datatype
         datatype (if (map? complex-datatype)
                    (get complex-datatype :datatype)
                    datatype)
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
       :epoch-microseconds (ft-fn (ArrowType$Timestamp. TimeUnit/MICROSECOND
                                                        (str (:timezone extra-data))))
       :epoch-nanoseconds (ft-fn (ArrowType$Timestamp. TimeUnit/NANOSECOND
                                                       (str (:timezone extra-data))))
       :epoch-days (ft-fn (ArrowType$Date. DateUnit/DAY))
       :local-date (ft-fn (ArrowType$Date. DateUnit/DAY))
       ;;packed local time is 64bit microseconds since midnight
       :local-time (ft-fn (ArrowType$Time. TimeUnit/MICROSECOND (int 64)))
       :time-nanoseconds (ft-fn (ArrowType$Time. TimeUnit/NANOSECOND (int 64)))
       :time-microseconds (ft-fn (ArrowType$Time. TimeUnit/MICROSECOND (int 64)))
       :time-milliseconds (ft-fn (ArrowType$Time. TimeUnit/MILLISECOND (int 32)))
       :time-seconds (ft-fn (ArrowType$Time. TimeUnit/SECOND (int 32)))
       :duration (ft-fn (ArrowType$Duration. TimeUnit/MICROSECOND))
       :string (if-let [^DictionaryEncoding encoding (:encoding extra-data)]
                 (ft-fn (ArrowType$Utf8.) encoding)
                 ;;If no encoding is provided then just save the string as text
                 (ft-fn (ArrowType$Utf8.)))
       :decimal (ft-fn (ArrowType$Decimal. (unchecked-int (get complex-datatype :precision))
                                           (unchecked-int (get complex-datatype :scale))
                                           (unchecked-int (get complex-datatype :bit-width))))
       :uuid (ft-fn (ArrowType$FixedSizeBinary. 16))
       :text (ft-fn (ArrowType$Utf8.))
       :encoded-text (ft-fn (ArrowType$Utf8.))))))


(defn- col->field
  ^Field [dictionaries {strings-as-text? :strings-as-text?}
          col]
  (let [colmeta (meta col)
        colmeta (if (identical? :uuid (get colmeta :datatype))
                  (assoc colmeta ARROW_EXTENSION_NAME ARROW_UUID_NAME)
                  colmeta)
        nullable? (boolean
                   (or (:nullable? colmeta)
                       (not (empty? (ds-proto/missing col)))))
        col-dtype (or (::complex-datatype colmeta) (:datatype colmeta))
        colname (:name colmeta)
        extra-data (merge (select-keys (meta col) [:timezone])
                          (when (and (not strings-as-text?)
                                     (= :string col-dtype))
                            (string-col->encoding dictionaries col)))]
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


(defn- try-json-parse
  [val]
  (when val
    (try (json/read-json val :key-fn keyword)
         (catch Exception e
           val))))


(defn- datafy-field
  [^Field field]
  (let [df (datafy (.. field getFieldType getType))]
    (when-not (map? df)
      (throw (Exception. (format "Failed to dataty datatype %s-%s"
                                 field (type (.. field getFieldType getType))))))
    (merge
     {:name (.getName field)
      :nullable? (.isNullable field)
      :field-type df
      :metadata (->> (.getMetadata field)
                     (map (fn [^Map$Entry entry]
                            [(try-json-parse (.getKey entry))
                             (try-json-parse (.getValue entry))]))
                     (into {}))}
     (when-let [c (seq (.getChildren field))]
       {:children (mapv datafy-field c)})
     (when-let [encoding (.getDictionary field)]
       {:dictionary-encoding (datafy encoding)}))))


(defn- read-schema
  "returns a pair of offset-data and schema"
  [{:keys [message _body _message-type]}]
  (let [schema (MessageSerializer/deserializeSchema ^Message message)
        ;; _ (println schema)
        fields
        (->> (.getFields schema)
             (mapv datafy-field))
        metadata (when-let [metadata (.getCustomMetadata schema)]
                   (into {} metadata))
        sparse-columns (when-let [sparse-columns (get metadata "sparse-columns")]
                         (->> (.split ^String sparse-columns ",")
                              (map #(Long/parseLong %))
                              (hamf/java-hashset)))]
    {:fields fields
     :encodings (->> (map :dictionary-encoding fields)
                     (remove nil?)
                     (map (juxt :id identity))
                     (into {}))
     :metadata metadata
     :sparse-columns sparse-columns}))


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


(defn- LE-wrap-data
  ^java.nio.ByteBuffer [buffer]
  (let [^java.nio.ByteBuffer bbuf (nio-buffer/->nio-buffer buffer)]
    (.order bbuf java.nio.ByteOrder/LITTLE_ENDIAN)))

(defn- read-long
  ^long [buffer]
  (if (instance? NativeBuffer buffer)
    (native-buffer/read-long buffer)
    (-> (LE-wrap-data buffer)
        (.getLong))))


(defn- decompress-buffers
  [^BodyCompression compression buffers]
  (if-not compression
    buffers
    (do 
      (when-not (== 0 (.method compression))
        (throw (Exception. (format "Only buffer batch compression supported - got %d"
                                   (.method compression)))))
      (let [decompressor (create-decompressor (.codec compression))
            buf-type (if (instance? NativeBuffer (hamf/first buffers))
                       :native-heap
                       :jvm-heap)
            buffers (mapv (fn [buffer]
                            (if (> (dtype/ecount buffer) 8)
                              (let [orig-len (read-long buffer)]
                                {:orig-len orig-len
                                 :buffer (dtype/sub-buffer buffer 8)})
                              {:orig-len (dtype/ecount buffer)
                               :buffer buffer}))
                          buffers)
            ;;All results get decompressed into one final buffer
            decomp-buf-len (->> (lznc/map :orig-len buffers)
                                (lznc/remove #(== -1 (long %)))
                                (reduce + 0)
                                (long))
            decomp-buf (dtype/make-container buf-type :int8 decomp-buf-len)]
        (->> buffers
             (reduce (fn [[res decomp-buf] {:keys [orig-len buffer]}]
                       ;;-1 indicates the buffer isn't actually compressed.
                       ;;And some buffers are just empty with size of 0
                       #_(println orig-len (dtype/ecount buffer)
                                  (when (> (dtype/ecount buffer) 32)
                                    (-> (native-buffer/set-native-datatype buffer :int32)
                                        (dtype/sub-buffer 0 8)
                                        (vec))))
                       (if (<= (long orig-len) 0)
                         [(conj res buffer) decomp-buf]
                         [(conj res (decompressor
                                     buffer (dtype/sub-buffer decomp-buf 0 orig-len)))
                          (dtype/sub-buffer decomp-buf orig-len)]))
                     [[] decomp-buf])
             (first))))))


(defn- read-record-batch
  ([^RecordBatch record-batch ^NativeBuffer data]
   {:compression (.compression record-batch)
    :nodes (->> (hamf/range (.nodesLength record-batch))
                (mapv #(let [node (.nodes record-batch (unchecked-int %))]
                         {:n-elems (.length node)
                          :n-null-entries (.nullCount node)})))
    :buffers (->> (hamf/range (.buffersLength record-batch))
                  (mapv #(let [buffer (.buffers record-batch (unchecked-int %))]
                           (dtype/sub-buffer data (.offset buffer)
                                             (.length buffer)))))})
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

(defn- as-shorts
  ^shorts [abuf]
  (let [bbuf (LE-wrap-data abuf)
        rv (short-array (quot (dtype/ecount abuf)  2))]
    (-> (.asShortBuffer bbuf)
        (.get rv))
    rv))

(defn- as-ints
  ^ints [abuf]
  (let [bbuf (LE-wrap-data abuf)
        rv (int-array (quot (dtype/ecount abuf) 4))]
    (-> (.asIntBuffer bbuf)
        (.get rv))
    rv))

(defn- as-longs
  ^longs [abuf]
  (let [bbuf (LE-wrap-data abuf)
        rv (long-array (quot (dtype/ecount abuf) 8))]
    (-> (.asLongBuffer bbuf)
        (.get rv))
    rv))

(defn- as-floats
  ^floats [abuf]
  (let [bbuf (LE-wrap-data abuf)
        rv (float-array (quot (dtype/ecount abuf) 4))]
    (-> (.asFloatBuffer bbuf)
        (.get rv))
    rv))

(defn- as-doubles
  ^doubles [abuf]
  (let [bbuf (LE-wrap-data abuf)
        rv (double-array (quot (dtype/ecount abuf) 8))]
    (-> (.asDoubleBuffer bbuf)
        (.get rv))
    rv))


(defn- set-buffer-datatype
  [buffer dtype]
  (if (instance? tech.v3.datatype.native_buffer.NativeBuffer buffer)
    (native-buffer/set-native-datatype buffer dtype)
    (let [abuf (dtype/->array-buffer buffer)]
      (let [offset (.-offset abuf)
            n-elems (.-n-elems abuf)
            src-data (.-ary-data abuf)]
        (case (casting/host-flatten dtype)
          (:int8 :uint8) (ArrayBuffer. src-data offset n-elems dtype nil nil)
          (:int16 :uint16) (ArrayBuffer. (as-shorts abuf) 0 (quot n-elems 2) dtype nil nil)
          (:int32 :uint32) (ArrayBuffer. (as-ints abuf) 0 (quot n-elems 4) dtype nil nil)
          (:int64 :uint64) (ArrayBuffer. (as-longs abuf) 0 (quot n-elems 8) dtype nil nil)
          :float32 (ArrayBuffer. (as-floats abuf) 0 (quot n-elems 4) dtype nil nil)
          :float64 (ArrayBuffer. (as-doubles abuf) 0 (quot n-elems 8) dtype nil nil))))))

(defn- ->jvm-array
  [buffer ^long off ^long len]
  (if (instance? NativeBuffer buffer)
    (native-buffer/->jvm-array buffer off len)
    (let [abuf (dtype/->array-buffer buffer)
          ary-data (.-ary-data abuf)
          offset (.-offset abuf)
          n-elems (.-n-elems abuf)]
      (if (and (== (.-offset abuf) off)
               (== (.-n-elems abuf) len))
        (.-ary-data abuf)
        (let [^ArrayLists$ArrayOwner owner  (ArrayLists/toList ary-data)]
          (.copyOfRange owner offset (+ offset n-elems)))))))

(defn- byte-length
  ^long [container]
  (* (dtype/ecount container) (casting/numeric-byte-width
                               (dtype/elemwise-datatype container))))


(defn- serialize-to-bytes
  "Serialize a numeric buffer to a byte buffer."
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
  [options buffers]
  (if-let [comp-map (get options :compression)]
    (let [buffers (vec buffers)
          n-buffers (count buffers)]
      {:compression-type (compression-kwd->file-type (comp-map :compression-type))
       ;;parallelize buffer compression
       :buffers 
       (->> buffers
            (hamf/pmap
             (fn [buffer]
               (let [buffers [buffer]
                     comp-fn (create-compressor comp-map)
                     os (ByteArrayOutputStream.)
                     data-len (byte-array 8)
                     ^ByteBuffer nio-buf (nio-buffer/as-nio-buffer data-len)
                     data-len-buf (dtype/->buffer data-len)]
                 (-> (reduce (fn [[res writer-cache] buffer]
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
                             buffers)
                     (first)))))
            (lznc/apply-concat)
            (vec))})
    {:buffers (vec (hamf/pmap #(-> (serialize-to-bytes %)
                                   (nio-buffer/as-nio-buffer))
                              buffers))}))

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


(defn- block-data
  [^long msg-start ^long data-start ^long data-end]
  {:offset msg-start
   :metadata-len (- data-start msg-start)
   :data-len (- data-end data-start)})


(defn- write-block-data
  [^FlatBufferBuilder builder block-data]
  (Block/createBlock builder (long (block-data :offset))
                     (int (block-data :metadata-len))
                     (long (block-data :data-len))))


(defn- write-dictionary
  "Write a dictionary to a dictionary batch.  Returns enough information to construct
  a Block entry in the footer."
  [^WriteChannel writer {:keys [byte-data offsets encoding is-delta?]} options]
  (let [n-elems (dec (count offsets))
        missing (no-missing n-elems)
        {:keys [compression-type buffers]}
        (->> [missing offsets byte-data] (compress-record-batch-buffers options))
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
                         (long (last-entry :length))))
        bbuf (finish-builder builder
                             (message-type->message-id :dictionary-batch)
                             (DictionaryBatch/createDictionaryBatch
                              builder enc-id rbatch-off (boolean is-delta?))
                             body-len)
        msg-start (.getCurrentPosition writer)
        _ (write-message-header writer bbuf)
        data-start (.getCurrentPosition writer)
        _ (doseq [buf buffers]
            (.write writer ^ByteBuffer buf)
            (.align writer))
        data-end (.getCurrentPosition writer)]
    (block-data msg-start data-start data-end)))


(defn- toggle-bit
  ^bytes [^bytes data ^long bit]
  (let [idx (quot bit 8)
        bit (rem bit 8)
        existing (unchecked-int (aget data idx))]
    (ArrayHelpers/aset data idx (unchecked-byte (bit-xor existing (bit-shift-left 1 bit)))))
  data)

(defn validity-info
  "returns [byte-array n-missing]"
  [col ^bytes all-valid-buf]
  (let [nbuf all-valid-buf
        n-missing (ds-proto/missing-count col)]
    [(if (== 0 n-missing)
       nbuf       
       (if (sparse-col/is-sparse? col)
         (reduce toggle-bit (byte-array (alength nbuf)) (first (ds-proto/column-buffer col)))
         (reduce toggle-bit (Arrays/copyOf nbuf (alength nbuf)) (ds-proto/missing col))))
     n-missing]))

(defn ^:no-doc boolean-bytes
  ^bytes [data]
  (let [rdr (dtype/->reader data)
        len (.lsize rdr)
        retval (byte-array (len->bitwise-len len))]
    (dotimes [idx len]
      (when (.readObject rdr idx)
        (toggle-bit retval idx)))
    retval))


(defn col->buffers
  [col ^long col-idx options]
  (let [col-dt (casting/un-alias-datatype (dtype/elemwise-datatype col))
        col-dt (if (and (:strings-as-text? options) (= col-dt :string))
                 :text
                 col-dt)
        cbuf (tech.v3.datatype.protocols/->buffer col)
        sparse? (contains? (get options :sparse-columns) col-idx)
        ^tech.v3.datatype.Buffer cbuf
        (if sparse?                    
          (if (sparse-col/is-sparse? col)
            (tech.v3.datatype.protocols/->buffer (ds-proto/column-data col))
            (let [c (col-base/make-container (dtype/elemwise-datatype col))
                  ^RoaringBitmap missing (ds-proto/missing col)]
              (dotimes [idx (dtype/ecount col)]
                (when-not (.contains missing (unchecked-int idx))
                  (.add c (.readObject cbuf idx))))
              (dtype/->buffer c)))
          cbuf)]
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
        :uuid (let [data (byte-array (* 16 (dtype/ecount cbuf)))
                    wbuf (-> (java.nio.ByteBuffer/wrap data)
                             (.order java.nio.ByteOrder/BIG_ENDIAN))]
                (reduce (fn [_ ^UUID v]
                          (if v
                            (do 
                              (.putLong wbuf (.getMostSignificantBits v))
                              (.putLong wbuf (.getLeastSignificantBits v)))
                            (do
                              (.putLong wbuf 0) (.putLong wbuf 0))))
                        nil cbuf)
                [(java.nio.ByteBuffer/wrap data)])
        :decimal (let [colmeta (meta col)
                       {:keys [scale precision bit-width]} (get colmeta ::complex-datatype)
                       byte-width (quot (+ (long bit-width) 7) 8)
                       ne (.lsize cbuf)
                       byte-data (byte-array (* ne byte-width))
                       le? (identical? :little-endian (tech.v3.datatype.protocols/platform-endianness))]
                   (dotimes [idx ne]
                     (when-let [^BigDecimal d (.readObject cbuf idx)]
                       (let [^BigInteger bb (.unscaledValue d)
                             bb-bytes (.toByteArray bb)
                             offset (* idx byte-width)]
                         (if le?
                           (let [bb-len (alength bb-bytes)]
                             (dotimes [bidx bb-len]
                               (let [write-pos (+ offset (- bb-len bidx 1))]
                                 (ArrayHelpers/aset byte-data write-pos (aget bb-bytes bidx)))))
                           (System/arraycopy bb-bytes 0 byte-data 0 (alength bb-bytes))))))
                   [(java.nio.ByteBuffer/wrap byte-data)])
        :string (let [str-t (ds-base/ensure-column-string-table col)
                      indices (dtype-proto/->array-buffer (str-table/indices str-t))]
                  [(nio-buffer/as-nio-buffer indices)])
        :text
        (let [byte-data (dtype/make-list :int8)
              offsets (dtype/make-list :int32)]
          (reduce (fn [_ strdata]
                    (let [strdata (str (or strdata ""))]
                      (.add offsets (.size byte-data))
                      (.addAllReducible byte-data (ArrayLists/toList (.getBytes strdata))))) nil cbuf)
          (.add offsets (.size byte-data))
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
        (->> (.values ^Map dataset)
             (map-indexed vector)
             (hamf/pmap (fn [[col-idx col]]
                          (let [[valid-buf n-missing] (validity-info col all-valid-buf)
                                buffers (hamf/concatv [valid-buf] (col->buffers col col-idx options))
                                lengths (hamf/mapv (comp pad byte-length) buffers)
                                col-len (long (reduce + 0 lengths))]
                            {:node {:n-elems n-rows
                                    :n-null-entries n-missing}
                             :buffers buffers
                             :length col-len})))
             (vec))
        nodes (lznc/map :node nodes-buffs-lens)
        {:keys [compression-type buffers]}
        (->> (lznc/map :buffers nodes-buffs-lens)
             (lznc/apply-concat)
             (compress-record-batch-buffers options))

        buf-entries (buffers->buf-entries buffers)
        last-entry (last buf-entries)
        body-len (if last-entry
                   (pad (+ (long (last-entry :offset)) (long (last-entry :length))))
                   0)
        builder (FlatBufferBuilder.)
        msg-start (.getCurrentPosition writer)
        _ (write-message-header writer
                                (finish-builder
                                 ;;record-batch message type
                                 builder (message-type->message-id :record-batch)
                                 (write-record-batch-header builder n-rows nodes buf-entries
                                                            compression-type)
                                 body-len))
        data-start (.getCurrentPosition writer)
        _ (doseq [buf buffers]
            (.write writer ^ByteBuffer buf)
            (.align writer))
        data-end (.getCurrentPosition writer)]
    (block-data msg-start data-start data-end)))


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
    (dtype/make-reader-fn
     :string :string n-elems
     (if (instance? NativeBuffer data)
       (fn [^long idx]
         (let [start-off (long (offsets idx))
               end-off (long (offsets (inc idx)))]
           (native-buffer/native-buffer->string data start-off (- end-off start-off))))
       (let [abuf (dtype/->array-buffer data)
             ^bytes src-data (.-ary-data abuf)
             data-off (.-offset abuf)
             ]
         (fn [^long idx]
           (let [start-off (.readLong offsets idx)
                 end-off (.readLong offsets (inc idx))]
             (String. src-data (+ start-off data-off) (- end-off start-off)))))))))

(defn- offsets-data->bytedata-reader
  ^List [offsets data n-elems]
  (let [n-elems (long n-elems)
        offsets (dtype/->reader offsets)]
    (reify ObjectReader
      (elemwiseDatatype [rdr] :object)
      (lsize [rdr] n-elems)
      (readObject [rdr idx]
        (let [start-off (long (offsets idx))
              end-off (long (offsets (inc idx)))]
          (-> (dtype/sub-buffer data start-off
                                (- end-off start-off))
              (dtype/->byte-array)))))))


(defn- dictionary->strings
  "Returns a map of {:id :strings}"
  [{:keys [id delta? records]}]
  (let [nodes (:nodes records)
        buffers (decompress-buffers (:compression records) (:buffers records))
        _ (assert (== 1 (count nodes)))
        _ (assert (== 3 (count buffers)))
        node (first nodes)
        [_bitwise offsets databuf] buffers
        n-elems (long (:n-elems node))
        offsets (-> (set-buffer-datatype offsets :int32)
                    (dtype/sub-buffer 0 (inc n-elems)))
        data (set-buffer-datatype databuf :int8)
        str-data (dtype/make-list :string (offsets-data->string-reader offsets data n-elems))]
    {:id id
     :delta? delta?
     :strings str-data}))


(defn- string-reader->text-reader
  ^Buffer [item]
  (let [data-buf (dtype/->reader item)]
    (reify
      ObjectReader
      (elemwiseDatatype [rdr] :text)
      (lsize [rdr] (.lsize data-buf))
      (readObject [rdr idx]
        (when-let [data (.readObject data-buf idx)]
          (Text. (str data)))))))


(defn- string-data->column-data
  [dict-map encoding offset-buf-dtype buffers n-elems options]
  (if encoding
    (StringTable. (-> (get dict-map (get encoding :id))
                      deref
                      (get :strings))
                  nil
                  (-> (first buffers)
                      (set-buffer-datatype (get-in encoding [:index-type :datatype]))
                      (->jvm-array 0 n-elems)
                      (ArrayLists/toList)))
    (let [[offsets varchar-data] buffers
          str-rdr (offsets-data->string-reader (set-buffer-datatype
                                                offsets offset-buf-dtype)
                                               varchar-data n-elems)]
      (if-not (:text-as-strings? options)
        (string-reader->text-reader str-rdr)
        str-rdr))))

(defn validity->missing
  ^RoaringBitmap [validity ^long n-elems]
  (hamf-rf/reduce-reducer
   (hamf-rf/long-consumer-reducer
    #(tech.v3.dataset.ByteValidity$MissingIndexReducer. n-elems (* 8 (dtype/ecount validity))))
   validity))

(defn validity->indexes
  ^IMutList [validity ^long n-elems]
  (->> validity
       (hamf-rf/reduce-reducer
        (hamf-rf/long-consumer-reducer
         #(tech.v3.dataset.ByteValidity$ValidityIndexReducer. n-elems (* 8 (dtype/ecount validity)))))))

(defn ^:no-doc byte-buffer->bitwise-boolean-buffer
  ^Buffer[bitbuffer ^long n-elems]
  (let [buf (dtype/->buffer bitbuffer)]
    (dtype/make-reader :boolean n-elems
                       (let [data (.readByte buf (quot idx 8))
                             shift-val (pmath/bit-shift-left 1 (rem idx 8))]
                         (if (pmath/== shift-val (pmath/bit-and data shift-val))
                           true
                           false)))))

(defn- field-node-count
  [field]
  (apply + 1 (map field-node-count
                  (get field :children))))

(defn- field-metadata
  [field]
  (-> (into (dissoc (:field-type field) :datatype) (:metadata field))
      ;;Fixes issue 330
      (with-meta nil)
      (assoc :name (:name field))))

(defmulti ^:private preparse-field
  "Transform a field into a column."
  (fn [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
    (get-in field [:field-type :datatype])))

(defn construct-column
  [sparse? node field buffers col-data-fn]
  (let [rc (long (:n-elems node))
        n-missing (long (:n-null-entries node))
        metadata (field-metadata field)
        validity-buf (nth buffers 0)
        data-buffers (subvec buffers 1)]    
    (if sparse?
      (let [^IMutList indexes (validity->indexes validity-buf rc)]
        (sparse-col/construct-sparse-col indexes (col-data-fn (.size indexes) data-buffers) rc metadata))
      (col-impl/construct-column (if (== 0 n-missing)
                                   (bitmap/->bitmap)
                                   (validity->missing validity-buf rc))
                                 (col-data-fn rc data-buffers)
                                 metadata))))

(defmethod ^:private preparse-field :string
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
  (assert (= 0 (count (:children field)))
          (format "Field %s cannot be parsed with default parser" field))
  (let [encoding (get field :dictionary-encoding)
        buffers (if (nil? encoding)
                  [(.next buf-iter) (.next buf-iter) (.next buf-iter)]
                  [(.next buf-iter) (.next buf-iter)])
        node (.next node-iter)
        col-data-fn (fn [n-elems data-buffers]
                      (string-data->column-data
                       dict-map encoding
                       (get-in field [:field-type :offset-buffer-datatype])
                       data-buffers
                       n-elems
                       options))]
    (fn parse-string-field
      [decompressor]
      (construct-column sparse? node field (decompressor buffers) col-data-fn))))

(defmethod ^:private preparse-field :boolean
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
  (assert (= 0 (count (:children field)))
          (format "Field %s cannot be parsed with default parser" field))
  (let [field-dtype (get-in field [:field-type :datatype])
        node (.next node-iter)
        nil-subtype? (identical? :null (get-in field [:field-type :subtype]))
        buffers (when-not nil-subtype?
                  [(.next buf-iter) (.next buf-iter)])
        col-data-fn (fn [n-elems data-buffers]
                      (byte-buffer->bitwise-boolean-buffer
                       (first data-buffers) n-elems))]
    (fn parse-boolean-field
      [decompressor]
      ;;nil subtype means null column
      (if nil-subtype?
        (let [n-elems (long (:n-elems node))]
          (col-impl/new-column
           (:name field)
           (dtype/const-reader false n-elems)
           (field-metadata field)
           (bitmap/->bitmap (range n-elems))))
        (construct-column sparse? node field (decompressor buffers) col-data-fn)))))


(defn- clone-downcast-text
  [dbuf]
  (if (= :text (dtype/elemwise-datatype dbuf))
    (dtype/make-container :string (dtype/emap str :string dbuf))
    (dtype/clone dbuf)))


(defmethod ^:private preparse-field :list
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
  (let [buffers [(.next buf-iter) (.next buf-iter)]
        node (.next node-iter)
        _ (assert (== 1 (count (:children field)))
                  (format "List types are expected to have exactly 1 child - found %d"
                          (count (:children field))))
        ;;L
        sub-column-parse-fn (preparse-field (first (:children field)) false node-iter buf-iter dict-map options)]
    (fn parse-list-field
      [decompressor]
      (let [sub-column (-> (sub-column-parse-fn decompressor)
                           ;;copy to jvm memory.  This is a quick insurance policy to ensure
                           ;;if this buffer leaves the resource context it doesn't contain
                           ;;native memory as only the parent buffer is cloned.  A faster
                           ;;but more involved option would be to make a custom reader with
                           ;;an overloaded clone pathway.
                           (clone-downcast-text)
                           (dtype/->buffer))
            col-data-fn (fn [^long n-elems data-buffers]
                          (let [offset-buf (first data-buffers)
                                offsets (-> (set-buffer-datatype
                                             offset-buf :int32)
                                            (dtype/sub-buffer 0 (inc n-elems))
                                            (dtype/->buffer))]
                            (dtype/make-reader :object n-elems
                                               (let [sidx (.readLong offsets idx)]
                                                 (dtype/sub-buffer sub-column
                                                                   sidx
                                                                   (- (.readLong offsets (unchecked-inc idx))
                                                                      sidx))))))]
        (construct-column sparse? node field (decompressor buffers) col-data-fn)))))

(defmethod ^:private preparse-field :binary
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
  (let [node (.next node-iter)
        buffers [(.next buf-iter) (.next buf-iter) (.next buf-iter)]
        col-data-fn (fn [^long n-elems data-buffers]
                      (let [[offset-buf data-buf] data-buffers
                            offsets (-> (native-buffer/set-native-datatype
                                         offset-buf :int32)
                                        (dtype/sub-buffer 0 (inc n-elems))
                                        (dtype/->buffer))
                            data-buf (-> (dtype/clone data-buf)
                                         (dtype/->buffer))]
                        (dtype/make-reader :object n-elems
                                           (let [sidx (.readLong offsets idx)]
                                             (dtype/sub-buffer data-buf
                                                               sidx
                                                               (- (.readLong offsets (unchecked-inc idx))
                                                                  sidx))))))]
    (fn parse-binary-field
      [decompressor]
      (construct-column sparse? node field (decompressor buffers) col-data-fn))))

(defmethod ^:private preparse-field :fixed-size-binary
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
  (let [node (.next node-iter)
        buffers [(.next buf-iter) (.next buf-iter)]
        field-width (long (get-in field [:field-type :byte-width]))
        fm (field-metadata field)
        col-data-fn (fn [n-elems data-buffers]
                      (let [data-buf (first data-buffers)
                            ^bytes data-ary (if (instance? NativeBuffer data-buf)
                                              (native-buffer/->jvm-array data-buf 0 (dtype/ecount data-buf))
                                              (dtype/->array data-buf))]
                        (if (= ARROW_UUID_NAME (get fm ARROW_EXTENSION_NAME))
                          (let [longsdata (-> (java.nio.ByteBuffer/wrap data-ary)
                                              (.order (java.nio.ByteOrder/BIG_ENDIAN)))]
                            (dtype/make-reader :uuid n-elems
                                               (let [lidx (* idx 16)]
                                                 (java.util.UUID. (.getLong longsdata lidx)
                                                                  (.getLong longsdata (+ lidx 8))))))
                          (let [ll (ArrayLists/toList data-ary)]
                            (dtype/make-reader :object n-elems
                                               (let [lidx (* idx field-width)]
                                                 (.subList ll lidx (+ lidx field-width))))))))]
    (fn parse-fixed-binary-field
      [decompressor]
      (construct-column sparse? node field (decompressor buffers) col-data-fn))))

(defn- copy-bytes
  ^bytes [^bytes data ^long sidx ^long eidx]
  (Arrays/copyOfRange data sidx eidx))

(defn- copy-reverse-bytes
  ^bytes [^bytes data ^long sidx ^long eidx]
  (let [ne (- eidx sidx)
        rv (byte-array ne)]
    (loop [idx 0]
      (when (< idx ne)
        (ArrayHelpers/aset rv idx (aget data (- eidx idx 1)))
        (recur (inc idx))))
    rv))

(defmethod ^:private preparse-field :decimal
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]
  (let [node (.next node-iter)
        buffers [(.next buf-iter) (.next buf-iter)]
        {:keys [^long precision ^long scale ^long bit-width]} (get field :field-type)
        byte-width (quot (+ bit-width 7) 8)
        col-data-fn
        (fn [n-elems data-buffers]
          (let [data-buf (first data-buffers)
                ^bytes data-ary (if (instance? NativeBuffer data-buf)
                                  (native-buffer/->jvm-array data-buf 0 (dtype/ecount data-buf))
                                  (dtype/->array data-buf))
                ;;biginteger data is always stored big endian I guess...
                ;;https://github.com/apache/arrow-java/blob/main/vector/src/main/java/org/apache/arrow/vector/util/DecimalUtility.java#L53
                array-copy (if (identical? :little-endian (tech.v3.datatype.protocols/platform-endianness))
                             copy-reverse-bytes
                             copy-bytes)]
            (dtype/make-reader :decimal n-elems
                               (let [idx (* idx byte-width)]
                                 (-> (BigInteger. ^bytes (array-copy data-ary idx (+ idx byte-width)))
                                     (BigDecimal. scale))))))]
    (fn parse-decimal
      [decompressor]
      (construct-column sparse? node field (decompressor buffers) col-data-fn))))


(defmethod ^:private preparse-field :default
  [field sparse? ^Iterator node-iter ^Iterator buf-iter dict-map options]  
  (assert (= 0 (count (:children field)))
          (format "Field %s cannot be parsed with default parser" field))
  (let [field-dtype (get-in field [:field-type :datatype])
        field-dtype (if-not (:integer-datetime-types? options)
                      (case field-dtype
                        :time-microseconds :packed-local-time
                        :epoch-microseconds :packed-instant
                        :epoch-days :packed-local-date
                        field-dtype)
                      field-dtype)
        node (.next node-iter)
        buffers [(.next buf-iter)
                 (.next buf-iter)]
        col-data-fn
        (fn [n-elems data-buffers]
          (let [data-buf (first data-buffers)]
            (-> (set-buffer-datatype data-buf field-dtype)
                (dtype/sub-buffer 0 n-elems))))]
    (fn parse-default-field
      [decompressor]
      (construct-column sparse? node field (decompressor buffers) col-data-fn))))

(defn- iter ^Iterator [^Iterable i] (when i (.iterator i)))


(defn- records->ds
  [schema dict-map record-batch options]
  (let [{:keys [nodes buffers]} record-batch
        node-iter (iter (lznc/map-indexed #(assoc %2 :idx %1) nodes))
        buf-iter (iter buffers)
        fields (:fields schema)
        keyfn (get options :key-fn identity)
        keep-set (if-let [alist (get options :column-allowlist)]
                   (set alist)
                   (set (lznc/map (comp keyfn :name) fields)))
        keep-set (if-let [blist (get options :column-blocklist)]
                   (set/difference keep-set (set blist))
                   keep-set)
        decompressor (if-let [compression (get record-batch :compression)]
                       #(decompress-buffers compression %)
                       identity)]
    (->> (:fields schema)
         (lznc/map-indexed #(let [col-idx (long %1)
                                  sparse? (contains? (get options :sparse-columns) col-idx)
                                  parse-fn (preparse-field %2 sparse? node-iter buf-iter dict-map options)]
                              (when (.contains ^Set keep-set (keyfn (get %2 :name)))
                                parse-fn)))
         (lznc/filter identity)
         (lznc/map #(% decompressor))
         (ds-impl/new-dataset options))))


(defn- bytes->short-array
  ^shorts [^bytes data nrows]
  (let [retval (short-array nrows)]
    (-> (ByteBuffer/wrap data)
        (.order ByteOrder/LITTLE_ENDIAN)
        (.asShortBuffer)
        (.get retval))
    retval))

(defn- bytes->int-array
  ^ints [^bytes data nrows]
  (let [retval (int-array nrows)]
    (-> (ByteBuffer/wrap data)
        (.order ByteOrder/LITTLE_ENDIAN)
        (.asIntBuffer)
        (.get retval))
    retval))

(defn- bytes->long-array
  ^longs [^bytes data nrows]
  (let [retval (long-array nrows)]
    (-> (ByteBuffer/wrap data)
        (.order ByteOrder/LITTLE_ENDIAN)
        (.asLongBuffer)
        (.get retval))
    retval))

(defn- bytes->float-array
  ^floats [^bytes data nrows]
  (let [retval (float-array nrows)]
    (-> (ByteBuffer/wrap data)
        (.order ByteOrder/LITTLE_ENDIAN)
        (.asFloatBuffer)
        (.get retval))
    retval))

(defn- bytes->double-array
  ^doubles [^bytes data nrows]
  (let [retval (double-array nrows)]
    (-> (ByteBuffer/wrap data)
        (.order ByteOrder/LITTLE_ENDIAN)
        (.asDoubleBuffer)
        (.get retval))
    retval))


(defn- stream->nbuf
  ^NativeBuffer [input]
  (if (instance? InputStream input)
    (let [^InputStream input input
          dlist (ArrayList.)
          total (long (loop [data-buf (byte-array 4096)
                             n-read (.read input data-buf (int 0) (int 4096))
                             total 0]
                        (if (> n-read 0)
                          (let [new-dbuf (byte-array 4096)]
                            (.add dlist (dtype/sub-buffer data-buf 0 n-read))
                            (recur new-dbuf (.read input new-dbuf (int 0)
                                                   (int 4096))
                                   (+ total n-read)))
                          total)))
          nbuf (dtype/make-container :native-heap :int8 total)]
      (.close input)
      (dtype/coalesce! nbuf dlist)
      nbuf)
    (do
      (assert (instance? NativeBuffer input))
      input)))


(defn- feather->ds
  [input options]
  (let [nbuf (stream->nbuf input)]
    (let [leng (dtype/ecount nbuf)
          m1 (native-buffer/read-int nbuf)
          _ (when-not (== m1 FeatherTable/MAGIC)
              (throw (Exception. "Initial magic number not found")))
          m2 (native-buffer/read-int nbuf (- leng 4))
          _ (when-not (== m2 FeatherTable/MAGIC)
              (throw (Exception. "End magic number not found")))
          meta-len (native-buffer/read-int nbuf (- leng 8))
          metabytes (dtype/sub-buffer nbuf (- leng 8 meta-len) meta-len)
          ctable (CTable/getRootAsCTable (nio-buffer/as-nio-buffer metabytes))
          ncols (.columnsLength ctable)
          nrows (.numRows ctable)
          version (.version ctable)
          int-dt-types? (get options :integer-datetime-types?)]
      (->> (range ncols)
           (map (fn [cidx]
                  (let [col (.columns ctable cidx)
                        cname (.name col)
                        primitive-ary (.values col)
                        fea-dt (.type primitive-ary)
                        n-missing (.nullCount primitive-ary)
                        usermeta (.userMetadata col)
                        data-off (.offset primitive-ary)
                        data-len (.totalBytes primitive-ary)
                        missing-len (if (== 0 n-missing)
                                      0
                                      (pad (quot (+ nrows 7) 8)))
                        missing
                        (if-not (== 0 n-missing)
                          (validity->missing (dtype/sub-buffer nbuf data-off missing-len)
                                             nrows)
                          (bitmap/->bitmap))
                        buffers-len (- data-len missing-len)
                        data (dtype/sub-buffer nbuf (+ data-off missing-len) buffers-len)
                        as-native (fn ([dtype]
                                       (-> (native-buffer/set-native-datatype data dtype)
                                           (dtype/sub-buffer 0 nrows)))
                                    ([dtype nrows]
                                       (-> (native-buffer/set-native-datatype data dtype)
                                           (dtype/sub-buffer 0 nrows))))
                        buffer
                        (condp = fea-dt
                          Type/BOOL
                          (byte-buffer->bitwise-boolean-buffer data nrows)
                          Type/INT8
                          (as-native :int8)
                          Type/UINT8
                          (as-native :uint8)
                          Type/INT16
                          (as-native :int16)
                          Type/UINT16
                          (as-native :uint16)
                          Type/INT32
                          (as-native :int32)
                          Type/UINT32
                          (as-native :uint32)
                          Type/DATE
                          (if int-dt-types?
                            (as-native :epoch-days)
                            (as-native :packed-local-date))
                          Type/INT64
                          (as-native :int64)
                          Type/UINT64
                          (as-native :uint64)
                          Type/TIMESTAMP
                          (as-native :epoch-milliseconds)
                          Type/TIME
                          (as-native :milliseconds)
                          Type/FLOAT
                          (as-native :float32)
                          Type/DOUBLE
                          (as-native :float64)
                          Type/UTF8
                          (offsets-data->string-reader
                           (as-native :int32 (inc nrows))
                           (dtype/sub-buffer data (pad (* (inc nrows) 4)))
                           nrows)
                          Type/LARGE_UTF8
                          (offsets-data->string-reader
                           (as-native :int64 (inc nrows))
                           (dtype/sub-buffer data (* (inc nrows) 8))
                           data nrows)
                          Type/BINARY
                          (offsets-data->bytedata-reader
                           (as-native :int32 (inc nrows))
                           (dtype/sub-buffer data (pad (* (inc nrows) 4)))
                           nrows)
                          Type/LARGE_BINARY
                          (offsets-data->bytedata-reader
                           (as-native :int64 (inc nrows))
                           (dtype/sub-buffer data (* (inc nrows) 8))
                           data nrows))]
                    #:tech.v3.dataset{:name cname
                                      :missing missing
                                      :data buffer
                                      :force-datatype? true})))
           (ds-impl/new-dataset options)))))

(defn- file-tags
  "Returns {:file-type :input} where input may be the original input
  or a wrapper."
  [input]
  (if (instance? InputStream input)
    (let [^InputStream input (if (.markSupported ^InputStream input)
                               input
                               (BufferedInputStream. input))
          n-bytes 16
          _ (.mark input n-bytes)
          tagbuf (byte-array n-bytes)
          _ (.read input tagbuf 0 n-bytes)
          buf (int-array (quot n-bytes 4))
          _ (-> (ByteBuffer/wrap tagbuf)
                (.order ByteOrder/LITTLE_ENDIAN)
                (.asIntBuffer)
                (.get buf))]
      (.reset input)
      {:file-tags (vec buf)
       :input input})
    ;;else native buffer
    {:file-tags (-> (native-buffer/set-native-datatype input :int32)
                    (dtype/sub-buffer 0 4)
                    (vec))
     :input input}))


(defn- file-type
  [input]
  (let [{:keys [file-tags input]} (file-tags input)]
    {:file-type
     (case (unchecked-int (file-tags 0))
       1330795073 :arrow-file
       826361158 :feather-v1
       -1 :arrow-ipc)
     :input input}))

(comment
  (def files ["test/data/alldtypes.arrow-feather-v1"
              "test/data/alldtypes.arrow-feather-compressed"
              "test/data/alldtypes.arrow-ipc"
              "test/data/alldtypes.arrow-feather"])

  (doseq [file files]
    (with-open [is (io/input-stream file)]
      (println (str "file: " file " - type: " (:file-type (file-type is))))))
  )


(defn- input->messages
  ^Iterable [input options]
  (if (instance? InputStream input)
    (stream-message-iterable input options)
    (message-iterable input)))


(defn- maybe-next
  [^Iterator iter] (when (and iter (.hasNext iter)) (.next iter)))

(deftype NextDatasetIter [schema ^Iterator messages fname
                          ^{:unsynchronized-mutable true
                            :tag long} idx
                          ^Map dict-map
                          options]
  Iterator
  (hasNext [this] (.hasNext messages))
  (next [this]
    (loop [msg (.next messages)]
      (if (identical? :dictionary-batch (get msg :message-type))
        (do 
          (.compute dict-map (get msg :id)
                    (hamf-fn/bi-function
                     k old-val
                     (delay
                       (let [new-val (dictionary->strings msg)]
                         (if (and old-val (new-val :delta?))
                           (let [old-strs (get @old-val :strings)
                                 new-strs (get new-val :strings)
                                 new-ec (+ (count new-strs) (count old-strs))
                                 rv (hamf/wrap-array-growable
                                     (make-array String new-ec)
                                     0)]
                             (.addAllReducible rv old-strs)
                             (.addAllReducible rv new-strs)
                             (assoc new-val :strings rv))
                           new-val)))))
          (recur (maybe-next messages)))
        (let [cur-idx idx]
          (set! idx (inc cur-idx))
          (-> (records->ds schema dict-map msg options)
              (ds-base/set-dataset-name (format "%s-%03d" fname cur-idx))))))))

(defn- discard
  [input n-bytes]
  (if (instance? InputStream input)
    (do
      (.read ^InputStream input (byte-array n-bytes) 0 n-bytes)
      input)
    (dtype/sub-buffer input 8)))

(defn- next-dataset-iter
  ^Iterator [input fname options]
  (let [messages (->> (input->messages input options)
                      (lznc/map parse-message)
                      (iter))
        schema (maybe-next messages)]
    (when-not (= :schema (get schema :message-type))
      (when (and (instance? InputStream input)
                 (get options :close-input-stream? true))
        (.close ^InputStream input))
      (throw (Exception. "Initial message is not a schema message.")))
    (NextDatasetIter. schema messages fname 0 (hamf/java-hashmap)
                      (assoc options :sparse-columns (:sparse-columns schema)))))

(defn stream->dataset-iterable
  "Loads data up to and including the first data record.  Returns the a lazy
  sequence of datasets.  Datasets can be loaded using mmapped data and when that is true
  realizing the entire sequence is usually safe, even for datasets that are larger than
  available RAM.
  The default resourc management pathway for this is :auto but you can override this
  by explicity setting the option `:resource-type`.  See documentation for
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
  will return integers as opposed to `java.time.LocalDate`s.

  * `:text-as-strings?` - Return strings instead of Text objects.  This breaks automatic round-tripping
  as it changes datatypes *but* can be useful when used with `:strings-as-text?` when writing data out.
  When used like this uncompressed mmap pathways typically have the highest performance - roughly 100x
  any other method."
  ^Iterable [fname & [options]]
  (reify Iterable
    (iterator [this]
      (let [input (case (get options :open-type :input-stream)
                    :mmap (mmap/mmap-file fname options)
                    :input-stream (apply io/input-stream fname (apply concat (seq options))))
            {:keys [file-type input]} (file-type input)]
        (case file-type
          :arrow-file
          (next-dataset-iter (discard input 8) fname options)
          :arrow-ipc
          (next-dataset-iter input fname options)
          :feather-v1
          (iter (hamf/vector (feather->ds input options))))))))

(defn ^:no-doc stream->dataset-seq
  "see docs for [[stream->dataset-iterable]]"
  [fname & [options]]
  (seq (stream->dataset-iterable fname options)))


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

  * `:integer-datetime-types?` - when true arrow columns in the appropriate packed
  datatypes will be represented as their integer types as opposed to their respective
  packed types.  For example columns of type `:epoch-days` will be returned to the user
  as datatype `:epoch-days` as opposed to `:packed-local-date`.  This means reading values
  will return integers as opposed to `java.time.LocalDate`s."
  ([fname options]
   (let [ds-iter (iter (stream->dataset-iterable fname options))]
     (when-let [ds (maybe-next ds-iter)]
       (when (.hasNext ds-iter)
         (throw (Exception. "File contains multiple record batches.
Please use stream->dataset-seq.")))
       (vary-meta ds assoc :name fname))))
  ([fname]
   (stream->dataset fname nil)))


(defn- simplify-datatype
  [datatype options]
  (let [datatype (packing/unpack-datatype datatype)]
    (if (and (= :string datatype) (get options :strings-as-text?))
      :text
      datatype)))

(defn decimal-column-metadata
  [col]
  (let [[scale precision bit-width]
        (reduce (fn [[scale precision bit-width :as acc] ^BigDecimal dec]
                  (if dec 
                    (let [ss (.scale dec)
                          pp (.precision dec)
                          bw (inc (.bitLength (.unscaledValue dec)))]
                      (when-not (nil? scale)
                        (when-not (== (long scale) ss)
                          (throw (RuntimeException. (str "column \"" (:name (meta col)) "\" has different scale than previous bigdecs
\texpected " scale " and got " ss)))))
                      ;;smallest arrow java supports is 128 bit width
                      [ss 
                       (max pp (long (or precision 1)))
                       (max (long (or bit-width 128)) bw)])
                    acc))
                [nil 1 16]
                col)        
        ;;java.lang.IllegalArgumentException: Library only supports 128-bit and 256-bit decimal values
        ;;ArrowType.java: 1713  org.apache.arrow.vector.types.pojo.ArrowType/getTypeForField
        ;;Field.java:  124  org.apache.arrow.vector.types.pojo.Field/convertField
        ;;Schema.java:  116  org.apache.arrow.vector.types.pojo.Schema/convertSchema
        ;;MessageSerializer.java:  192  org.apache.arrow.vector.ipc.message.MessageSerializer/deserializeSchema
        bit-width (-> (max 128 (ham_fisted.IntegerOps/nextPow2 (long bit-width)))
                      (min 256))]
    {:scale scale
     :bit-width bit-width
     :precision precision
     :datatype :decimal}))


(defn ^:no-doc prepare-dataset-for-write
  "Normalize schemas and convert datatypes to datatypes appropriate for arrow
  serialization."
  ;;prev ds was the dataset immediately previous to this one.  We can assume its
  ;;schema is normalized already.
  [ds prev-ds ds-schema options]
  (let [ds (ds-base/select-columns ds (map :name ds-schema))
        ds-meta (map meta (ds-base/columns ds))
        ;;Step one is normalize our schema
        ;;meaning upcast types when possible or error out.
        ds (if prev-ds
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
             ds)]
    ;;Second step, once the datatypes are set is to cast the types to arrow compatible
    ;;datatypes
    (reduce
     (fn [ds col]
       (let [col-dt (dtype/elemwise-datatype col)]
         (cond
           (and (identical? :string col-dt)
                (not (:strings-as-text? options)))
           (let [missing (ds-proto/missing col)
                 metadata (meta col)
                 str-table-column-data
                 (fn [prev-ds data] 
                   (if (nil? prev-ds)
                     (str-table/->string-table data)
                     (let [prev-col (ds-base/column prev-ds (:name metadata))
                           ^StringTable prev-str-t (ds-proto/column-data prev-col)
                           container (str-table/fast-string-container (HashMap. ^Map (.str->int prev-str-t))
                                                                      (ArrayList. ^List (.int->str prev-str-t)))]
                       (str-table/string-table-from-strings container data))))]
             (assoc ds (metadata :name) (->> (ds-proto/column-data col)
                                             (str-table-column-data prev-ds)
                                             (ds-proto/with-column-data col))))
           ;;detect precision, scale and whether we need 128 or 256 bytes of accuracy
           (identical? :decimal col-dt)
           (assoc ds (ds-col/column-name col)
                  (vary-meta col assoc ::complex-datatype (decimal-column-metadata col)))
           :else
           ds)))
     ds
     (ds-base/columns ds))))


(deftype PrepDsIter [^:unsynchronized-mutable prev-ds
                     ^:unsynchronized-mutable ds-schema
                     ^Iterator ds-seq
                     options]
  Iterator
  (hasNext [this] (.hasNext ds-seq))
  (next [this] (let [nds (.next ds-seq)
                     pds prev-ds
                     dsc (if ds-schema
                           ds-schema
                           (do
                             (set! ds-schema (mapv meta (vals nds)))
                             ds-schema))
                     prepared (prepare-dataset-for-write nds pds ds-schema options)]
                 (set! prev-ds prepared)
                 prepared)))


(defn- prepare-ds-seq-for-write
  ^Iterator [options ds-seq]
  (PrepDsIter. nil nil (.iterator ^Iterable (hamf-proto/->iterable ds-seq)) options))

(defn ^:no-doc ds->schema
  "Return a map of :schema, :dictionaries where :schema is an Arrow schema
  and dictionaries is a list of {:byte-data :offsets}.  Dataset must be prepared first
  - [[prepare-dataset-for-write]] to ensure that columns have the appropriate datatypes."
  ([ds options]
   (let [dictionaries (ArrayList.)
         col-vals (.values ^Map ds)
         sparse-columns (get options :sparse-columns)]
     {:schema
      (Schema. ^Iterable
               (->> col-vals
                    (map (partial col->field dictionaries options)))
               (if-not (empty? sparse-columns)
                 {"sparse-columns" (String/join "," ^"[Ljava.lang.String;" (into-array String (map str sparse-columns)))}
                 {}))
      :dictionaries dictionaries}))
  ([ds]
   (ds->schema ds {})))


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


(def ^{:tag 'bytes
       :private true} arrow-file-begin-tag (byte-array [65 82 82 79 87 49 0 0]))
(def ^{:tag 'bytes
       :private true} arrow-file-end-tag (byte-array [65 82 82 79 87 49]))


(defn- write-footer
  [^WriteChannel writer ^Schema schema dict-blocks record-blocks]
  ;;IPC continuation
  (.writeIntLittleEndian writer -1)
  ;;Empty integer for padding
  (.writeIntLittleEndian writer 0)
  (let [builder (FlatBufferBuilder.)
        schema-offset (.getSchema schema builder)
        _ (Footer/startDictionariesVector builder (count dict-blocks))
        _ (doseq [dict (reverse dict-blocks)]
            (write-block-data builder dict))
        dict-offset (.endVector builder)
        _ (Footer/startRecordBatchesVector builder (count record-blocks))
        _ (doseq [rb (reverse record-blocks)]
            (write-block-data builder rb))
        rb-offset (.endVector builder)
        _ (.finish builder (Footer/createFooter builder 4 schema-offset dict-offset rb-offset 0))
        footer-data (.dataBuffer builder)
        start (.getCurrentPosition writer)
        _ (.write writer footer-data)
        end (.getCurrentPosition writer)
        metalen (- end start)]
    (.writeIntLittleEndian writer metalen)
    (.write writer (ByteBuffer/wrap arrow-file-end-tag))))


(defn dataset-seq->stream!
  "Write a sequence of datasets as an arrow stream file.  File will contain one record set
  per dataset.  Datasets in the sequence must have matching schemas or downstream schema
  must be able to be safely widened to the first schema.

  Options:

  * `:strings-as-text?` - defaults to true - Save out strings into arrow files without
     dictionaries.  This works well if you want to load an arrow file in-place or if
     you know the strings in your dataset are either really large or should not be in
     string tables.  **Saving multiple datasets with `{:strings-as-text false}` requires arrow
     7.0.0+ support from your python or R code due to
     [Arrow issue 13467](https://issues.apache.org/jira/browse/ARROW-13467).  - the conservative
     pathway for now is to set `:strings-as-text?` to true and only save text!!**.

  * `:format` - one of `[:file :ipc]`,  defaults to `:file`.
    - `:file` - arrow file format, compatible with pyarrow's [open_file](https://arrow.apache.org/docs/python/generated/pyarrow.ipc.open_file.html#pyarrow.ipc.open_file).  The suggested
       suffix is `.arrow`.
    - `:ipc` - arrow streaming format, compatible with pyarrow's [open_ipc](https://arrow.apache.org/docs/python/generated/pyarrow.ipc.open_file.html#pyarrow.ipc.open_ipc) pathway.  The
       suggested suffix is `.arrows`.

  * `:compression` - Either `:zstd` or `:lz4`,  defaults to no compression (nil).
  Per-column compression of the data can result in some significant size savings
  (2x+) and thus some significant time savings when loading over the network.
  Using compression makes loading via mmap non-lazy - If you are going to use
  compression mmap probably doesn't make sense and most likely will result in
  slower loading times.
     - `:lz4` - Decent and very fast compression.
     - `:zstd` - Good compression, somewhat slower than `:lz4`.  Can also have a
       level parameter that ranges from 1-12 in which case compression is specified
       in map form: `{:compression-type :zstd :level 5}`."
  ([path options ds-seq]
   ;;We use the first dataset to setup schema information the rest of the datasets
   ;;must follow.  So the serialization of the first dataset differs from the serialization
   ;;of the subsequent datasets
   (when (empty? ds-seq)
     (throw (Exception. "Empty dataset sequence")))
   (let [options (-> options
                     (update :compression validate-compression-for-write)
                     (update :strings-as-text? (fn [val]
                                                 (if (nil? val)
                                                   true
                                                   val))))
         ds-seq-iter (prepare-ds-seq-for-write options ds-seq)
         ds (when (.hasNext ds-seq-iter)
              (.next ds-seq-iter))
         file-tag? (= :file (get options :format :file))
         dict-blocks (ArrayList.)
         record-blocks (ArrayList.)
         sparse-columns (if-let [sparse-col (get options :sparse-columns)]
                          (into #{} sparse-col)
                          #{})
         colvec (.values ^Map ds)
         sparse-columns (->> (range (ds-base/column-count ds))
                             (filter #(sparse-col/is-sparse? (colvec %)))
                             (reduce conj sparse-columns))
         options (assoc options :sparse-columns sparse-columns)]
     ;;Any native mem allocated in this context will be released
     (with-open [ostream (io/output-stream! path)]
       (let [writer (arrow-output-stream-writer ostream)
             _ (when file-tag?
                 (.write writer (ByteBuffer/wrap arrow-file-begin-tag)))
             {:keys [schema dictionaries]} (ds->schema ds options)]
         (write-schema writer schema)
         (.addAll dict-blocks (lznc/map #(write-dictionary writer % options) dictionaries))
         (.add record-blocks (resource/stack-resource-context
                              (write-dataset writer ds options)))
         (loop [continue? (.hasNext ds-seq-iter)
                options (assoc options :dictionaries dictionaries)]
           (when continue?
             (let [ds (.next ds-seq-iter)
                   ;;Passing in previous dictionaries so we can chain them for differential
                   ;;dictionary encoding
                   {:keys [dictionaries]} (ds->schema ds (assoc options :dictionaries dictionaries))]
               (.addAll dict-blocks (lznc/map #(write-dictionary writer % options) dictionaries))
               (.add record-blocks (resource/stack-resource-context (write-dataset writer ds options)))
               (recur (.hasNext ds-seq-iter) (assoc options :dictionaries dictionaries)))))

         (when file-tag?
           (write-footer writer schema dict-blocks record-blocks))
         (.getCurrentPosition writer)))))
  ([path ds-seq]
   (dataset-seq->stream! path nil ds-seq)))


(defn dataset->stream!
  "Write a dataset as an arrow file.  File will contain one record set.
  See documentation for [[dataset-seq->stream!]].

  * `:strings-as-text?` defaults to false."
  ([ds path options]
   (dataset-seq->stream! path (update options :strings-as-text?
                                      (fn [val]
                                        (if (nil? val)
                                          false
                                          val))) [ds]))
  ([ds path]
   (dataset->stream! ds path {})))


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


(defmethod ds-io/data->dataset :arrow
  [data options]
  (stream->dataset data options))


(defmethod ds-io/dataset->data! :arrow
  [ds output options]
  (dataset->stream! ds output (update options :format
                                      (fn [item]
                                        (if (nil? item)
                                          :file
                                          item)))))


(defmethod ds-io/data->dataset :arrows
  [data options]
  (stream->dataset data options))


(defmethod ds-io/dataset->data! :arrows
  [ds output options]
  (dataset->stream! ds output (update options :format
                                      (fn [item]
                                        (if (nil? item)
                                          :ipc
                                          item)))))
