(ns tech.v3.libs.arrow.in-place
  (:require [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.libs.arrow.datatype :as arrow-dtype]
            ;;Protocol definitions that make datafy work
            [tech.v3.libs.arrow.schema :as arrow-schema]
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
            [clojure.datafy :refer [datafy]])
  (:import [org.apache.arrow.vector.ipc.message MessageSerializer]
           [org.apache.arrow.flatbuf Message DictionaryBatch RecordBatch
            FieldNode Buffer]
           [org.roaringbitmap RoaringBitmap]
           [com.google.flatbuffers FlatBufferBuilder]
           [org.apache.arrow.vector.types.pojo Field Schema ArrowType$Int
            ArrowType$Utf8 ArrowType$Timestamp DictionaryEncoding]
           [org.apache.arrow.vector.types MetadataVersion]
           [org.apache.arrow.vector.ipc WriteChannel]
           [tech.v3.dataset.string_table StringTable]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype ObjectReader ArrayHelpers ByteConversions]
           [java.io OutputStream InputStream]
           [java.util List ArrayList]
           [java.time ZoneId]
           [java.nio.channels WritableByteChannel]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(defn create-in-memory-byte-channel
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


(defn arrow-in-memory-writer
  ^WriteChannel []
  (let [storage (create-in-memory-byte-channel)]
    {:channel (WriteChannel. storage)
     :storage storage}))


(defn arrow-output-stream-writer
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


(defn pad
  ^long [^long data]
  (let [padding (rem data 8)]
    (if-not (== 0 padding)
      (+ data (- 8 padding))
      data)))



(defn message-id->message-type
  [^long message-id]
  (case message-id
    1 :schema
    2 :dictionary-batch
    3 :record-batch
    {:unexpected-message-type message-id}))


(defn read-message
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


(defn message-seq
  "Given a native buffer of arrow stream data, produce a sequence of flatbuf messages"
  [^NativeBuffer data]
  (when-let [msg (read-message data)]
    (cons msg (lazy-seq (message-seq (:next-data msg))))))


(defn read-int-LE
  (^long [^InputStream is byte-buf]
   (let [^bytes byte-buf (or byte-buf (byte-array 4))]
     (.read is byte-buf)
     (ByteConversions/intFromBytesLE (aget byte-buf 0) (aget byte-buf 1)
                                     (aget byte-buf 2) (aget byte-buf 3))))
  (^long [^InputStream is]
   (read-int-LE is nil)))


(defn read-stream-message
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

(defn stream-message-seq
  [is]
  (if-let [msg (try (read-stream-message is)
                    (catch Throwable e
                      (.close ^InputStream is)
                      (throw e)))]
    (cons msg (lazy-seq (stream-message-seq is)))
    (do (.close ^InputStream is) nil)))


(defn string-column->dict
  [col]
  (let [str-t (ds-base/ensure-column-string-table col)
        int->str (str-table/int->string str-t)
        indices (dtype-proto/->array-buffer (str-table/indices str-t))
        n-elems (.size int->str)
        bit-width (casting/int-width (dtype/elemwise-datatype indices))
        metadata (meta col)
        colname (:name metadata)
        dict-id (.hashCode ^Object colname)
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


(defn string-col->encoding
  "Given a string column return a map of :dict-id :table-width.  The dictionary
  id is the hashcode of the column mame."
  [^List dictionaries col]
  (let [dict (string-column->dict col)]
    (.add dictionaries dict)
    {:encoding (:encoding dict)}))


(defn col->field
  ^Field [dictionaries {:keys [strings-as-text?]} col]
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
                            (string-col->encoding dictionaries col)))]
    (try
      (arrow-schema/make-field
       (ml-utils/column-safe-name colname)
       (arrow-schema/datatype->field-type2 col-dtype nullable? colmeta extra-data))
      (catch Throwable e
        (throw (Exception. (format "Column %s metadata conversion failure:\n%s"
                                   colname e)
                           e))))))

(defn ->timezone
  (^ZoneId [& [item]]
   (cond
     (instance? ZoneId item)
     item
     (string? item)
     (ZoneId/of ^String item)
     :else
     (dtype-dt/utc-zone-id))))

(defn datetime-cols->epoch
  [ds {:keys [timezone]}]
  (let [timezone (when timezone (->timezone timezone))]
    (reduce
     (fn [ds col]
       (let [col-dt (packing/unpack-datatype (dtype/elemwise-datatype col))]
         (if (and (not= col-dt :local-time)
                  (dtype-dt/datetime-datatype? col-dt))
           (assoc ds
                  (col-proto/column-name col)
                  (col-impl/new-column
                   (col-proto/column-name col)
                   (dtype-dt/datetime->epoch
                    timezone
                    (if (= :local-date (packing/unpack-datatype col-dt))
                      :epoch-days
                      :epoch-milliseconds)
                    col)
                   (assoc (meta col)
                          :timezone (str timezone)
                          :source-datatype (dtype/elemwise-datatype col))
                   (col-proto/missing col)))
           ds)))
     ds
     (ds-base/columns ds))))

(defn prepare-dataset-for-write
  "Convert dataset column datatypes to datatypes appropriate for arrow
  serialization.

  Options:

  * `:strings-as-text` - Serialize strings as text.  This leads to the most efficient
  format for mmap operations."
  ([ds options]
   (cond-> (datetime-cols->epoch ds options)
     (not (:strings-as-text? options))
     (ds-base/ensure-dataset-string-tables)))
  ([ds]
   (prepare-dataset-for-write ds nil)))


(defn ds->schema
  "Return a map of :schema, :dictionaries where :schema is an Arrow schema
  and dictionaries is a list of {:byte-data :offsets}.  Dataset must be prepared first
  - [[prepare-dataset-for-write]] to ensure that columns have the appropriate datatypes."
  ([ds options]
   (let [dictionaries (ArrayList.)]
     {:schema
      (Schema. ^Iterable
               (->> (ds-base/columns ds)
                    (map (partial col->field dictionaries options))))
      :dictionaries dictionaries}))
  ([ds]
   (ds->schema ds {})))


(defn read-schema
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


(defn write-message-header
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


(defn write-schema
  "Writes a schema to a message header."
  [writer schema]
  (write-message-header writer
                        (MessageSerializer/serializeMetadata ^Schema schema)))


(defn read-record-batch
  ([^RecordBatch record-batch ^NativeBuffer data]
   {:nodes (->> (range (.nodesLength record-batch))
                (mapv #(let [node (.nodes record-batch (int %))]
                         {:n-elems (.length node)
                          :n-null-entries (.nullCount node)})))
    :buffers (->> (range (.buffersLength record-batch))
                  (mapv #(let [buffer (.buffers record-batch (int %))]
                           (dtype/sub-buffer data (.offset buffer)
                                             (.length buffer)))))})
  ([{:keys [message body _message-type]}]
   (read-record-batch (.header ^Message message (RecordBatch.)) body)))


(defn write-record-batch-header
  ^long [^FlatBufferBuilder builder n-rows nodes buffers]
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
        buffers-offset (.endVector builder)]
    (RecordBatch/startRecordBatch builder)
    (RecordBatch/addLength builder (long n-rows))
    (RecordBatch/addNodes builder node-offset)
    (RecordBatch/addBuffers builder buffers-offset)
    (RecordBatch/endRecordBatch builder)))


(defn no-missing
  [^long n-elems]
  (let [n-bytes (/ (+ n-elems 7) 8)
        c (dtype/make-container :int8 n-bytes)]
    (dtype/set-constant! c -1)
    c))

(defn byte-length
  ^long [container]
  (* (dtype/ecount container) (casting/numeric-byte-width
                               (dtype/elemwise-datatype container))))


(defn serialize-to-bytes
  [^WriteChannel writer num-data]
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
    (.write writer backing-buf)))


(defn finish-builder
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


(defn write-dictionary
  [^WriteChannel writer {:keys [byte-data offsets encoding]}]
  (let [n-elems (dec (count offsets))
        missing (no-missing n-elems)
        missing-len (pad (count missing))
        enc-id (.getId ^DictionaryEncoding encoding)
        offset-len (pad (byte-length offsets))
        data-len (pad (count byte-data))
        builder (FlatBufferBuilder.)
        rbatch-off (write-record-batch-header
                    builder
                    n-elems
                    [{:n-elems n-elems
                      :n-null-entries 0}]
                    [{:offset 0
                      :length missing-len}
                     {:offset missing-len
                      :length offset-len}
                     {:offset (+ missing-len offset-len)
                      :length (count byte-data)}])]
    (write-message-header writer
                          (finish-builder builder 2 ;;dict message type
                                          (DictionaryBatch/createDictionaryBatch
                                           builder enc-id rbatch-off false)
                                          (+ missing-len offset-len data-len)))
    (serialize-to-bytes writer missing)
    (.align writer)
    (serialize-to-bytes writer offsets)
    (.align writer)
    (serialize-to-bytes writer byte-data)
    (.align writer)))

(defn- toggle-bit
  ^bytes [^bytes data ^long bit]
  (let [idx (quot bit 8)
        bit (rem bit 8)
        existing (unchecked-int (aget data idx))]
    (ArrayHelpers/aset data idx (unchecked-byte (bit-xor existing (bit-shift-left 1 bit)))))
  data)


(defn missing-bytes
  ^bytes [^RoaringBitmap bmp ^bytes all-valid-buf]
  (let [nbuf (-> (dtype/clone all-valid-buf)
                 (dtype/->byte-array))]
    (.forEach bmp (reify org.roaringbitmap.IntConsumer
                    (accept [this data]
                      (toggle-bit nbuf (Integer/toUnsignedLong data)))))
    nbuf))

(defn boolean-bytes
  ^bytes [data]
  (let [rdr (dtype/->reader data)
        len (.lsize rdr)
        retval (byte-array (quot (+ len 7) 8))]
    (dotimes [idx len]
      (when (.readBoolean rdr idx)
        (toggle-bit retval idx)))
    retval))


(defn col->buffers
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
        :boolean [(nio-buffer/as-nio-buffer (boolean-bytes cbuf))]
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


(defn write-dataset
  [^WriteChannel writer dataset options]
  (let [n-rows (ds-base/row-count dataset)
        ;;Length of the byte valid buffer
        valid-len (/ (+ n-rows 7) 8)
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
        data-bufs (mapcat :buffers nodes-buffs-lens)
        buffers (->> data-bufs
                     (reduce (fn [[cur-off buffers] buffer]
                               (let [nlen (pad (byte-length buffer))]
                                 [(+ (long cur-off) nlen)
                                  (conj buffers {:offset cur-off
                                                 :length nlen})]))
                             [0 []])
                     (second))
        builder (FlatBufferBuilder.)]
    (write-message-header writer
                          (finish-builder
                           builder 3 ;;record-batch message type
                           (write-record-batch-header builder n-rows nodes buffers)
                           (apply + (map :length nodes-buffs-lens))))
    (doseq [buffer data-bufs]
      (serialize-to-bytes writer buffer)
      (.align writer))))


(defn- check-message-type
  [expected-type actual-type]
  (when-not (= actual-type expected-type)
    (throw (Exception.
            (format "Expected message type %s, got %s"
                    expected-type actual-type)))))


(defn read-dictionary-batch
  [{:keys [message body message-type]}]
  (check-message-type :dictionary-batch message-type)
  (let [^DictionaryBatch db (.header ^Message message (DictionaryBatch.))]
    {:id (.id db)
     :delta? (.isDelta db)
     :records (read-record-batch (.data db) body)}))


(defmulti parse-message
  "Given a message, parse it just a bit into a more interpretable datastructure."
  :message-type)


(defmethod parse-message :schema
  [msg]
  (assoc (read-schema msg)
         :message-type (:message-type msg)))


(defmethod parse-message :dictionary-batch
  [msg]
  (assoc (read-dictionary-batch msg)
         :message-type (:message-type msg)))


(defmethod parse-message :record-batch
  [msg]
  (assoc (read-record-batch msg)
         :message-type (:message-type msg)))


(defn parse-message-printable
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


(def fixed-type-layout [:validity :data])
(def variable-type-layout [:validity :int32 :int8])
(def large-variable-type-layout [:validity :int64 :int8])


(defn offsets-data->string-reader
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


(defn dictionary->strings
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


(defn string-data->column-data
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
          (arrow-dtype/string-reader->text-reader)))))


(defn records->ds
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
                                  :epoch-milliseconds :packed-instant
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
                            (arrow-dtype/int8-buf->missing
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
                        (arrow-dtype/byte-buffer->bitwise-boolean-buffer
                         (second specific-bufs) n-elems)
                        (and (:epoch->datetime? options)
                             (arrow-dtype/epoch-datatypes field-dtype))
                        (dtype-dt/epoch->datetime
                         (:timezone metadata)
                         (arrow-dtype/default-datetime-datatype field-dtype)
                         (-> (native-buffer/set-native-datatype
                              (second specific-bufs) field-dtype)
                             (dtype/sub-buffer 0 n-elems)))
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
  [schema messages fname idx options]
  (when (seq messages)
    (let [dict-messages (take-while #(= (:message-type %) :dictionary-batch)
                                    messages)
          rest-messages (drop (count dict-messages) messages)
          dict-map (->> dict-messages
                        (map dictionary->strings)
                        (map (juxt :id identity))
                        (into {}))
          data-record (first rest-messages)]
      (cons
       (-> (records->ds schema dict-map data-record options)
           (ds-base/set-dataset-name (format "%s-%03d" fname idx)))
       (lazy-seq (parse-next-dataset schema (rest rest-messages)
                                     fname (inc (long idx)) options))))))


(defn stream->dataset-seq-inplace
  "Loads data up to and including the first data record.  Returns the a lazy
  sequence of datasets.  Datasets use mmapped data, however, so realizing the
  entire sequence is usually safe, even for datasets that are larger than
  available RAM.
  This method is expected to be called from within a stack resource context
  unless options include {:resource-type :gc}.  See documentation for
  tech.v3.datatype.mmap/mmap-file."
  [fname & [options]]
  (let [fdata (mmap/mmap-file fname options)
        messages (mapv parse-message (message-seq fdata))
        schema (first messages)
        _ (when-not (= :schema (:message-type schema))
            (throw (Exception. "Initial message is not a schema message.")))
        messages (rest messages)]
    (parse-next-dataset schema messages fname 0 options)))


(defn message-seq->dataset
  [fname message-seq options]
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


(defn read-stream-dataset-inplace
  "Loads data up to and including the first data record.  Returns the dataset
  and the memory-mapped file.
  This method is expected to be called from within a stack resource context."
  [fname & [options]]
  (let [fdata (mmap/mmap-file fname options)
        messages (message-seq fdata)]
    (message-seq->dataset fname messages options)))


(defn stream->dataset-seq-copying
  [fname & [options]]
  (let [is (io/input-stream fname)
        messages (map #(try (parse-message %)
                            (catch Throwable e
                              (.close ^InputStream is)
                              (throw e)))
                      (stream-message-seq is))
        schema (first messages)
        _ (when-not (= :schema (:message-type schema))
            (throw (Exception. "Initial message is not a schema message.")))
        messages (rest messages)]
    (parse-next-dataset schema messages fname 0 options)))


(defn read-stream-dataset-copying
  [fname & [options]]
  (with-open [is (io/input-stream fname)]
    (let [messages (stream-message-seq is)]
      (message-seq->dataset fname messages options))))


(defn write-dataset!
  "Write a dataset as an arrow stream file.  File will contain one record set.

  Options:

  * `strings-as-text?`: - defaults to false - Save out strings into arrow files without
     dictionaries.  This works well if you want to load an arrow file in-place or if
     you know the strings in your dataset are either really large or should not be in
     string tables."
  ([ds path options]
   (let [ds (prepare-dataset-for-write ds options)
         {:keys [schema dictionaries]} (ds->schema ds options)]
     ;;Any native mem allocated in this context will be released
     (resource/stack-resource-context
      (with-open [ostream (io/output-stream! path)]
        (let [writer (arrow-output-stream-writer ostream)]
          (write-schema writer schema)
          (doseq [dict dictionaries] (write-dictionary writer dict))
          (write-dataset writer ds options))))))
  ([ds path]
   (write-dataset-to-stream! ds path {})))


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
