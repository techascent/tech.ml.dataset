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
            [clojure.datafy :refer [datafy]])
  (:import [org.apache.arrow.vector.ipc.message MessageSerializer]
           [org.apache.arrow.flatbuf Message DictionaryBatch RecordBatch
            FieldNode Buffer]
           [com.google.flatbuffers FlatBufferBuilder]
           [org.apache.arrow.vector.types.pojo Field Schema ArrowType$Int
            ArrowType$Utf8 ArrowType$Timestamp DictionaryEncoding]
           [org.apache.arrow.vector.types MetadataVersion]
           [org.apache.arrow.vector.ipc WriteChannel]
           [tech.v3.dataset.string_table StringTable]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype ObjectReader]
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


(defn align-offset
  ^long [^long off]
  (let [alignment (rem off 8)]
    (if (== 0 alignment)
      off
      (+ off (- 8 alignment)))))


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
              aligned-offset (align-offset (+ offset msg-size body-length))]
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
        offsets (dyn-int-list/dynamic-int-list 0)]
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
       (arrow-schema/datatype->field-type col-dtype nullable? colmeta extra-data))
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

(defn pad
  ^long [^long data]
  (let [padding (rem data 8)]
    (if-not (== 0 padding)
      (+ data (- 8 padding))
      data)))


(defn serialize-to-bytes
  [^WriteChannel writer num-data]
  (let [data-dt (casting/un-alias-datatype (dtype/elemwise-datatype num-data))
        n-bytes (byte-length num-data)
        backing-buf (byte-array n-bytes)
        bbuf (java.nio.ByteBuffer/wrap backing-buf)
        ary-data (dtype/->array (if (= data-dt :boolean)
                                  :int8
                                  data-dt)
                                num-data)]
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
                   (.put ^doubles ary-data)))
    (.write writer backing-buf)))


(defn write-dictionary-batch
  [^WriteChannel writer {:keys [byte-data offsets encoding]}]
  (let [builder (FlatBufferBuilder.)
        n-elems (dec (count offsets))
        missing (no-missing n-elems)
        missing-len (pad (count missing))
        enc-id (.getId ^DictionaryEncoding encoding)
        offset-len (pad (byte-length offsets))
        data-len (pad (count byte-data))
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
                      :length (count byte-data)}])
        dict-off (do
                   (DictionaryBatch/startDictionaryBatch builder)
                   (DictionaryBatch/addId builder enc-id)
                   (DictionaryBatch/addData builder rbatch-off)
                   (DictionaryBatch/addIsDelta builder false)
                   (DictionaryBatch/endDictionaryBatch builder))
        body-len (+ missing-len offset-len data-len)
        version (.toFlatbufID (MetadataVersion/DEFAULT))
        builder-data (do
                       (Message/startMessage builder)
                       (Message/addHeaderType builder 2)
                       (Message/addHeader builder dict-off)
                       (Message/addVersion builder version)
                       (Message/addBodyLength builder body-len)
                       (.finish builder (Message/endMessage builder))
                       (.dataBuffer builder))]

    (write-message-header writer builder-data)
    (serialize-to-bytes writer missing)
    (.align writer)
    (serialize-to-bytes writer offsets)
    (.align writer)
    (serialize-to-bytes writer byte-data)
    (.align writer)))


(defn- check-message-type
  [expected-type actual-type]
  (when-not (= actual-type expected-type)
    (throw (Exception.
            (format "Expected message type %s, got %s"
                    expected-type actual-type) ))))


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
        str-data (dtype/make-container :list :string
                   (offsets-data->string-reader offsets data n-elems))]
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
                  col-metadata (dissoc (:field-type field) :datatype)
                  encoding (get field :dictionary-encoding)
                  n-buffers (long (if (and (= :string field-dtype)
                                           (not encoding))
                                    3
                                    2))
                  specific-bufs (subvec buffers buf-idx
                                        (+ buf-idx n-buffers))
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
                        (dtype-dt/epoch->datetime (:timezone metadata)
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
         (ds-impl/new-dataset))))


(defn- parse-next-dataset
  [fdata schema messages fname idx options]
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
       (lazy-seq (parse-next-dataset fdata schema (rest rest-messages)
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
    (parse-next-dataset fdata schema messages fname 0 options)))


(defn read-stream-dataset-inplace
  "Loads data up to and including the first data record.  Returns the dataset
  and the memory-mapped file.
  This method is expected to be called from within a stack resource context."
  [fname & [options]]
  (let [fdata (mmap/mmap-file fname options)
        messages (mapv parse-message (message-seq fdata))
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
  )
