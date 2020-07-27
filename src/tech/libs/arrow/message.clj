(ns tech.libs.arrow.message
  (:require [tech.libs.arrow.mmap :as mmap]
            [tech.libs.arrow :as arrow]
            [tech.v2.datatype :as dtype]
            [clojure.core.protocols :as clj-proto]
            [clojure.datafy :refer [datafy]])
  (:import [tech.libs.arrow.mmap NativeBuffer]
           [org.apache.arrow.vector.ipc.message MessageSerializer
            MessageMetadataResult]
           [org.apache.arrow.flatbuf Message MessageHeader DictionaryBatch RecordBatch]
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.vector.types.pojo FieldType ArrowType Field Schema
            ArrowType$Int ArrowType$FloatingPoint ArrowType$Bool
            ArrowType$Utf8 ArrowType$Date ArrowType$Time ArrowType$Timestamp
            ArrowType$Duration DictionaryEncoding]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


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
  [^NativeBuffer data]
  (let [msg-size (mmap/read-int data)
        [msg-size offset] (if (== -1 msg-size)
                            [(mmap/read-int data 4) 8]
                            [msg-size 4])
        offset (long offset)
        msg-size (long msg-size)]
    (when (> msg-size 0)
      (let [new-msg (Message/getRootAsMessage (-> (dtype/sub-buffer data offset msg-size)
                                              (dtype/->buffer-backing-store)))
            next-buf (dtype/sub-buffer data (+ offset msg-size))
            body-length (.bodyLength new-msg)
            aligned-offset (align-offset (+ offset msg-size body-length))]
        (merge
         {:data (dtype/sub-buffer data aligned-offset)
          :message new-msg
          :message-type (message-id->message-type (.headerType new-msg))}
         (when-not (== 0 body-length)
           {:body (dtype/sub-buffer next-buf 0 (.bodyLength new-msg))}))))))


(defn message-seq
  [^NativeBuffer data]
  (when-let [msg (read-message data)]
    (cons msg (lazy-seq (message-seq (:data msg))))))


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
     :encoding :utf-8})
  ArrowType$FloatingPoint
  (datafy [this]
    {:datatype (condp = (.getPrecision this)
                 FloatingPointPrecision/HALF :float16
                 FloatingPointPrecision/SINGLE :float32
                 FloatingPointPrecision/DOUBLE :float64)})
  ArrowType$Timestamp
  (datafy [this]
    (merge
     (if (= (.getUnit this) TimeUnit/MILLISECOND)
       {:datatype :epoch-milliseconds}
       {:datatype :int64 :time-unit (condp = (.getUnit this)
                                      TimeUnit/MICROSECOND :microsecond
                                      TimeUnit/NANOSECOND :nanosecond
                                      TimeUnit/SECOND :second)})
     (when (and (.getTimezone this)
                (not= 0 (count (.getTimezone this))))
       {:timezone (.getTimezone this)})))
  DictionaryEncoding
  (datafy [this] {:id (.getId this)
                  :ordered? (.isOrdered this)
                  :index-type (datafy (.getIndexType this))}))


(defn read-schema
  "returns a pair of offset-data and schema"
  [{:keys [message body message-type]}]
  (let [schema (MessageSerializer/deserializeSchema ^Message message)]
    {:fields (->> (.getFields schema)
                  (mapv (fn [^Field field]
                          (merge
                           {:name (.getName field)
                            :nullable? (.isNullable field)
                            :field-type (datafy (.getType (.getFieldType field)))
                            :metadata (.getMetadata field)}
                           (when-let [encoding (.getDictionary field)]
                             {:dictionary-encoding (datafy encoding)})))))
     :metadata (.getCustomMetadata schema)}))


(defn read-record-batch
  ([^RecordBatch record-batch ^NativeBuffer data]
   {:nodes (->> (range (.nodesLength record-batch))
                (mapv #(let [node (.nodes record-batch (int %))]
                         {:n-elems (.length node)
                          :n-null-entries (.nullCount node)})))
    :buffers (->> (range (.buffersLength record-batch))
                  (mapv #(let [buffer (.buffers record-batch (int %))]
                           (dtype/sub-buffer data (.offset buffer) (.length buffer)))))})
  ([{:keys [message body message-type] :as msg}]
   (read-record-batch (.header ^Message message (RecordBatch.)) body)))


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
     :isDelta (.isDelta db)
     :records (read-record-batch (.data db) body)}))


(defmulti parse-message :message-type)

(defmethod parse-message :schema
  [msg]
  (read-schema msg))

(defmethod parse-message :dictionary-batch
  [msg]
  (read-dictionary-batch msg))

(defmethod parse-message :record-batch
  [msg]
  (read-record-batch msg))


(comment
  ;;Overriding to use GC memory management.  Default is stack and thus needs to be in
  ;;a call to resource/stack-resource-context.
  (def fdata (mmap/read-only-mmap-file "big-stocks.feather" {:resource-type :gc}))
  (def messages (vec (message-seq fdata)))
  (assert (= 3 (count messages)))
  (assert (= [:schema :dictionary-batch :record-batch] (mapv :message-type messages)))
  (def schema (nth messages 0))
  (def dict (nth messages 1))
  (def record (nth messages 2))
  (mapv parse-message messages)
  )
