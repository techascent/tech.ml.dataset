(ns tech.v3.libs.arrow.in-place
  (:require [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.libs.arrow.datatype :as arrow-dtype]
            ;;Protocol definitions that make datafy work
            [tech.v3.libs.arrow.schema]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [tech.v3.dataset.base :as ds-base]
            [clojure.datafy :refer [datafy]])
  (:import [org.apache.arrow.vector.ipc.message MessageSerializer
            MessageMetadataResult]
           [org.apache.arrow.vector TypeLayout Float8Vector]
           [org.apache.arrow.flatbuf Message MessageHeader DictionaryBatch RecordBatch]
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.vector.types.pojo FieldType ArrowType Field Schema
            ArrowType$Int ArrowType$FloatingPoint ArrowType$Bool
            ArrowType$Utf8 ArrowType$Date ArrowType$Time ArrowType$Timestamp
            ArrowType$Duration DictionaryEncoding]
           [tech.v3.dataset.string_table StringTable]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype ObjectReader]
           [java.util ArrayList HashMap List]))


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


(defn read-schema
  "returns a pair of offset-data and schema"
  [{:keys [message body message-type]}]
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
  [{:keys [id delta? records] :as dict}]
  (let [nodes (:nodes records)
        buffers (:buffers records)
        _ (assert (== 1 (count nodes)))
        _ (assert (== 3 (count buffers)))
        node (first nodes)
        [bitwise offsets databuf] buffers
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
      (-> (offsets-data->string-reader (native-buffer/set-native-datatype offsets offset-buf-dtype)
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
