(ns tech.v3.libs.arrow.in-place
  (:require [tech.v3.datatype.mmap :as mmap]
            [tech.v3.libs.arrow.datatype :as arrow-dtype]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [tech.v3.dataset.base :as ds-base]
            [tech.resource :as resource]
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
  [dict-map encoding buffers n-elems]
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
      (offsets-data->string-reader (native-buffer/set-native-datatype offsets :int32)
                                   varchar-data n-elems))))


(defn records->ds
  [schema dict-map record-batch]
  (let [{:keys [fields]} schema
        {:keys [nodes buffers]} record-batch]
    (assert (= (count fields) (count nodes)))
    (->> (map vector fields nodes)
         (reduce (fn [[retval ^long buf-idx] [field node]]
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
                             (case field-dtype
                               :string (string-data->column-data
                                        dict-map encoding (drop 1 specific-bufs)
                                        n-elems)
                               :boolean
                               (arrow-dtype/byte-buffer->bitwise-boolean-buffer
                                (second specific-bufs) n-elems)
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
  [fdata schema messages fname idx]
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
       (-> (records->ds schema dict-map data-record)
           (ds-base/set-dataset-name (format "%s-%03d" fname idx))
           ;;Assoc on the file data so the gc doesn't release the source memory map
           ;;until no one is accessing the dataset any more in the case where
           ;;the file was opened with {:resource-type :gc}
           (resource/track (constantly fdata)))
       (lazy-seq (parse-next-dataset fdata schema (rest rest-messages)
                                     fname (inc (long idx))))))))


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
    (parse-next-dataset fdata schema messages fname 0)))


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
    (-> (records->ds schema dict-map data-record)
        (ds-base/set-dataset-name fname)
        ;;Ensure the file data is linked via gc to the dataset.
        (resource/track (constantly fdata)))))


(comment
  ;;Overriding to use GC memory management.  Default is stack and thus needs to be in
  ;;a call to resource/stack-resource-context.
  (def fdata (mmap/mmap-file "test.arrow" {:resource-type :gc}))
  (def messages (mapv parse-message (message-seq fdata)))
  (assert (= 3 (count messages)))
  (assert (= [:schema :dictionary-batch :record-batch] (mapv :message-type messages)))
  (def schema (nth messages 0))
  (def dict (nth messages 1))
  (def record (nth messages 2))
  (def dict-data (dictionary->strings dict))
  (def dict-map {(:id dict-data) dict-data})
  (def inplace-ds (records->ds schema dict-map record))

  (require '[criterium.core :as crit])
  (require '[tech.v3.datatype.functional :as dfn])
  (require '[tech.resource :as resource])
  (require '[tech.parallel.for :as parallel-for])
  (require '[tech.v3.datatype.typecast :as typecast])

  (defn fast-reduce-plus
    ^double [rdr]
    (let [rdr (typecast/datatype->reader :float64 rdr)]
      (parallel-for/indexed-map-reduce
       (.lsize rdr)
       (fn [^long start-idx ^long group-len]
         (let [end-idx (long (+ start-idx group-len))]
           (loop [idx (long start-idx)
                  result 0.0]
             (if (< idx end-idx)
               (recur (unchecked-inc idx)
                      (+ result (.read rdr idx)))
               result))))
       (partial reduce +))))

  (defn sum-test-1
    []
    (resource/stack-resource-context
     (let [ds (read-dataset-inplace "big-stocks.feather")]
       (fast-reduce-plus (ds "price")))))
  ;; Evaluation count : 96 in 6 samples of 16 calls.
  ;;            Execution time mean : 7.955908 ms
  ;;   Execution time std-deviation : 1.019668 ms
  ;;  Execution time lower quantile : 6.706196 ms ( 2.5%)
  ;;  Execution time upper quantile : 9.318124 ms (97.5%)
  ;;                  Overhead used : 2.377233 ns


  (require '[tech.io :as io])
  (defn sum-test-2
    []
    (let [ds (io/get-nippy "big-stocks.nippy")]
      (dfn/reduce-+ (ds "price"))))

  ;; Evaluation count : 12 in 6 samples of 2 calls.
  ;;            Execution time mean : 86.257718 ms
  ;;   Execution time std-deviation : 681.391723 µs
  ;;  Execution time lower quantile : 85.376117 ms ( 2.5%)
  ;;  Execution time upper quantile : 86.987293 ms (97.5%)
  ;;                  Overhead used : 2.635202 ns

  (import '[org.apache.arrow.vector.ipc ArrowStreamReader ArrowStreamWriter ArrowFileReader])
  (defn sum-test-3
    []
    (with-open [istream (io/input-stream "big-stocks.feather")
                reader (ArrowStreamReader. istream (arrow/allocator))]
      (.loadNextBatch reader)
      (fast-reduce-plus (.get (.getFieldVectors (.getVectorSchemaRoot reader))
                              2))))
  ;; Evaluation count : 6 in 6 samples of 1 calls.
  ;;            Execution time mean : 104.142056 ms
  ;;   Execution time std-deviation : 810.545821 µs
  ;;  Execution time lower quantile : 103.241336 ms ( 2.5%)
  ;;  Execution time upper quantile : 105.123550 ms (97.5%)
  ;;                  Overhead used : 2.377233 ns

    (defn mean-test-4
    []
    (with-open [istream (io/input-stream "big-stocks.feather")
                reader (ArrowStreamReader. istream (arrow/allocator))]
      (.loadNextBatch reader)
      (let [^Float8Vector vecdata (.get
                                   (.getFieldVectors (.getVectorSchemaRoot reader))
                                   2)
            n-elems (dtype/ecount vecdata)]
        (dfn/mean (dtype/make-reader :float64 (dtype/ecount vecdata)
                                     (.get vecdata idx))))))
   ;;  Evaluation count : 6 in 6 samples of 1 calls.
   ;;           Execution time mean : 170.486968 ms
   ;;  Execution time std-deviation : 1.872698 ms
   ;; Execution time lower quantile : 168.327549 ms ( 2.5%)
   ;; Execution time upper quantile : 172.799712 ms (97.5%)
    ;;                 Overhead used : 2.635202 ns


    (defn inplace-load
      []
      (resource/stack-resource-context
       (read-dataset-inplace "big-stocks.feather")
       :ok))
   ;;  Evaluation count : 4566 in 6 samples of 761 calls.
   ;;           Execution time mean : 133.148490 µs
   ;;  Execution time std-deviation : 1.476486 µs
   ;; Execution time lower quantile : 131.670752 µs ( 2.5%)
   ;; Execution time upper quantile : 134.852840 µs (97.5%)
   ;;                 Overhead used : 2.635202 ns

    (defn nippy-load
      []
      (io/get-nippy "big-stocks.nippy"))
;;     Evaluation count : 12 in 6 samples of 2 calls.
;;              Execution time mean : 62.058715 ms
;;     Execution time std-deviation : 3.312573 ms
;;    Execution time lower quantile : 58.691642 ms ( 2.5%)
;;    Execution time upper quantile : 66.867313 ms (97.5%)
;;                    Overhead used : 2.635202 ns

;; Found 1 outliers in 6 samples (16.6667 %)
;; 	low-severe	 1 (16.6667 %)
;;  Variance from outliers : 14.1278 % Variance is moderately inflated by outliers

    (defn arrow-stream-load
      []
      (with-open [istream (io/input-stream "big-stocks.feather")
                  reader (ArrowStreamReader. istream (arrow/allocator))]
        (.loadNextBatch reader)))

   ;;  Evaluation count : 6 in 6 samples of 1 calls.
   ;;           Execution time mean : 104.879396 ms
   ;;  Execution time std-deviation : 1.227596 ms
   ;; Execution time lower quantile : 103.101944 ms ( 2.5%)
   ;; Execution time upper quantile : 106.109886 ms (97.5%)
   ;;                 Overhead used : 2.635202 ns


    ;;Fails to load the file...
    (defn arrow-file-load
      []
      (with-open [istream (java.io.RandomAccessFile. "big-stocks.file.feather" "r")
                  reader (ArrowFileReader. (.getChannel istream) (arrow/allocator))]
        (.loadNextBatch reader)))


    )
