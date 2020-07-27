(ns tech.libs.arrow.message
  (:require [tech.libs.arrow.mmap :as mmap]
            [tech.v2.datatype :as dtype])
  (:import [tech.libs.arrow.mmap NativeBuffer]
           [org.apache.arrow.vector.ipc.message MessageSerializer
            MessageMetadataResult]
           [org.apache.arrow.flatbuf Message MessageHeader]
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
          :head-type (.headerType new-msg)}
         (when-not (== 0 body-length)
           {:body (dtype/sub-buffer next-buf 0 (.bodyLength new-msg))}))))))


(defn message-seq
  [^NativeBuffer data]
  (when-let [msg (read-message data)]
    (cons msg (lazy-seq (message-seq (:data msg))))))


(defn read-schema
  "returns a pair of offset-data and schema"
  [data]
  (let [[rest-data ^Message msg] (read-message data)]
    (when-not (== MessageHeader/Schema (.headerType msg))
      (throw (Exception. "Schema information missing")))
    [rest-data (MessageSerializer/deserializeSchema msg)]))
