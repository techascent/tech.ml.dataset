(ns tech.v3.dataset.file-backed-text
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.mmap-writer :as mmap-writer])
  (:import [java.nio.charset Charset]
           [tech.v3.datatype DataWriter PrimitiveList ObjectReader Buffer]
           [tech.v3.dataset Text]))


(deftype FileBackedTextBuilder [^Charset encoding
                                ^DataWriter writer
                                ^PrimitiveList offsets
                                cached-io]
  PrimitiveList
  (lsize [_this] (.lsize offsets))
  (elemwiseDatatype [_this] :text)
  (ensureCapacity [_this _amt])
  (addBoolean [_this _data] (errors/throw-unimplemented))
  (addLong [_this _data] (errors/throw-unimplemented))
  (addDouble [_this _data] (errors/throw-unimplemented))
  (addObject [_this data]
    (reset! cached-io nil)
    (let [data (str data)]
      (if (empty? data)
        (.addLong offsets (.lsize writer))
        (let [bytes (.getBytes ^String data encoding)]
          (.addLong offsets (.lsize writer))
          (.writeBytes writer bytes)))))
  ObjectReader
  (readObject [_this idx]
    (let [start-offset (.readLong  offsets idx)
          end-offset (if (== idx (dec (.lsize offsets)))
                       (.lsize writer)
                       (.readLong offsets (inc idx)))
          ^Buffer byte-io (swap! cached-io
                                 (fn [old-io]
                                   (if old-io old-io (dtype/->buffer writer))))
          byte-data (-> (dtype/sub-buffer byte-io
                                          start-offset
                                          (- end-offset start-offset))
                        (dtype/->byte-array))
          str-data (String. byte-data encoding)]
      (Text. str-data))))


(defn file-backed-text
  "Create a file-backed text store.  Texsts are written to disk but retrievable
  (as Text objects) when ->reader is called on the return value."
  (^PrimitiveList [{:keys [mmap-file-path]
                    :as options}]
   (let [mmap-writer (if mmap-file-path
                       (mmap-writer/mmap-writer mmap-file-path options)
                       (mmap-writer/temp-mmap-writer options))
         encoding (if-let [encoding (:encoding options)]
                    (cond
                      (string? encoding)
                      (Charset/forName encoding)
                      (instance? Charset encoding)
                      encoding
                      :else
                      (errors/throwf "Unrecognized encoding: %s" encoding))
                    (Charset/forName "UTF-8"))
         offsets (dtype/make-list :int64)
         cached-io (atom nil)]
     ;;Force failure here before we get going if we have to.
     (FileBackedTextBuilder. encoding mmap-writer offsets cached-io)))
  (^PrimitiveList [] (file-backed-text nil)))
