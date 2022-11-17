(ns tech.v3.dataset.file-backed-text
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.mmap-writer :as mmap-writer]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [java.nio.charset Charset]
           [ham_fisted IMutList ChunkedList]
           [tech.v3.datatype DataWriter Buffer]
           [tech.v3.dataset Text]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defn- read-text-object
  [^IMutList offsets ^Buffer byte-io ^Charset encoding ^long idx]
    (let [start-offset (.getLong offsets idx)
          end-offset (if (== idx (unchecked-dec (.size offsets)))
                       (.lsize byte-io)
                       (.getLong offsets (unchecked-inc idx)))
          byte-data (-> (dtype/sub-buffer byte-io
                                          start-offset
                                          (- end-offset start-offset))
                        (dtype/->byte-array))
          str-data (String. byte-data encoding)]
      (Text. str-data)))


(defn- reduce-text-builder
  [^IMutList offsets ^Buffer byte-io ^Charset encoding
   rfn acc sidx eidx]
  (let [sidx (long sidx)
        eidx (long eidx)]
    (loop [sidx sidx
           acc acc]
      (if (and (< sidx eidx) (not (reduced? acc)))
        (recur (unchecked-inc sidx)
               (rfn acc (read-text-object offsets byte-io encoding sidx)))
        acc))))


(deftype FileBackedTextBuilder [^Charset encoding
                                ^DataWriter writer
                                ^IMutList offsets
                                cached-io]
  dtype-proto/PClone
  (clone [this]
    (->> (dtype/make-container :text (.lsize this))
         (dtype/copy! this)))
  Buffer
  (lsize [_this] (.size offsets))
  (elemwiseDatatype [_this] :text)
  (subBuffer [this sidx eidx]
    (ChunkedList/sublistCheck sidx eidx (.size offsets))
    (if (and (== 0 sidx) (== eidx (.size offsets)))
      this
      (let [^Buffer byte-io (swap! cached-io
                                   (fn [old-io]
                                     (if old-io old-io (dtype/->buffer writer))))
            ^IMutList offsets offsets
            ^Charset encoding encoding
            ne (- eidx sidx)]
        (if (== ne 0)
          (reify Buffer (elemwiseDatatype [rdr] 0) (lsize [rdr] 0))
          (reify Buffer
            (elemwiseDatatype [rdr] :text)
            (lsize [rdr] ne)
            (subBuffer [rdr ssidx seidx]
              (ChunkedList/sublistCheck ssidx seidx ne)
              (.subBuffer this (+ sidx ssidx) (+ sidx seidx)))
            (readObject [rdr idx]
              (read-text-object offsets
                                byte-io
                                encoding
                                (+ sidx idx)))
            (reduce [rdr rfn init]
              (reduce-text-builder offsets byte-io encoding rfn init sidx eidx)))))))
  (add [_this data]
    (reset! cached-io nil)
    (let [data (str data)]
      (if (empty? data)
        (.addLong offsets (.lsize writer))
        (let [bytes (.getBytes ^String data encoding)]
          (.addLong offsets (.lsize writer))
          (.writeBytes writer bytes))))
    true)
  (readObject [_this idx]
    (read-text-object offsets
                      (swap! cached-io
                             (fn [old-io]
                               (if old-io old-io (dtype/->buffer writer))))
                      encoding
                      idx))
  (reduce [this rfn init]
    (reduce-text-builder offsets (swap! cached-io
                                        (fn [old-io]
                                          (if old-io old-io (dtype/->buffer writer))))
                         encoding rfn init 0 (.size offsets))))


(defn file-backed-text
  "Create a file-backed text store.  Texts are written to disk but retrievable
  (as Text objects) when ->reader is called on the return value."
  (^Buffer [{:keys [mmap-file-path]
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
     ;;Force failure here before we get going - not all platforms support mmap.
     (FileBackedTextBuilder. encoding mmap-writer offsets cached-io)))
  (^Buffer [] (file-backed-text nil)))
