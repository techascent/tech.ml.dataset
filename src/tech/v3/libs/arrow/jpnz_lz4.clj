(ns tech.v3.libs.arrow.jpnz-lz4
  (:require [tech.v3.datatype :as dtype]
            [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.array_buffer ArrayBuffer]
           [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn- ensure-bytes-array-buffer
  ^ArrayBuffer [data]
  (if-let [ary-buf (dtype/as-array-buffer data)]
    (if (= :int8 (dtype/elemwise-datatype ary-buf))
      ary-buf
      (dtype/make-container :int8 data))))

(defn create-jpnz-lz4-frame-compressor
  [comp-map]
  (assert (= :lz4 (get comp-map :compression-type)))
  (fn [compbuf dstbuf]
    (let [^ByteArrayOutputStream dstbuf (or dstbuf (java.io.ByteArrayOutputStream.))
          os (net.jpountz.lz4.LZ4FrameOutputStream. dstbuf)
          srcbuf (ensure-bytes-array-buffer compbuf)
          ^bytes src-data (.ary-data srcbuf)]
      (.write os src-data (unchecked-int (.offset srcbuf)) (unchecked-int (.n-elems srcbuf)))
      (.close os)
      (let [final-bytes (.toByteArray dstbuf)]
        (.reset dstbuf)
        {:writer-cache dstbuf
         :dst-buffer final-bytes}))))

(defn create-jpnz-lz4-decompressor
  []
  (log/warn "Unable to load native lz4 library, falling back to jpountz.
Dependent block frames are not supported!!")
  (fn [srcbuf dstbuf]
    (let [src-byte-data (dtype/->byte-array srcbuf)
          bis (ByteArrayInputStream. src-byte-data)
          is (net.jpountz.lz4.LZ4FrameInputStream. bis)
          temp-dstbuf (byte-array (dtype/ecount dstbuf))]
      (.read is temp-dstbuf)
      (dtype/copy! temp-dstbuf dstbuf))))
