(ns tech.libs.arrow.mmap
  (:require [tech.io :as io]
            [tech.resource :as resource]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.jna :as dtype-jna]
            [tech.v2.datatype.typecast :as typecast]
            [primitive-math :as pmath])
  (:import [xerial.larray.mmap MMapBuffer MMapMode]
           [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe]
           [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)


(defn unsafe
  ^Unsafe []
  UnsafeUtil/unsafe)


(defprotocol PToNativeBuffer
  (convertible-to-native-buffer? [buf])
  (->native-buffer [buf]))


(defmacro native-buffer->reader
  [datatype advertised-datatype buffer address n-elems]
  (let [byte-width (casting/numeric-byte-width datatype)]
    `(reify
       PToNativeBuffer
       (convertible-to-native-buffer? [this#] true)
       (->native-buffer [this#] ~buffer)
       ;;Forward protocol methods that are efficiently implemented by the buffer
       dtype-proto/PClone
       (clone [this#]
         (-> (dtype-proto/clone ~buffer)
             (dtype-proto/->reader {})))
       dtype-proto/PBuffer
       (sub-buffer [this# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->reader {})))
       dtype-proto/PSetConstant
       (set-constant! [buffer# offset# value# elem-count#]
         (-> (dtype-proto/set-constant! ~buffer offset# value# elem-count#)
             (dtype-proto/->reader {})))
       dtype-proto/PToJNAPointer
       (convertible-to-data-ptr? [item#] true)
       (->jna-ptr [item#] (Pointer. ~address))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [item#] (< ~n-elems Integer/MAX_VALUE))
       (->buffer-backing-store [item#]
         (dtype-proto/->buffer-backing-store ~buffer))
       ~(typecast/datatype->reader-type (casting/safe-flatten datatype))
       (getDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (read [rdr# ~'idx]
         ~(case datatype
            :int8 `(.getByte (unsafe) (pmath/+ ~address ~'idx))
            :uint8 `(-> (.getByte (unsafe) (pmath/+ ~address ~'idx))
                        (pmath/byte->ubyte))
            :int16 `(.getShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
            :uint16 `(-> (.getShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
                         (pmath/short->ushort))
            :int32 `(.getInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
            :uint32 `(-> (.getInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
                         (pmath/int->uint))
            :int64 `(.getLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
            :uint64 `(-> (.getLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))))
            :float32 `(.getFloat (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
            :float64 `(.getDouble (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))))))))


(defmacro native-buffer->writer
  [datatype advertised-datatype buffer address n-elems]
  (let [byte-width (casting/numeric-byte-width datatype)]
    `(reify
       PToNativeBuffer
       (convertible-to-native-buffer? [this#] true)
       (->native-buffer [this#] ~buffer)
       ;;Forward protocol methods that are efficiently implemented by the buffer
       dtype-proto/PClone
       (clone [this#]
         (-> (dtype-proto/clone ~buffer)
             (dtype-proto/->writer {})))
       dtype-proto/PBuffer
       (sub-buffer [this# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->writer {})))
       dtype-proto/PSetConstant
       (set-constant! [buffer# offset# value# elem-count#]
         (-> (dtype-proto/set-constant! ~buffer offset# value# elem-count#)
             (dtype-proto/->writer {})))
       dtype-proto/PToJNAPointer
       (convertible-to-data-ptr? [item#] true)
       (->jna-ptr [item#] (Pointer. ~address))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [item#] (< ~n-elems Integer/MAX_VALUE))
       (->buffer-backing-store [item#]
         (dtype-proto/->buffer-backing-store ~buffer))
       ~(typecast/datatype->writer-type (casting/safe-flatten datatype))
       (getDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (write [rdr# ~'idx ~'value]
         ~(case datatype
            :int8 `(.putByte (unsafe) (pmath/+ ~address ~'idx) ~'value)
            :uint8 `(.putByte (unsafe) (pmath/+ ~address ~'idx)
                              (unchecked-byte ~'value))
            :int16 `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                               ~'value)
            :uint16 `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                                (unchecked-short ~'value))
            :int32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                             ~'value)
            :uint32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                              (unchecked-int ~'value))
            :int64 `(.putLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                              ~'value)
            :uint64 `(.putLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                               ~'value)
            :float32 `(.putFloat (unsafe)
                                 (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                                 ~'value)
            :float64 `(.putDouble (unsafe)
                                  (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                                  ~'value))))))


;;Size is in elements, not in bytes
(defrecord NativeBuffer [^long address ^long n-elems datatype]
  PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] this)
  dtype-proto/PDatatype
  (get-datatype [this] datatype)
  dtype-proto/PCountable
  (ecount [this] n-elems)
  dtype-proto/PClone
  (clone [this]
    (dtype/make-container
     (if (casting/unsigned-integer-type? datatype)
       :typed-buffer
       :java-array)
     datatype
     this))
  dtype-proto/PBuffer
  (sub-buffer [this offset length]
    (let [offset (long offset)
          length (long length)]
      (when-not (<= (+ offset length) n-elems)
        (throw (Exception.
                (format "Offset+length (%s) > n-elems (%s)"
                        (+ offset length) n-elems))))
      (NativeBuffer. (+ address offset) length datatype)))
  dtype-proto/PSetConstant
  (set-constant! [buffer offset value elem-count]
    (if (or (= :datatype :int8)
            (= (double value) 0.0))
      (.setMemory (unsafe) (+ address (long offset))
                  (* (long elem-count) (casting/numeric-byte-width datatype))
                  (byte 0))
      (let [writer (dtype/->writer (dtype/sub-buffer buffer offset elem-count))
            value (casting/cast value datatype)]
        (dotimes [iter elem-count]
          (writer iter value)))))
  dtype-proto/PToJNAPointer
  (convertible-to-data-ptr? [item#] true)
  (->jna-ptr [item#] (Pointer. address))
  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item#] (< n-elems Integer/MAX_VALUE))
  (->buffer-backing-store [item#]
    (let [ptr (Pointer. address)
          unaliased-dtype (casting/un-alias-datatype datatype)
          n-bytes (* n-elems (casting/numeric-byte-width unaliased-dtype))]
      (dtype-jna/pointer->nio-buffer ptr unaliased-dtype n-bytes)))
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (-> (case (casting/un-alias-datatype datatype)
          :int8 (native-buffer->reader :int8 datatype this address n-elems)
          :uint8 (native-buffer->reader :uint8 datatype this address n-elems)
          :int16 (native-buffer->reader :int16 datatype this address n-elems)
          :uint16 (native-buffer->reader :uint16 datatype this address n-elems)
          :int32 (native-buffer->reader :int32 datatype this address n-elems)
          :uint32 (native-buffer->reader :uint32 datatype this address n-elems)
          :int64 (native-buffer->reader :int64 datatype this address n-elems)
          :uint64 (native-buffer->reader :uint64 datatype this address n-elems)
          :float32 (native-buffer->reader :float32 datatype this address n-elems)
          :float64 (native-buffer->reader :float64 datatype this address n-elems))
        (dtype-proto/->reader options)))
  dtype-proto/PToWriter
  (convertible-to-writer? [this] true)
  (->writer [this options]
    (-> (case (casting/un-alias-datatype datatype)
          :int8 (native-buffer->writer :int8 datatype this address n-elems)
          :uint8 (native-buffer->writer :uint8 datatype this address n-elems)
          :int16 (native-buffer->writer :int16 datatype this address n-elems)
          :uint16 (native-buffer->writer :uint16 datatype this address n-elems)
          :int32 (native-buffer->writer :int32 datatype this address n-elems)
          :uint32 (native-buffer->writer :uint32 datatype this address n-elems)
          :int64 (native-buffer->writer :int64 datatype this address n-elems)
          :uint64 (native-buffer->writer :uint64 datatype this address n-elems)
          :float32 (native-buffer->writer :float32 datatype this address n-elems)
          :float64 (native-buffer->writer :float64 datatype this address n-elems))
        (dtype-proto/->writer options))))


(defn as-native-buffer
  ^NativeBuffer [item]
  (when (convertible-to-native-buffer? item)
    (->native-buffer item)))


(defn set-native-datatype
  ^NativeBuffer [item datatype]
  (if-let [nb (as-native-buffer item)]
    (let [original-size (.n-elems nb)
          n-bytes (* original-size (casting/numeric-byte-width
                                    (dtype/get-datatype item)))
          new-byte-width (casting/numeric-byte-width
                          (dtype/get-datatype item))]
      (NativeBuffer. (.address nb) (quot n-bytes new-byte-width) datatype))))


;;One off data reading
(defn read-double
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 8) 0))
   (.getDouble (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 8) 0))
   (.getDouble (unsafe) (.address native-buffer))))


(defn read-float
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 4) 0))
   (.getFloat (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 4) 0))
   (.getFloat (unsafe) (.address native-buffer))))


(defn read-long
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 8) 0))
   (.getLong (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 8) 0))
   (.getLong (unsafe) (.address native-buffer))))


(defn read-int
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 4) 0))
   (.getInt (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 4) 0))
   (.getInt (unsafe) (.address native-buffer))))


(defn read-short
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 2) 0))
   (unchecked-long
    (.getShort (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 2) 0))
   (unchecked-long
    (.getShort (unsafe) (.address native-buffer)))))


(defn read-byte
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 1) 0))
   (unchecked-long
    (.getByte (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 1) 0))
   (unchecked-long
    (.getByte (unsafe) (.address native-buffer)))))


(defn read-only-mmap-file
  "mmap a file returning a map containing longs of {:address and :size}.
  File is bound to the active stack resource context and all memory will be
  released when the resource context itself releases."
  ([fpath {:keys [resource-type]
                 :or {resoure-type :stack}}]
   (let [file (io/file fpath)
         _ (when-not (.exists file)
             (throw (Exception. (format "%s not found" fpath))))
         ;;Mapping to read-only means pages can be shared between processes
         map-buf (MMapBuffer. file MMapMode/READ_ONLY)]
     (resource/track map-buf #(.close map-buf) resource-type)
     (->NativeBuffer (.address map-buf) (.size map-buf) :int8)))
  ([fpath]
   (read-only-mmap-file fpath {})))
