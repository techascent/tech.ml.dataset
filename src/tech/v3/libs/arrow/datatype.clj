(ns tech.v3.libs.arrow.datatype
  "Arrow <-> datatype bindings and data manipulations."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.errors :as errors]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype Buffer ObjectReader BooleanBuffer]
           [org.apache.arrow.vector VarCharVector BitVector TinyIntVector UInt1Vector
            SmallIntVector UInt2Vector IntVector UInt4Vector BigIntVector UInt8Vector
            Float4Vector Float8Vector DateDayVector DateMilliVector TimeMilliVector
            DurationVector TimeStampMicroTZVector TimeStampMicroVector TimeStampVector
            TimeStampMilliVector TimeStampMilliTZVector FieldVector VectorSchemaRoot
            BaseVariableWidthVector BaseFixedWidthVector TimeStampNanoVector
            TimeStampNanoTZVector TimeStampSecVector TimeStampSecTZVector]
           [org.apache.arrow.vector.dictionary DictionaryProvider Dictionary
            DictionaryProvider$MapDictionaryProvider]
           [org.apache.arrow.memory ArrowBuf]
           [org.roaringbitmap RoaringBitmap]
           [java.util List]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


;; Base datatype bindings
(defn arrow-buffer->native-buffer
  ^NativeBuffer [datatype ^ArrowBuf buf]
  (let [native-buf (native-buffer/wrap-address
                    (.memoryAddress buf)
                    (.capacity buf) :int8
                    :little-endian buf)]
    (if (= datatype :int8)
      native-buf
      (native-buffer/set-native-datatype native-buf datatype))))


(defn int8-buf->missing
  ^RoaringBitmap [data-buf ^long n-elems]
  (let [data-buf (dtype/->reader data-buf)
        ^RoaringBitmap missing (bitmap/->bitmap)
        n-bytes (quot (+ n-elems 7) 8)]
    (dotimes [idx n-bytes]
      (let [offset (pmath/* 8 idx)
            data (unchecked-int (.readByte data-buf idx))]
        (when-not (== data -1)
          ;;TODO - find more elegant way of pulling this off
          (when (== 0 (pmath/bit-and data 1))
            (.add missing offset))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 1)))
            (.add missing (pmath/+ offset 1)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 2)))
            (.add missing (pmath/+ offset 2)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 3)))
            (.add missing (pmath/+ offset 3)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 4)))
            (.add missing (pmath/+ offset 4)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 5)))
            (.add missing (pmath/+ offset 5)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 6)))
            (.add missing (pmath/+ offset 6)))
          (when (== 0 (pmath/bit-and data (pmath/bit-shift-left 1 7)))
            (.add missing (pmath/+ offset 7))))))
    (dtype-proto/set-and missing (range n-elems))))


(defn valid-buf->missing
  ^RoaringBitmap [^ArrowBuf buffer ^long n-elems]
  (int8-buf->missing (arrow-buffer->native-buffer :int8 buffer) n-elems))


(defn add-bit
  ^long [^long data ^long bit-idx ^RoaringBitmap bitmap ^long offset]
  ;;Logic here is reversed as data is an inclusion mask and the bitmap
  ;;is an exclusion mask
  (if (.contains bitmap (+ offset bit-idx))
    data
    (bit-or data (bit-shift-left 1 bit-idx))))


(defn missing->valid-buf
  ^ArrowBuf [^RoaringBitmap bitmap ^ArrowBuf buffer ^long n-elems]
  (let [nio-buf (arrow-buffer->native-buffer :int8 buffer)
        n-bytes (quot (+ n-elems 7) 8)
        writer (dtype/->buffer nio-buf)]
    (dtype/set-constant! nio-buf 0 n-bytes (byte -1))
    (when-not (.isEmpty bitmap)
      (dotimes [idx n-bytes]
        (let [offset (pmath/* 8 idx)
              data (-> (unchecked-long 0)
                       (add-bit 0 bitmap offset)
                       (add-bit 1 bitmap offset)
                       (add-bit 2 bitmap offset)
                       (add-bit 3 bitmap offset)
                       (add-bit 4 bitmap offset)
                       (add-bit 5 bitmap offset)
                       (add-bit 6 bitmap offset)
                       (add-bit 7 bitmap offset))]
          (.writeByte writer idx (unchecked-byte data)))))
    buffer))


(defn varchar->string-reader
  "copies the data into a list of strings."
  ^List [^VarCharVector fv]
  (let [n-elems (dtype/ecount fv)
        value-buf (arrow-buffer->native-buffer :int8 (.getDataBuffer fv))
        offset-buf (arrow-buffer->native-buffer :int32 (.getOffsetBuffer fv))
        offset-rdr (dtype/->reader offset-buf)]
    (reify ObjectReader
      (elemwiseDatatype [rdr] :string)
      (lsize [rdr] n-elems)
      (readObject [rdr idx]
        (let [cur-offset (.readLong offset-rdr idx)
              next-offset (.readLong offset-rdr (inc idx))
              str-data (dtype/sub-buffer value-buf cur-offset
                                         (- next-offset cur-offset))]
          (errors/when-not-errorf
           str-data
           "Nil value returned from sub buffer: %s - 0x%x - %d-%d"
           (type value-buf) (.address value-buf) cur-offset next-offset)
         (String. ^bytes (dtype/->byte-array str-data)))))))


(defn varchar->strings
  ^List [^VarCharVector fv]
  (dtype/make-container :list :string (varchar->string-reader fv)))


(defn dictionary->strings
  ^List [^Dictionary dict]
  (varchar->strings (.getVector dict)))


(defn strings->varchar!
  ^VarCharVector [string-reader ^RoaringBitmap missing ^VarCharVector fv]
  (let [byte-list (dtype/make-list :int8)
        offsets (dtype/make-list :int32)
        ^RoaringBitmap missing (or missing (bitmap/->bitmap))
        str-rdr (dtype/->reader string-reader)
        n-elems (dtype/ecount str-rdr)]
    (.add offsets 0)
    (dotimes [idx n-elems]
      (when-not (.contains missing idx)
        (let [byte-data (.getBytes ^String (str-rdr idx) "UTF-8")]
          (.addAll byte-list (dtype/as-concrete-buffer byte-data))))
      (.addLong offsets (.lsize byte-list)))
    (.allocateNew fv (.size byte-list) n-elems)
    (.setLastSet fv n-elems)
    (.setValueCount fv n-elems)
    (let [valid-buf (.getValidityBuffer fv)
          data-buf (.getDataBuffer fv)
          offset-buf (.getOffsetBuffer fv)]
      (missing->valid-buf missing valid-buf n-elems)
      (dtype/copy! offsets
                   (dtype/sub-buffer (arrow-buffer->native-buffer :int32 offset-buf)
                                     0 (dtype/ecount offsets)))
      (dtype/copy! byte-list
                   (dtype/sub-buffer (arrow-buffer->native-buffer :int8 data-buf)
                                     0 (dtype/ecount byte-list))))
    fv))


(defn byte-buffer->bitwise-boolean-buffer
  ^Buffer[bitbuffer ^long n-elems]
  (let [buf (dtype/->buffer bitbuffer)]
    (reify BooleanBuffer
      (lsize [rdr] n-elems)
      (allowsRead [rdr] (.allowsRead buf))
      (allowsWrite [rdr] (.allowsWrite buf))
      (readBoolean [rdr idx]
        (let [data (.readByte buf (quot idx 8))]
          (if (pmath/== 1 (pmath/bit-and data (pmath/bit-shift-left 1 (rem idx 8))))
            true
            false)))
      (writeBoolean [wtr idx value]
        (locking buf
          (let [byte-idx (quot idx 8)
                byte-data (unchecked-int (.readByte buf byte-idx))
                bitmask (unchecked-int (pmath/bit-shift-left 1 (rem idx 8)))
                byte-data (if value
                            (pmath/bit-or byte-data bitmask)
                            (pmath/bit-and byte-data (pmath/bit-not bitmask)))]
            (.writeByte buf byte-idx (unchecked-byte byte-data))))))))


(defn bitwise-vec->boolean-buffer
  ^Buffer [^BitVector data]
  (byte-buffer->bitwise-boolean-buffer
   (arrow-buffer->native-buffer :int8 (.getDataBuffer data))
   (dtype/ecount data)))


(defn primitive-vec->native-buffer
  ^NativeBuffer [^FieldVector vvec]
  (let [data (.getDataBuffer vvec)]
    (if (= :boolean (dtype/get-datatype vvec))
      (arrow-buffer->native-buffer :int8 data)
      (-> (arrow-buffer->native-buffer (dtype/get-datatype vvec) data)
          (dtype-proto/sub-buffer 0 (dtype/ecount vvec))))))


(def datatype->vec-type-map
  {:boolean 'BitVector
   :uint8 'UInt1Vector
   :int8 'TinyIntVector
   :uint16 'UInt2Vector
   :int16 'SmallIntVector
   :uint32 'UInt4Vector
   :int32 'IntVector
   :uint64 'UInt8Vector
   :int64 'BigIntVector
   :float32 'Float4Vector
   :float64 'Float8Vector
   :string 'VarCharVector
   :text 'VarCharVector
   :epoch-milliseconds 'TimeStampMilliVector})


(defn as-bit-vector ^BitVector [item] item)
(defn as-uint8-vector ^UInt1Vector [item] item)
(defn as-int8-vector ^TinyIntVector [item] item)
(defn as-uint16-vector ^UInt2Vector [item] item)
(defn as-int16-vector ^SmallIntVector [item] item)
(defn as-uint32-vector ^UInt4Vector [item] item)
(defn as-int32-vector ^IntVector [item] item)
(defn as-uint64-vector ^UInt8Vector [item] item)
(defn as-int64-vector ^BigIntVector [item] item)
(defn as-float32-vector ^Float4Vector [item] item)
(defn as-float64-vector ^Float8Vector [item] item)
(defn as-varchar-vector ^VarCharVector [item] item)
(defn as-timestamp-vector ^TimeStampVector [item] item)
(defn as-timestamp-milli-vector ^TimeStampMilliVector [item] item)
(defn as-timestamp-micro-vector ^TimeStampMicroVector [item] item)
(defn as-timestamp-micro-tz-vector ^TimeStampMicroTZVector [item] item)


(defmacro datatype->vec-type
  [datatype item]
  (case datatype
    :boolean `(as-bit-vector ~item)
    :uint8 `(as-uint8-vector ~item)
    :int8 `(as-int8-vector ~item)
    :uint16 `(as-uint16-vector ~item)
    :int16 `(as-int16-vector ~item)
    :uint32 `(as-uint32-vector ~item)
    :int32 `(as-int32-vector ~item)
    :uint64 `(as-uint64-vector ~item)
    :int64 `(as-int64-vector ~item)
    :float32 `(as-float32-vector ~item)
    :float64 `(as-float64-vector ~item)
    :string `(as-varchar-vector ~item)
    :epoch-milliseconds `(as-timestamp-milli-vector ~item)))


(def extension-datatypes
  [[:epoch-milliseconds `TimeStampMilliTZVector]
   [:int64 `TimeStampMilliVector]
   [:int64 `TimeStampVector]])

(defn- primitive-datatype?
  [datatype]
  (boolean #{:int8 :uint8
             :int16 :uin16
             :int32 :uint32
             :int64 :uint64
             :float32 :float64
             :epoch-milliseconds}))


(defmacro implement-datatype-protos
  []
  `(do
     ~@(->> (concat datatype->vec-type-map
                    extension-datatypes)
            (map (fn [[dtype vectype]]
                   `(extend-type ~vectype
                      dtype-proto/PElemwiseDatatype
                      (elemwise-datatype [item#] ~dtype)
                      dtype-proto/PDatatype
                      (datatype [item#] :arrow-vector)
                      dtype-proto/PECount
                      (ecount [item#] (.getValueCount item#))
                      dtype-proto/PToNativeBuffer
                      (convertible-to-native-buffer? [item#]
                        ~(and (not= :boolean dtype)
                              (primitive-datatype? dtype)))
                      (->native-buffer [item#]
                        (primitive-vec->native-buffer item#))
                      (clone [~'item]
                        (dtype/make-container
                         :jvm-heap
                         ~dtype
                         ~'item))
                      dtype-proto/PToBuffer
                      (convertible-to-buffer? [item#] true)
                      (->buffer [~'item]
                        ~(case dtype
                           :boolean `(bitwise-vec->boolean-buffer ~'item)
                           :string `(varchar->string-reader ~'item)
                           `(-> (primitive-vec->native-buffer ~'item)
                                (dtype/->buffer))))))))))


(implement-datatype-protos)
