(ns tech.libs.arrow.copying
  (:require [tech.ml.dataset.base :as ds-base]
            [tech.ml.protocols.column :as col-proto]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.string-table :as str-table]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.mmap :as mmap]
            [clojure.edn :as edn]
            [tech.ml.utils :as ml-utils]
            [tech.io :as io]
            [primitive-math :as pmath])
  (:import [org.apache.arrow.vector.types.pojo FieldType ArrowType Field Schema
            ArrowType$Int ArrowType$FloatingPoint ArrowType$Bool
            ArrowType$Utf8 ArrowType$Date ArrowType$Time ArrowType$Timestamp
            ArrowType$Duration DictionaryEncoding ]
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.memory RootAllocator BaseAllocator BufferAllocator]
           [org.apache.arrow.vector VarCharVector BitVector TinyIntVector UInt1Vector
            SmallIntVector UInt2Vector IntVector UInt4Vector BigIntVector UInt8Vector
            Float4Vector Float8Vector DateDayVector DateMilliVector TimeMilliVector
            DurationVector TimeStampMicroTZVector TimeStampMicroVector TimeStampVector
            TimeStampMilliVector TimeStampMilliTZVector FieldVector VectorSchemaRoot
            BaseVariableWidthVector BaseFixedWidthVector]
           [org.apache.arrow.vector.dictionary DictionaryProvider Dictionary
            DictionaryProvider$MapDictionaryProvider]
           [org.apache.arrow.vector.ipc ArrowStreamReader ArrowStreamWriter
            ArrowFileWriter ArrowFileReader]
           [org.apache.arrow.vector.types Types]
           [org.apache.arrow.memory ArrowBuf]
           [org.roaringbitmap RoaringBitmap]
           [java.util Map ArrayList List HashMap]
           [tech.ml.dataset.impl.column Column]
           [tech.v2.datatype ObjectWriter]
           [tech.ml.dataset.string_table StringTable]
           [tech.ml.dataset.dynamic_int_list DynamicIntList]
           [java.time ZoneId]
           [java.nio ByteBuffer Buffer ByteOrder]
           [tech.v2.datatype.typed_buffer TypedBuffer]
           [tech.v2.datatype BooleanWriter]
           [it.unimi.dsi.fastutil.bytes ByteArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [com.sun.jna Pointer]
           [sun.misc Unsafe]
           [org.apache.arrow.memory.util MemoryUtil]
           [tech.v2.datatype.mmap NativeBuffer]))


;;TODO - check out gandiva


(set! *warn-on-reflection* true)


;; Allocator management

(defonce ^:dynamic *allocator* (delay (RootAllocator. Long/MAX_VALUE)))


(defn allocator
  (^BufferAllocator []
   (let [alloc-deref @*allocator*]
     (cond
       (instance? clojure.lang.IDeref alloc-deref)
       @alloc-deref
       (instance? BufferAllocator alloc-deref)
       alloc-deref
       :else
       (throw (Exception. "No allocator provided.  See `with-allocator`")))))
  (^BufferAllocator [options]
   (or (:allocator options) (allocator))))


(defmacro with-allocator
  "Bind a new allocator.  alloc* must be either an instance of
  org.apache.arrow.memory.BaseAllocator or an instance of IDeref that resolves to an
  instance of BaseAllocator."
  [alloc* & body]
  `(with-bindings {#'*allocator* alloc*}
     ~@body))



;; Base datatype bindings
(defn arrow-buffer->typed-buffer
  ^NativeBuffer [datatype ^ArrowBuf buf]
  (let [native-buf (NativeBuffer. (.memoryAddress buf)
                                  (.capacity buf) :int8)]
    (if (= datatype :int8)
      native-buf
      (mmap/set-native-datatype native-buf datatype))))


(defn int8-buf->missing
  ^RoaringBitmap [data-buf n-elems]
  (let [data-buf (typecast/datatype->reader :int8 data-buf)
        ^RoaringBitmap missing (dtype/->bitmap-set)
        n-bytes (quot (+ n-elems 7) 8)]
    (dotimes [idx n-bytes]
      (let [offset (pmath/* 8 idx)
            data (unchecked-int (.read data-buf idx))]
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
          (.add missing (pmath/+ offset 7)))))
    missing))


(defn valid-buf->missing
  ^RoaringBitmap [^ArrowBuf buffer ^long n-elems]
  (int8-buf->missing (arrow-buffer->typed-buffer :int8 buffer) n-elems))


(defn add-bit
  ^long [^long data ^long bit-idx ^RoaringBitmap bitmap ^long offset]
  ;;Logic here is reversed as data is an inclusion mask and the bitmap
  ;;is an exclusion mask
  (if (.contains bitmap (+ offset bit-idx))
    data
    (bit-or data (bit-shift-left 1 bit-idx))))


(defonce t (atom nil))
(defn missing->valid-buf
  ^ArrowBuf [^RoaringBitmap bitmap ^ArrowBuf buffer ^long n-elems]
  (reset! t buffer)
  (let [nio-buf (arrow-buffer->typed-buffer :int8 buffer)
        ^RoaringBitmap missing (dtype/->bitmap-set)
        n-bytes (quot (+ n-elems 7) 8)
        writer (typecast/datatype->writer :int8 nio-buf)]
    (if (.isEmpty bitmap)
      ;;Common case
      (dtype/set-constant! nio-buf 0 (byte -1) n-bytes)
      ;;Tedious case.  It may be better here to iterate and find next missing
      ;;some similar pathway so we have a fastpath when there are no missing
      ;;elements
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
          (.write writer idx (unchecked-byte data)))))
    buffer))


(defn varchar->string-reader
  "copies the data into a list of strings."
  ^List [^VarCharVector fv]
  (let [n-elems (dtype/ecount fv)
        value-buf (arrow-buffer->typed-buffer :int8 (.getDataBuffer fv))
        offset-buf (arrow-buffer->typed-buffer :int32 (.getOffsetBuffer fv))
        offset-rdr (dtype/->reader offset-buf)]
    (dtype/object-reader
     n-elems
     (fn [^long idx]
       (let [cur-offset (offset-rdr idx)
             next-offset (offset-rdr (inc idx))]
         (String. ^bytes (dtype/->array-copy
                          (dtype/sub-buffer value-buf cur-offset
                                            (- next-offset cur-offset))))))
     :string)))


(defn varchar->strings
  ^List [^VarCharVector fv]
  (doto (ArrayList.)
    (.addAll (varchar->string-reader fv))))


(defn dictionary->strings
  ^List [^Dictionary dict]
  (varchar->strings (.getVector dict)))


(defn strings->varchar!
  ^VarCharVector [string-reader ^RoaringBitmap missing ^VarCharVector fv]
  (let [byte-list (ByteArrayList.)
        offsets (IntArrayList.)
        ^RoaringBitmap missing (or missing (dtype/->bitmap-set))
        str-rdr (dtype/->reader string-reader)
        n-elems (dtype/ecount str-rdr)]
    (.add offsets 0)
    (dotimes [idx n-elems]
      (when-not (.contains missing idx)
        (let [byte-data (.getBytes ^String (str-rdr idx) "UTF-8")]
          (.addAll byte-list (ByteArrayList/wrap byte-data))))
      (.add offsets (.size byte-list)))
    (.allocateNew fv (.size byte-list) n-elems)
    (.setLastSet fv n-elems)
    (.setValueCount fv n-elems)
    (let [valid-buf (.getValidityBuffer fv)
          data-buf (.getDataBuffer fv)
          offset-buf (.getOffsetBuffer fv)]
      (missing->valid-buf missing valid-buf n-elems)
      (dtype/copy! offsets (arrow-buffer->typed-buffer :int32 offset-buf))
      (dtype/copy! byte-list (arrow-buffer->typed-buffer :int8 data-buf)))
    fv))


(defn bitwise-vec->boolean-reader
  [^BitVector data]
  (let [rdr (typecast/datatype->reader
             :int8
             (arrow-buffer->typed-buffer
              :int8 (.getDataBuffer data)))
        n-elems (dtype/ecount data)]
    (dtype/make-reader
     :boolean n-elems
     ;;idx is the indexing variable that is implicitly defined in scope.
     (let [byte-data (.read rdr (quot idx 8))]
       (if (pmath/== 1 (pmath/bit-and data (pmath/bit-shift-left 1 (rem idx 8))))
         true
         false)))))


(defn bitwise-vec->boolean-writer
  [^BitVector data]
  (let [rdr (typecast/datatype->reader
             :int8
             (arrow-buffer->typed-buffer
              :int8 (.getDataBuffer data)))
        src-wtr (typecast/datatype->writer
                 :int8
                 (arrow-buffer->typed-buffer
                  :int8 (.getDataBuffer data)))
        n-elems (dtype/ecount data)]
    (reify BooleanWriter
      (lsize [wtr] n-elems)
      (write [wtr idx value]
        (locking src-wtr
          (let [byte-idx (quot idx 8)
                byte-data (unchecked-int (.read rdr byte-idx))
                bitmask (unchecked-int (pmath/bit-shift-left 1 (rem idx 8)))
                byte-data (if value
                            (pmath/bit-or byte-data bitmask)
                            (pmath/bit-and byte-data (pmath/bit-not bitmask)))]
            (.write src-wtr byte-idx (unchecked-byte byte-data))
            (if (pmath/== 1 (pmath/bit-and data byte-data bitmask))
              true
              false)))))))


(defn primitive-vec->typed-buffer
  [^FieldVector vvec]
  (let [data (.getDataBuffer vvec)]
    (if (= :boolean (dtype/get-datatype vvec))
      (arrow-buffer->typed-buffer :int8 data)
      (-> (arrow-buffer->typed-buffer (dtype/get-datatype vvec) data)
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
   :encoded-text 'VarCharVector
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
   [:int64 `TimeStampMilliVector]])

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
                      dtype-proto/PDatatype
                      (get-datatype [item#] ~dtype)
                      dtype-proto/PCountable
                      (ecount [item#] (.getValueCount item#))
                      dtype-proto/PToNioBuffer
                      (convertible-to-nio-buffer? [item#] ~(primitive-datatype? dtype))
                      (->buffer-backing-store [item#]
                        (-> (primitive-vec->typed-buffer item#)
                            (dtype-proto/->buffer-backing-store)))
                      dtype-proto/PToJNAPointer
                      (convertible-to-data-ptr? [item#] ~(primitive-datatype? dtype))
                      (->jna-ptr [item#]
                        (let [arrow-buf# (.getDataBuffer item#)]
                          (Pointer. (.memoryAddress arrow-buf#))))
                      dtype-proto/PClone
                      (clone [~'item]
                        (dtype/make-container
                         ~(if (or (casting/unsigned-integer-type? dtype)
                                  (= dtype :epoch-milliseconds))
                            :typed-buffer
                            :java-array)
                         ~dtype
                         ~'item))
                      dtype-proto/PToReader
                      (convertible-to-reader? [item#] true)
                      (->reader [~'item options#]
                        (->
                         ~(case dtype
                            :boolean `(bitwise-vec->boolean-reader ~'item)
                            :string `(varchar->string-reader ~'item)
                            :text `(varchar->string-reader ~'item)
                            :encoded-text `(varchar->string-reader ~'item)
                            `(primitive-vec->typed-buffer ~'item))
                         (dtype-proto/->reader options#)))
                      dtype-proto/PToWriter
                      (convertible-to-writer? [item#] ~(if (= dtype :string)
                                                         `false
                                                         `true))
                      (->writer [~'item options#]
                        (->
                         ~(if (= dtype :boolean)
                            `(bitwise-vec->boolean-writer ~'item)
                            `(primitive-vec->typed-buffer ~'item))
                         (dtype-proto/->writer options#)))))))))


(implement-datatype-protos)


(defn make-field
  ^Field [^String name ^FieldType field-type]
  (Field. name field-type nil))


(defn field-type
  ^FieldType
  ([nullable? ^FieldType datatype ^DictionaryEncoding dict-encoding ^Map str-str-meta]
   (FieldType. (boolean nullable?) datatype dict-encoding str-str-meta))
  ([nullable? datatype str-meta]
   (field-type nullable? datatype nil str-meta))
  ([nullable? datatype]
   (field-type nullable? datatype nil)))


(defn ->str-str-meta
  [metadata]
  (->> metadata
       (map (fn [[k v]] [(pr-str k) (pr-str v)]))
       (into {})))


(defn datatype->field-type
  (^FieldType [datatype & [nullable? metadata extra-data]]
   (let [nullable? (or nullable? (= :object (casting/flatten-datatype datatype)))
         metadata (->str-str-meta metadata)
         ft-fn (fn [arrow-type & [dict-encoding]]
                 (field-type nullable? arrow-type dict-encoding metadata))
         datatype (dtype-dt/unpack-datatype datatype)]
     (case (if (= :epoch-milliseconds datatype)
             datatype
             (casting/un-alias-datatype datatype))
       :boolean (ft-fn (ArrowType$Bool.))
       :uint8 (ft-fn (ArrowType$Int. 8 false))
       :int8 (ft-fn (ArrowType$Int. 8 true))
       :uint16 (ft-fn (ArrowType$Int. 16 false))
       :int16 (ft-fn (ArrowType$Int. 16 true))
       :uint32 (ft-fn (ArrowType$Int. 32 false))
       :int32 (ft-fn (ArrowType$Int. 32 true))
       :uint64 (ft-fn (ArrowType$Int. 64 false))
       :int64 (ft-fn (ArrowType$Int. 64 true))
       :float32 (ft-fn (ArrowType$FloatingPoint. FloatingPointPrecision/SINGLE))
       :float64 (ft-fn (ArrowType$FloatingPoint. FloatingPointPrecision/DOUBLE))
       :epoch-milliseconds (ft-fn (ArrowType$Timestamp. TimeUnit/MILLISECOND (str (:timezone extra-data))))
       :local-time (ft-fn (ArrowType$Time. TimeUnit/MILLISECOND (int 8)))
       :duration (ft-fn (ArrowType$Duration. TimeUnit/MICROSECOND))
       :instant (ft-fn (ArrowType$Timestamp. TimeUnit/MILLISECOND (str (:timezone extra-data))))
       :string (let [^DictionaryEncoding encoding (:encoding extra-data)
                     int-type (.getIndexType encoding)]
                 (when-not encoding
                   (throw (Exception.
                           "String tables must have a dictionary encoding")))
                 (ft-fn int-type encoding))
       :text (ft-fn (ArrowType$Utf8.))))))


(defmulti metadata->field-type
  "Convert column metadata into an arrow field"
  (fn [meta any-missing?]
    :datatype))



(defn string-column->dict
  "Given a string column, return a map of {:dictionary :indices} which
  will be encoded according to the data in string-col->dict-id-table-width"
  ^Dictionary [col]
  (let [str-t (ds-base/ensure-column-string-table col)
        indices (str-table/indices str-t)
        int->str (str-table/int->string str-t)
        bit-width (casting/int-width (dtype/get-datatype indices))
        metadata (meta col)
        colname (:name metadata)
        dict-id (.hashCode ^Object colname)
        arrow-indices-type (ArrowType$Int. bit-width true)
        encoding (DictionaryEncoding. dict-id false arrow-indices-type)
        ftype (datatype->field-type :text true)
        varchar-vec (strings->varchar! (dtype/->reader int->str :string)
                                       nil
                                       (VarCharVector. "unnamed" (allocator)))]
    (Dictionary. varchar-vec encoding)))


(defn string-col->encoding
  "Given a string column return a map of :dict-id :table-width.  The dictionary
  id is the hashcode of the column mame."
  [^DictionaryProvider$MapDictionaryProvider dict-provider colname col]
  (let [dict (string-column->dict col)]
    (.put dict-provider ^Dictionary dict)
    {:encoding (.getEncoding dict)}))


(defn idx-col->field
  ^Field [dict-provider ^long idx col]
  (let [colmeta (meta col)
        nullable? (boolean
                   (or (:nullable? colmeta)
                       (not (.isEmpty
                             ^RoaringBitmap
                             (col-proto/missing col)))))
        col-dtype (:datatype colmeta)
        colname (:name colmeta)
        extra-data (merge (select-keys (meta col) [:timezone])
                          (when (= :string col-dtype)
                            (string-col->encoding dict-provider colname col)))]
    (try
      (make-field
       (ml-utils/column-safe-name colname)
       (datatype->field-type col-dtype nullable? colmeta extra-data))
      (catch Throwable e
        (throw (Exception. (format "Column %s metadata conversion failure:\n%s"
                                   colname e)
                           e))))))


(defn ds->arrow-schema
  [ds]
  (let [dict-provider (DictionaryProvider$MapDictionaryProvider.
                       (make-array Dictionary 0))]
    {:schema
     (Schema. ^Iterable
              (->> (ds-base/columns ds)
                   (map-indexed (partial idx-col->field dict-provider))))
     :dict-provider dict-provider}))


(defn as-roaring-bitmap
  ^RoaringBitmap [bmp] bmp)


(defn copy-column->arrow!
  ^FieldVector [col missing field-vec]
  (let [dtype (dtype/get-datatype col)]
    (if (or (= dtype :text)
            (= dtype :encoded-text))
      (strings->varchar! col missing field-vec)
      (do
        (when-not (instance? BaseFixedWidthVector field-vec)
          (throw (Exception. (format "Input is not a fixed-width vector"))))
        (let [n-elems (dtype/ecount col)
              ^BaseFixedWidthVector field-vec field-vec
              _ (do (.allocateNew field-vec n-elems)
                    (.setValueCount field-vec n-elems))
              data-buf (.getDataBuffer field-vec)
              valid-buf (.getValidityBuffer field-vec)
              data (if (= :string dtype)
                     (-> (ds-base/column->string-table col)
                         (str-table/indices))
                     col)]
          (missing->valid-buf missing valid-buf n-elems)
          (dtype/copy! data field-vec))))
    field-vec))


(defn ->timezone
  (^ZoneId [& [item]]
   (cond
     (instance? ZoneId item)
     item
     (string? item)
     (ZoneId/of ^String item)
     :else
     (dtype-dt/utc-zone-id))))


(defn datetime-cols-to-millis-from-epoch
  [ds {:keys [timezone]}]
  (let [timezone (->timezone timezone)]
    (reduce
     (fn [ds col]
       (let [col-dt (dtype-dt/unpack-datatype (dtype/get-datatype col))]
         (if (dtype-dt/datetime-datatype? col-dt)
           (let [timezone (if (or (= :local-date col-dt)
                              (= :local-date-time col-dt))
                            timezone
                            nil)]
             (assoc ds
                    (col-proto/column-name col)
                    (col-impl/new-column
                     (col-proto/column-name col)
                     (cond
                       (= :local-date col-dt)
                       (dtype-dt-ops/local-date->milliseconds-since-epoch col 0 timezone)
                       (= :local-date-time col-dt)
                       (dtype-dt-ops/local-date-time->milliseconds-since-epoch col timezone)
                       :else
                       (dtype-dt-ops/->milliseconds col))
                     (assoc (meta col)
                            :timezone (str timezone)
                            :source-datatype (dtype/get-datatype col))
                     (col-proto/missing col))))
           ds)))
     ds
     (ds-base/columns ds))))


(defn copy-ds->vec-root
  [^VectorSchemaRoot vec-root ds]
  (.setRowCount vec-root (ds-base/row-count ds))
  (->> (ds-base/columns ds)
       (map-indexed
        (fn [^long idx col]
          (let [field-vec (.getVector vec-root idx)
                coldata (dtype-dt/unpack col)
                col-type (dtype/get-datatype coldata)
                missing (col-proto/missing col)]
            (cond
              (= :string col-type)
              (-> (ds-base/column->string-table col)
                  (str-table/indices)
                  (copy-column->arrow! missing field-vec))
              :else
              (copy-column->arrow! coldata missing field-vec)))))
       (dorun)))


(defn write-dataset-to-stream!
  ([ds path options]
   (let [ds (ds-base/ensure-dataset-string-tables ds)
         ds (datetime-cols-to-millis-from-epoch ds options)
         {:keys [schema dict-provider]} (ds->arrow-schema ds)
         ^DictionaryProvider dict-provider dict-provider]
     (with-open [ostream (io/output-stream! path)
                 vec-root (VectorSchemaRoot/create
                           ^Schema schema
                           ^BufferAllocator (allocator))
                 writer (ArrowStreamWriter. vec-root dict-provider ostream)]
       (.start writer)
       (copy-ds->vec-root vec-root ds)
       (.writeBatch writer)
       (.end writer))))
  ([ds path]
   (write-dataset-to-stream! ds path {})))


(defn write-dataset-to-file!
  ([ds path options]
   (let [ds (ds-base/ensure-dataset-string-tables ds)
         ds (datetime-cols-to-millis-from-epoch ds options)
         {:keys [schema dict-provider]} (ds->arrow-schema ds)
         ^DictionaryProvider dict-provider dict-provider]
     (with-open [ostream (java.io.RandomAccessFile. ^String path "w")
                 vec-root (VectorSchemaRoot/create
                           ^Schema schema
                           ^BufferAllocator (allocator))
                 writer (ArrowFileWriter. vec-root dict-provider
                                          (.getChannel ostream))]
       (.start writer)
       (copy-ds->vec-root vec-root ds)
       (.writeBatch writer)
       (.end writer))))
  ([ds path]
   (write-dataset-to-file! ds path {})))


(defn field-vec->column
  [{:keys [fix-date-types?]
    :or {fix-date-types? true}}
   dict-map
   ^FieldVector fv]
  (let [field (.getField fv)
        n-elems (dtype/ecount fv)
        ft (.getFieldType field)
        encoding (.getDictionary ft)
        ^Dictionary dict (when encoding
                           (get dict-map (.getId encoding)))
        metadata (try (->> (.getMetadata field)
                           (map (fn [[k v]]
                                  [(edn/read-string k)
                                   (edn/read-string v)]))
                           (into {}))
                      (catch Throwable e
                        (throw
                         (Exception.
                          (format "Failed to deserialize metadata: %s\n%s"
                                  e
                                  (.getMetadata field))))))
        valid-buf (.getValidityBuffer fv)
        value-buf (.getDataBuffer fv)
        offset-buf (when (instance? BaseVariableWidthVector fv)
                     (.getOffsetBuffer fv))
        missing (valid-buf->missing valid-buf n-elems)
        coldata
        (cond
          dict
          (let [strs (dictionary->strings dict)
                n-dict-elems (dtype/ecount strs)
                data (DynamicIntList. (dtype/->list-backing-store (dtype/clone fv))
                                      nil)
                _ (when-not (== 0 (dtype/ecount data))
                    (.getInt data 0))
                n-table-elems (dtype/ecount strs)
                str->int (HashMap. n-table-elems)]
            (dotimes [idx n-table-elems]
              (.put str->int (.get strs idx) idx))
            (StringTable. strs str->int data))
          offset-buf
          (varchar->strings fv)
          ;;Mapping back to local-dates takes a bit of time.  This is only
          ;;necessary if you really need them.
          (and fix-date-types?
               (:timezone metadata)
               (:source-datatype metadata)
               (dtype-dt/datetime-datatype? (:source-datatype metadata))
               (not= (:source-datatype metadata) (dtype/get-datatype fv)))
          (let [src-dt (:source-datatype metadata)]
            (->
             (case (dtype-dt/unpack-datatype src-dt)
               :local-date
               (dtype-dt-ops/milliseconds-since-epoch->local-date
                fv (ZoneId/of (str (:timezone metadata))))
               :local-date-time
               (dtype-dt-ops/milliseconds-since-epoch->local-date-time
                fv (ZoneId/of (str (:timezone metadata))))
               (dtype-dt-ops/milliseconds->datetime src-dt fv))
             (#(if (dtype-dt/packed-datatype? src-dt)
                 (dtype-dt/pack %)
                 %))
             (dtype/clone)))
          :else
          (dtype/clone fv))]
    (col-impl/new-column (:name metadata) coldata metadata missing)))


(defn arrow->ds
  [ds-name ^VectorSchemaRoot schema-root dict-map options]
  (->> (.getFieldVectors schema-root)
       (map (partial field-vec->column options dict-map))
       (ds-impl/new-dataset ds-name)))


(defn read-stream-dataset-copying
  ([path options]
   (with-open [istream (io/input-stream path)
               reader (ArrowStreamReader. istream (allocator))]
     (when (.loadNextBatch reader)
       (arrow->ds path
                  (.getVectorSchemaRoot reader)
                  (.getDictionaryVectors reader)
                  options))))
  ([path]
   (read-stream-dataset-copying path {})))


(comment
  (require '[tech.ml.dataset :as ds])
  (def stocks (ds/->dataset "test/data/stocks.csv"))
  (write-dataset-to-stream! stocks "test.arrow" {:timezone "US/Eastern"})
  (def big-stocks (apply ds/concat-copying (repeat 10000 stocks)))
  (write-dataset-to-stream! big-stocks "big-stocks.feather")
  (write-dataset-to-file! big-stocks "big-stocks.file.feather")
  (io/put-nippy! "big-stocks.nippy" big-stocks)
  )
