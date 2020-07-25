(ns tech.libs.arrow
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
            [tech.v2.datatype.typed-buffer :as typed-buf]
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
            TimeStampMilliVector FieldVector VectorSchemaRoot BaseVariableWidthVector]
           [io.netty.buffer ArrowBuf]
           [org.apache.arrow.vector.dictionary DictionaryProvider Dictionary
            DictionaryProvider$MapDictionaryProvider]
           [org.apache.arrow.vector.ipc ArrowStreamReader ArrowStreamWriter]
           [org.apache.arrow.vector.types Types]
           [org.roaringbitmap RoaringBitmap]
           [java.util Map ArrayList List HashMap]
           [tech.ml.dataset.impl.column Column]
           [tech.v2.datatype ObjectWriter]
           [tech.ml.dataset.string_table StringTable]
           [tech.ml.dataset.dynamic_int_list DynamicIntList]
           [java.time ZoneId]
           [java.nio Buffer ByteOrder]
           [tech.v2.datatype.typed_buffer TypedBuffer]))


;;TODO - check out gandiva


(set! *warn-on-reflection* true)


(defn arrow-buffer->typed-nio
  ^Buffer [datatype ^ArrowBuf buf]
  (let [nio-buf (-> (.nioBuffer buf)
                    (.order ByteOrder/LITTLE_ENDIAN))]
    (case datatype
      :int8 nio-buf
      :int16 (.asShortBuffer nio-buf)
      :int32 (.asIntBuffer nio-buf)
      :int64 (.asLongBuffer nio-buf)
      :float32 (.asFloatBuffer nio-buf)
      :float64 (.asDoubleBuffer nio-buf))))


(defn varchar->strings
  "copies the data into a list of strings."
  ^List [^VarCharVector fv]
  (let [n-elems (.getValueCount fv)
        value-buf (arrow-buffer->typed-nio :int8 (.getDataBuffer fv))
        offset-buf (arrow-buffer->typed-nio :int32 (.getOffsetBuffer fv))
        n-value-elems (dtype/ecount value-buf)
        offset-rdr (dtype/->reader offset-buf)
        retval (ArrayList. n-elems)]
    (.addAll retval
             (dtype/object-reader
              n-elems
              (fn [^long idx]
                (let [cur-offset (offset-rdr idx)
                      next-offset (offset-rdr (inc idx))]
                  (String. ^bytes (dtype/->array-copy
                                   (dtype/sub-buffer value-buf cur-offset
                                                     (- next-offset cur-offset))))))
              :string))
    retval))


(defn dictionary->strings
  ^List [^Dictionary dict]
  (varchar->strings (.getVector dict)))


(defn valid-buf->missing
  ^RoaringBitmap [^ArrowBuf buffer ^long n-elems]
  (let [nio-buf (typecast/datatype->reader :int8 (arrow-buffer->typed-nio :int8 buffer))
        ^RoaringBitmap missing (dtype/->bitmap-set)
        n-bytes (quot (+ n-elems 7) 8)]
    (dotimes [idx n-bytes]
      (let [offset (pmath/* 8 idx)
            data (unchecked-int (.read nio-buf idx))]
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


(defn bitwise-buf->booleans
  [^ArrowBuf data n-elems]
  (let [rdr (typecast/datatype->reader
             :int8
             (arrow-buffer->typed-nio :int8 data))
        n-elems (long n-elems)]
    (dtype/make-reader
     :boolean n-elems
     ;;idx is the indexing variable that is implicitly defined in scope.
     (let [byte-data (.read rdr (quot idx 8))]
       (if (pmath/== 1 (pmath/bit-and data (pmath/bit-shift-left 1 (rem idx 8))))
         true
         false)))))


(defn primitive-vec->typed-buffer
  [^FieldVector vvec]
  (let [data (.getDataBuffer vvec)]
    (case (dtype/get-datatype vvec)
      :boolean (bitwise-buf->booleans vvec (dtype/ecount vvec))
      :int8 (arrow-buffer->typed-nio :int8 data)
      :uint8 (TypedBuffer. :uint8 (arrow-buffer->typed-nio :int8 data))
      :int16 (arrow-buffer->typed-nio :int16 data)
      :uint16 (TypedBuffer. :uint16 (arrow-buffer->typed-nio :int16 data))
      :int32 (arrow-buffer->typed-nio :int32 data)
      :uint32 (TypedBuffer. :uint32 (arrow-buffer->typed-nio :int32 data))
      :int64 (arrow-buffer->typed-nio :int64 data)
      :uint64 (TypedBuffer. :uint64 (arrow-buffer->typed-nio :int64 data))
      :float32 (arrow-buffer->typed-nio :float32 data)
      :float64 (arrow-buffer->typed-nio :float64 data))))



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


(declare data->arrow-vector)


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
        varchar-vec (data->arrow-vector (dtype/->reader int->str :string)
                                        "unnamed" ftype nil)]
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


(defonce ^:dynamic *allocator* (delay (RootAllocator. Long/MAX_VALUE)))


(defn allocator
  (^BufferAllocator []
   (let [alloc-deref @*allocator*]
     (cond
       (instance? clojure.lang.IDeref alloc-deref)
       @alloc-deref
       (instance? BaseAllocator alloc-deref)
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
    :epoch-milliseconds `(as-timestamp-vector ~item)))


(defmacro make-arrow-vector-macro
  [datatype name field-type]
  (if-let [type-sym (datatype->vec-type-map datatype)]
    `(new ~(resolve (datatype->vec-type-map datatype))
          (str ~name)
          ~field-type
          (allocator))
    (throw (Exception. (format "Unable to find vec type for datatype %s"
                               datatype)))))


(defn make-arrow-vector
  [datatype name field-type]
  (case datatype
    :boolean (make-arrow-vector-macro :boolean name field-type)
    :uint8 (make-arrow-vector-macro :uint8 name field-type)
    :int8 (make-arrow-vector-macro :int8 name field-type)
    :uint16 (make-arrow-vector-macro :uint16 name field-type)
    :int16 (make-arrow-vector-macro :int16 name field-type)
    :uint32 (make-arrow-vector-macro :uint32 name field-type)
    :int32 (make-arrow-vector-macro :int32 name field-type)
    :uint64 (make-arrow-vector-macro :uint64 name field-type)
    :int64 (make-arrow-vector-macro :int64 name field-type)
    :float32 (make-arrow-vector-macro :float32 name field-type)
    :float64 (make-arrow-vector-macro :float64 name field-type)
    :string (make-arrow-vector-macro :string name field-type)
    :epoch-milliseconds (make-arrow-vector-macro :epoch-milliseconds name field-type)))


(defmacro implement-datatype-protos
  []
  `(do
     ~@(->> datatype->vec-type-map
            (map (fn [[dtype vectype]]
                   `(extend-type ~vectype
                      dtype-proto/PDatatype
                      (get-datatype [item#] ~dtype)
                      dtype-proto/PCountable
                      (ecount [item#] (.getValueCount item#))))))))



(defn as-roaring-bitmap
  ^RoaringBitmap [bmp] bmp)


(defmacro assign-arrow-vec
  [datatype rdr missing vec]
  `(let [~'rdr (dtype/->reader ~rdr ~datatype)
         n-elems# (dtype/ecount ~'rdr)
         missing# (as-roaring-bitmap (or ~missing (dtype/->bitmap-set)))
         nullable?# (not (.isEmpty missing#))
         ~'vec (doto (datatype->vec-type ~datatype ~vec)
                (.setInitialCapacity (dtype/ecount ~'rdr))
                (.setValueCount (dtype/ecount ~'rdr)))]
     (dotimes [~'idx n-elems#]
       (if (.contains missing# ~'idx)
         (.setNull ~'vec ~'idx)
         (do
           (.setIndexDefined ~'vec ~'idx)
           ~(cond
              (= :string datatype)
              `(.setSafe ~'vec ~'idx (.getBytes ^String (~'rdr ~'idx) "UTF-8"))
              (= :boolean datatype)
              `(.setSafe ~'vec ~'idx (casting/datatype->cast-fn
                                      :boolean :int8 (~'rdr ~'idx)))
              (or (= :int8 datatype)
                  (= :int16 datatype))
              `(.setSafe ~'vec ~'idx (casting/datatype->cast-fn
                                      ::unknown :int32 (~'rdr ~'idx)))
              (= :epoch-milliseconds datatype)
              `(.setSafe ~'vec ~'idx (casting/datatype->cast-fn
                                      :unknown :int64 (~'rdr ~'idx)))
              :else
              `(.setSafe ~'vec ~'idx (casting/datatype->cast-fn
                                      :unknown ~datatype (~'rdr ~'idx)))))))
     ~'vec))


(def base-convertible-datatypes
  [:uint8 :int8 :uint16 :int16 :uint32 :int32 :uint64 :int64
   :float32 :float64 :string :epoch-milliseconds])


(defmacro rdr-convert-fn-map-macro
  []
  (->> base-convertible-datatypes
       (map (fn [datatype]
              [datatype `(fn
                           ([rdr# missing# vec#]
                            (assign-arrow-vec ~datatype rdr# missing# vec#)))]))
       (into {})))


(def rdr-convert-map (rdr-convert-fn-map-macro))


(defn data->arrow-vector
  ^FieldVector [data name field-type missing]
  (let [data (dtype-dt/unpack data)
        datatype (dtype/get-datatype data)
        convert-fn (get rdr-convert-map datatype)]
    (when-not convert-fn
      (throw (Exception. "Failed to find conversion from reader type to arrow")))
    (convert-fn data missing (make-arrow-vector datatype name field-type))))


(defn copy-to-arrow!
  ^FieldVector [data missing field-vec]
  (let [data (dtype-dt/unpack data)
        datatype (dtype/get-datatype data)
        convert-fn (get rdr-convert-map datatype)]
    (when-not convert-fn
      (throw (Exception. "Failed to find conversion from reader type to arrow")))
    (convert-fn data missing field-vec)))


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
                            :timezone timezone
                            :source-datatype (dtype/get-datatype col))
                     (col-proto/missing col))))
           ds)))
     ds
     (ds-base/columns ds))))


(defn write-dataset!
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
                       (copy-to-arrow! missing field-vec))
                   :else
                   (copy-to-arrow! coldata missing field-vec)))))
            (dorun))
       (.writeBatch writer))))
  ([ds path]
   (write-dataset! ds path {})))


(defn field-vec->column
  [^FieldVector fv dict-map]
  (let [field (.getField fv)
        ft (.getFieldType field)
        encoding (.getDictionary ft)
        ^Dictionary dict (when encoding
                           (get dict-map (.getId encoding)))
        metadata (->> (.getMetadata field)
                      (map (fn [[k v]]
                             [(edn/read-string k)
                              (edn/read-string v)]))
                      (into {}))
        valid-buf (.getValidityBuffer fv)
        value-buf (.getDataBuffer fv)
        offset-buf (when (instance? BaseVariableWidthVector fv)
                     (.getOffsetBuffer fv))
        missing (valid-buf->missing valid-buf)
        coldata
        (cond
          dict
          (let [strs (dictionary->strings dict)
                n-dict-elems (dtype/ecount strs)
                data (-> (primitive-vec->typed-buffer (dtype/get-datatype fv) value-buf)
                         (dtype/clone))
                n-elems (dtype/ecount data)
                str->int (HashMap. (dtype/ecount strs))]
            (dotimes [idx n-elems]
              (.put str->int (.get strs idx) idx))
            (StringTable. strs str->int data))
          offset-buf
          (varchar->strings fv)
          :else
          (-> (primitive-vec->typed-buffer (dtype/get-datatype fv) value-buf)
              (dtype/clone)))]
    (col-impl/new-column (:name metadata) coldata metadata missing)))


(defn read-dataset
  [path]
  (with-open [istream (io/input-stream path)
              reader (ArrowStreamReader. istream (allocator))]
    (.loadNextBatch reader)
    (let [schema-root (.getVectorSchemaRoot reader)
          dict-map (.getDictionaryVectors reader)]
      (->> (.getFieldVectors schema-root)
           (map (fn [field-vec]
                  (case (dtype/get-datatype field-vec)

                    )
                  ))
           (ds-impl/new-dataset path)))))


(comment
  (require '[tech.ml.dataset :as ds])
  (def stocks (ds/->dataset "test/data/stocks.csv"))
  (write-dataset! stocks "test.arrow" {:timezone "US/Eastern"})
  )
