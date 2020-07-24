(ns tech.libs.arrow
  (:require [tech.ml.dataset.base :as ds-base]
            [tech.ml.protocols.column :as col-proto]
            [tech.ml.dataset.dynamic-int-list :as int-list]
            [tech.ml.dataset.string-table :as str-table]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype :as dtype]
            [tech.ml.utils :as ml-utils])
  (:import [org.apache.arrow.vector.types.pojo FieldType ArrowType Field Schema
            ArrowType$Int ArrowType$FloatingPoint ArrowType$Bool
            ArrowType$Utf8 ArrowType$Date ArrowType$Time ArrowType$Timestamp
            ArrowType$Duration DictionaryEncoding]
           [org.apache.arrow.memory RootAllocator BaseAllocator BufferAllocator]
           [io.netty.buffer ArrowBuf]
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.vector.dictionary DictionaryProvider Dictionary]
           [org.apache.arrow.vector VarCharVector BitVector TinyIntVector UInt1Vector
            SmallIntVector UInt2Vector IntVector UInt4Vector BigIntVector UInt8Vector
            Float4Vector Float8Vector DateDayVector DateMilliVector TimeMilliVector
            DurationVector TimeStampMicroTZVector FieldVector]
           [org.apache.arrow.vector.types Types]
           [org.roaringbitmap RoaringBitmap]
           [java.util Map]
           [tech.ml.dataset.impl.column Column]
           [tech.v2.datatype ObjectWriter]
           [tech.ml.dataset.string_table StringTable]
           [tech.ml.dataset.dynamic_int_list DynamicIntList]))


(set! *warn-on-reflection* true)


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
     (case (casting/un-alias-datatype datatype)
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
       :local-date (ft-fn (ArrowType$Date. DateUnit/DAY))
       :local-date-time (ft-fn (ArrowType$Date. DateUnit/MILLISECOND))
       :local-time (ft-fn (ArrowType$Time. TimeUnit/MILLISECOND (int 64)))
       :duration (ft-fn (ArrowType$Duration. TimeUnit/MICROSECOND))
       :instance (ft-fn (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC"))
       :zoned-date-time (ft-fn (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC"))
       :offset-date-time (ft-fn (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC"))
       :string (let [{:keys [dict-id ordered? str-table-width]
                      :or {ordered? true
                           str-table-width 32}} extra-data
                     int-type (ArrowType$Int. (int str-table-width) true)]
                 (when-not dict-id
                   (throw (Exception. "String tables must have a dictionary id")))
                 (ft-fn int-type
                        (DictionaryEncoding. (int dict-id) (boolean ordered?)
                                             int-type)))
       :text (ft-fn (ArrowType$Utf8.))))))


(defmulti metadata->field-type
  "Convert column metadata into an arrow field"
  (fn [meta any-missing?]
    :datatype))


(defn string-col->dict-id-table-width
  "Given a string column return a map of :dict-id :table-width.  The dictionary
  id is the hashcode of the column mame."
  [col]
  {:dict-id (.hashCode ^Object (:name (meta col)))
   :str-table-width (-> (ds-base/column->string-table col)
                        (str-table/indices)
                        (dtype/get-datatype)
                        (casting/int-width))})


(defn idx-col->field
  ^Field [^long idx col]
  (let [colmeta (meta col)
        nullable? (boolean
                   (or (:nullable? colmeta)
                       (not (.isEmpty
                             ^RoaringBitmap
                             (col-proto/missing col)))))
        col-dtype (:datatype colmeta)
        colname (:name colmeta)
        extra-data (when (= :string col-dtype)
                     (string-col->dict-id-table-width col))]
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
  (Schema. ^Iterable
           (->> (ds-base/columns ds)
                (map-indexed idx-col->field))))


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
   :text 'VarCharVector})


(defmacro make-arrow-vector
  [datatype name field-type]
  (if-let [type-sym (datatype->vec-type-map datatype)]
    `(new ~(resolve (datatype->vec-type-map datatype))
          (str ~name)
          ~field-type
          (allocator))
    (throw (Exception. (format "Unable to find vec type for datatype %s"
                               datatype)))))


(defn as-roaring-bitmap
  ^RoaringBitmap [bmp] bmp)


(defmacro assign-arrow-vec
  [datatype rdr name field-type missing]
  `(let [~'rdr (dtype/->reader ~rdr ~datatype)
         n-elems# (dtype/ecount ~'rdr)
         missing# (as-roaring-bitmap (or ~missing (dtype/->bitmap-set)))
         nullable?# (not (.isEmpty missing#))
         ~'vec (doto (make-arrow-type ~datatype ~name ~field-type)
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
              :else
              `(.setSafe ~'vec ~'idx (casting/datatype->cast-fn
                                      :unknown ~datatype (~'rdr ~'idx)))))))
     ~'vec))


(def base-convertible-datatypes
  [:uint8 :int8 :uint16 :int16 :uint32 :int32 :uint64 :int64
   :float32 :float64 :string])


(defmacro rdr-convert-fn-map-macro
  []
  (->> base-convertible-datatypes
       (map (fn [datatype]
              [datatype `(fn [rdr# name# field-type# missing#]
                           (assign-arrow-vec ~datatype rdr# name#
                                             field-type# missing#))]))
       (into {})))


(def rdr-convert-map (rdr-convert-fn-map-macro))


(defn data->arrow-vector
  ^FieldVector [data name field-type missing]
  (let [data (dtype-dt/unpack data)
        datatype (dtype/get-datatype data)
        convert-fn (get rdr-convert-map datatype)]
    (when-not convert-fn
      (throw (Exception. "Failed to find conversion from reader type to arrow")))
    (convert-fn data name field-type missing)))


(defn string-column->dict-indices
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
                                        "unnamed" ftype nil)
        dict (Dictionary. varchar-vec encoding)
        indices-ftype (field-type true arrow-indices-type
                                  encoding (->str-str-meta metadata))]
    {:dictionary dict
     :indices (data->arrow-vector indices
                                  (ml-utils/column-safe-name colname)
                                  indices-ftype (col-proto/missing col))}))
