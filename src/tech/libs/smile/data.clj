(ns tech.libs.smile.data
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.string-table :as str-table]
            [clojure.set :as set])
  (:import [tech.ml.dataset.impl.dataset Dataset]
           [smile.data DataFrame DataFrameImpl DataFrameFactory]
           [java.time.format DateTimeFormatter]
           [java.time LocalDate LocalTime LocalDateTime]
           [smile.math.matrix Matrix]
           [smile.data.type StructType StructField DataType DataType$ID DataTypes]
           [smile.data.vector BaseVector Vector BooleanVector ByteVector ShortVector
            IntVector LongVector FloatVector DoubleVector StringVector
            BooleanVectorImpl ByteVectorImpl ShortVectorImpl
            IntVectorImpl LongVectorImpl FloatVectorImpl DoubleVectorImpl
            StringVectorImpl VectorImpl
            VectorFactory]
           [smile.data.measure NominalScale DiscreteMeasure]
           [org.roaringbitmap RoaringBitmap]
           [java.util Collection List ArrayList]
           [java.io Writer]))


(set! *warn-on-reflection* true)


(def datatype->smile-map
  {:boolean DataTypes/BooleanType
   :int8 DataTypes/ByteType
   :character DataTypes/CharType
   :int16 DataTypes/ShortType
   :int32 DataTypes/IntegerType
   :int64 DataTypes/LongType
   :float32 DataTypes/FloatType
   :float64 DataTypes/DoubleType
   :decimal DataTypes/DecimalType
   :string DataTypes/StringType
   :local-date DataTypes/DateType
   :local-time DataTypes/TimeType
   :local-date-time DataTypes/DateTimeType
   :packed-local-date DataTypes/DateType
   :packed-local-time DataTypes/TimeType
   :packed-local-date-time DataTypes/DateTimeType
   :object DataTypes/ObjectType})


(def smile->datatype-map
  (-> (set/map-invert datatype->smile-map)
      (assoc DataTypes/DateType :local-date
             DataTypes/TimeType :local-time
             DataTypes/DateTimeType :local-date-time)))


(defn datatype->smile
  ^DataType [dtype]
  (when dtype
    (get datatype->smile-map dtype DataTypes/ObjectType)))


(defn colname->str
  ^String [cname]
  (cond
    (string? cname) cname
    (keyword? cname) (name cname)
    (symbol? cname) (name cname)
    :else
    (str cname)))


(defn smile-struct-field
  ^StructField [name dtype]
  (StructField. (colname->str name)
                (datatype->smile dtype)))


(defn column->field
  ^StructField [col]
  (smile-struct-field (ds-col/column-name col)
                      (dtype/get-datatype col)))


(defn datatype->smile-vec-type
  "Datatype has to be a compile time constant"
  [datatype]
  (case (casting/safe-flatten datatype)
    :boolean 'BooleanVector
    :int8 'ByteVector
    :int16 'ShortVector
    :int32 'IntVector
    :int64 'LongVector
    :float32 'FloatVector
    :float64 'DoubleVector
    :string 'StringVector
    :object 'Vector))


(defn datatype->smile-vec-impl-type
  "Datatype has to be a compile time constant"
  [datatype]
  (case (casting/safe-flatten datatype)
    :boolean 'BooleanVectorImpl
    :int8 'ByteVectorImpl
    :int16 'ShortVectorImpl
    :int32 'IntVectorImpl
    :int64 'LongVectorImpl
    :float32 'FloatVectorImpl
    :float64 'DoubleVectorImpl
    :string 'StringVectorImpl
    'VectorImpl))


(defn column->smile
  ^BaseVector [col]
  (let [col-base-dtype (dtype/get-datatype col)
        col-data (if (dtype-dt/packed-datatype? col-base-dtype)
                   (dtype-dt/unpack col)
                   col)
        col-dtype (dtype/get-datatype col-data)
        col-ary (if-let [col-ary (dtype/->array col-data)]
                  col-ary
                  (dtype/make-container :java-array col-dtype col-data))
        field (column->field col)]
    (case (casting/safe-flatten col-dtype)
      :boolean (VectorFactory/booleanVector field ^booleans col-ary)
      :int8 (VectorFactory/byteVector field ^bytes col-ary)
      :int16 (VectorFactory/shortVector field ^shorts col-ary)
      :int32 (VectorFactory/intVector field ^ints col-ary)
      :int64 (VectorFactory/longVector field ^longs col-ary)
      :float32 (VectorFactory/floatVector field ^floats col-ary)
      :float64 (VectorFactory/doubleVector field ^doubles col-ary)
      :string (VectorFactory/stringVector field ^"[Ljava.lang.String;" col-ary)
      (VectorFactory/genericVector field col-ary))))


(defn dataset->dataframe
  "Convert a dataset to a smile.data DataFrame"
  ^DataFrame [ds]
  (if (instance? DataFrame ds)
    ds
    (DataFrameFactory/construct ^"[Lsmile.data.vector.BaseVector;"
                                (into-array BaseVector (map column->smile
                                                            (vals ds))))))


(defmacro construct-primitive-smile-vec
  [datatype name data]
  (case datatype
    :boolean `(BooleanVectorImpl. ~name (typecast/as-boolean-array ~data))
    :int8 `(ByteVectorImpl. ~name (typecast/as-byte-array ~data))
    :int16 `(ShortVectorImpl. ~name (typecast/as-short-array ~data))
    :int32 `(IntVectorImpl. ~name (typecast/as-int-array ~data))
    :int64 `(LongVectorImpl. ~name (typecast/as-long-array ~data))
    :float32 `(FloatVectorImpl. ~name (typecast/as-float-array ~data))
    :float64 `(DoubleVectorImpl. ~name (typecast/as-double-array ~data))))


(extend-type BaseVector
  dtype-proto/PDatatype
  (get-datatype [v]
    (if-let [retval (get smile->datatype-map (.type v))]
      retval
      :object))
  dtype-proto/PCountable
  (ecount [v] (long (.size v))))


(defmacro extend-primitive-vec
  [datatype]
  `(extend-type ~(datatype->smile-vec-impl-type datatype)
     dtype-proto/PDatatype
     (get-datatype [v#] ~datatype)
     dtype-proto/PToArray
     (->sub-array [v#]
       {:java-array (.array v#)
        :offset 0
        :length (dtype/ecount (.array v#))})
     (->array-copy [v#]
       (dtype/clone (.array v#)))
     dtype-proto/PToNioBuffer
     (convertible-to-nio-buffer? [v#] true)
     (->buffer-backing-store [v#]
       (dtype-proto/->buffer-backing-store (.array v#)))
     dtype-proto/PBuffer
     (sub-buffer [v# offset# length#]
       (dtype-proto/sub-buffer (.array v#) offset# length#))
     dtype-proto/PClone
     (clone [v#]
       (construct-primitive-smile-vec
        ~datatype (StructField. (.name v#) (.type v#) (.measure v#))
        (dtype/clone (.array v#))))
     dtype-proto/PToReader
     (convertible-to-reader? [v#] true)
     (->reader [v# options#] (dtype-proto/->reader (.array v#) options#))
     dtype-proto/PToWriter
     (convertible-to-writer? [v#] true)
     (->writer [v# options#] (dtype-proto/->writer (.array v#) options#))))


(extend-primitive-vec :boolean)
(extend-primitive-vec :int8)
(extend-primitive-vec :int16)
(extend-primitive-vec :int32)
(extend-primitive-vec :int64)
(extend-primitive-vec :float32)
(extend-primitive-vec :float64)


(extend-type VectorImpl
  dtype-proto/PToArray
  (->sub-array [v]
    {:java-array (.array v)
     :offset 0
     :length (dtype/ecount (.array v))})
  (->array-copy [v]
    (dtype/clone (.array v)))
  dtype-proto/PBuffer
  (sub-buffer [v offset length]
    (dtype-proto/sub-buffer (.array v) offset length))
  dtype-proto/PClone
  (clone [v]
    (let [new-field (StructField. (.name v) (.type v) (.measure v))]
      (if (= :string (dtype/get-datatype v))
        (StringVectorImpl. new-field ^"[Ljava.lang.String;" (dtype/clone (.array v)))
        (VectorImpl. new-field (.array v)))))
  dtype-proto/PToReader
  (convertible-to-reader? [v] true)
  (->reader [v options] (dtype-proto/->reader (.array v) options))
  dtype-proto/PToWriter
  (convertible-to-writer? [v#] true)
  (->writer [v options] (dtype-proto/->writer (.array v) options)))


(defn dataframe->dataset
  ([df {:keys [unify-strings?]
        :or {unify-strings? true}
        :as options}]
   (->> df
        (map (fn [^BaseVector smile-vec]
               {:name (.name smile-vec)
                :data
                (if (and unify-strings?
                         (= :string (dtype/get-datatype smile-vec)))
                  (let [col-rdr (dtype/->reader smile-vec)
                        str-rdr (dtype/object-reader
                                 (dtype/ecount col-rdr)
                                 #(let [read-val (col-rdr %)]
                                    (cond
                                      (string? read-val)
                                      read-val
                                      (nil? read-val)
                                      ""
                                      :else
                                      (throw (Exception.
                                              (format "Value not a string: %s"
                                                      read-val))))))]
                    (str-table/string-table-from-strings str-rdr))
                  smile-vec)}))
        (ds-impl/new-dataset options  {:name "_unnamed"})))
  ([df]
   (dataframe->dataset df {})))


(defmethod print-method DataFrameImpl
  [dataset w]
  (.write ^Writer w (.toString ^Object dataset)))
