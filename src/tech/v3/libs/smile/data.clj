(ns tech.libs.smile.data
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.string-table :as str-table]
            [clojure.set :as set])
  (:import [tech.v3.dataset.impl.dataset Dataset]
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
           [smile.data.measure NominalScale]
           [org.roaringbitmap RoaringBitmap]
           [java.util Collection List ArrayList]
           [java.io Writer]))


(set! *warn-on-reflection* true)


(def datatype->smile-map
  {:boolean DataTypes/BooleanType
   :int8 DataTypes/ByteType
   :char DataTypes/CharType
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
  (let [col-data (packing/unpack col)
        col-dtype (dtype/get-datatype col-data)
        col-ary (dtype-cmc/->array col-data)
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
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [v]
    (if-let [retval (get smile->datatype-map (.type v))]
      retval
      :object))
  dtype-proto/PECount
  (ecount [v] (long (.size v))))


(defmacro extend-primitive-vec
  [datatype]
  `(extend-type ~(datatype->smile-vec-impl-type datatype)
     dtype-proto/PElemwiseDatatype
     (elemwise-datatype [v#] ~datatype)
     dtype-proto/PToArrayBuffer
     (convertible-to-array-buffer? [V#] true)
     (->array-buffer [v#]
       (array-buffer/array-buffer (.array v#) (dtype-proto/elemwise-datatype v#)))
     dtype-proto/PClone
     (clone [v#]
       (construct-primitive-smile-vec
        ~datatype (StructField. (.name v#) (.type v#) (.measure v#))
        (dtype/clone (.array v#))))))


(extend-primitive-vec :boolean)
(extend-primitive-vec :int8)
(extend-primitive-vec :int16)
(extend-primitive-vec :int32)
(extend-primitive-vec :int64)
(extend-primitive-vec :float32)
(extend-primitive-vec :float64)


(extend-type VectorImpl
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [v] true)
  (->array-buffer [v]
    (array-buffer/array-buffer (.array v) (dtype-proto/elemwise-datatype v)))
  dtype-proto/PClone
  (clone [v]
    (let [new-field (StructField. (.name v) (.type v) (.measure v))]
      (if (= :string (dtype/get-datatype v))
        (StringVectorImpl. new-field ^"[Ljava.lang.String;" (dtype/clone (.array v)))
        (VectorImpl. new-field (.array v))))))


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
                  (let [str-rdr (dtype/emap #(when % (str %)) :string smile-vec)]
                    (str-table/string-table-from-strings str-rdr))
                  smile-vec)}))
        (ds-impl/new-dataset options  {:name "_unnamed"})))
  ([df]
   (dataframe->dataset df {})))


(defmethod print-method DataFrameImpl
  [dataset w]
  (.write ^Writer w (.toString ^Object dataset)))
