(ns tech.libs.smile.data
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [clojure.set :as set])
  (:import [tech.ml.dataset.impl.dataset Dataset]
           [smile.data DataFrame Tuple]
           [smile.data.type StructType StructField DataType DataType$ID DataTypes]
           [smile.data.vector BaseVector Vector BooleanVector ByteVector ShortVector
            IntVector LongVector FloatVector DoubleVector StringVector]
           [org.roaringbitmap RoaringBitmap]
           [java.util Collection List ArrayList]))


(set! *warn-on-reflection* true)


(def datatype->smile-map
  {:boolean DataTypes/BooleanType
   :int8 DataTypes/ByteType
   :character DataTypes/CharType
   :int16 DataTypes/ShortType,
   :int32 DataTypes/IntegerType,
   :int64 DataTypes/LongType,
   :float32 DataTypes/FloatType,
   :float64 DataTypes/DoubleType,
   :decimal DataTypes/DecimalType,
   :string DataTypes/StringType,
   :local-date DataTypes/DateType,
   :local-time DataTypes/TimeType,
   :local-date-time DataTypes/DateTimeType,
   :packed-local-date DataTypes/DateType,
   :packed-local-time DataTypes/TimeType,
   :packed-local-date-time DataTypes/DateTimeType,
   :object DataTypes/ObjectType})


(def smile->datatype-map (set/map-invert datatype->smile-map))


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


(defn column->field
  ^StructField [col]
  (StructField. (colname->str (ds-col/column-name col))
                (datatype->smile (dtype/get-datatype col))))


(defn dataset->schema
  ^StructType [ds]
  (->> ds
       (mapv column->field)
       (#(StructType. ^List %))))

(defmacro datatype->smile-vec-type
  "Datatype has to be a compile time constant"
  [datatype]
  (case (casting/safe-flatten datatype)
    :boolean `BooleanVector
    :int8 `ByteVector
    :int16 `ShortVector
    :int32 `IntegerVector
    :int64 `LongVector
    :float32 `FloatVector
    :float64 `DoubleVector
    :string `StringVector
    :object `Vector))


(defmacro implement-smile-vector
  [datatype column]
  `(let [field (column->field col)
        col-rdr (dtype/->reader col)
        ^RoaringBitmap missing (ds-col/missing col)]
    (reify BaseVector
      (name [v] (.name field))
      (type [v] (.type field))
      (measure [v] (.measure field))
      (size [v] (count col))
      ;;Data is not always backed by array
      (array [v] (dtype/->array col))
      (toDoubleArray [v] (dtype/make-container :java-array :float64 col))
      (toIntArray [v] (dtype/make-container :java-array :int32 col))
      (toStringArray [v] (dtype/make-container :java-array :string col))
      (get [v ^int i]
        (when-not (.contains missing i)
          (col-rdr i)))
      (^BaseVector get [v ^ints indexes]
       (column->smile (ds-col/select col indexes)))
      (getByte [v i]
        (.read (typecast/datatype->reader :int8 col-rdr) i))
      (getShort [v i]
        (.read (typecast/datatype->reader :int16 col-rdr) i))
      (getInt [v i]
        (.read (typecast/datatype->reader :int32 col-rdr) i))
      (getLong [v i]
        (.read (typecast/datatype->reader :int64 col-rdr) i))
      (getFloat [v i]
        (.read (typecast/datatype->reader :float32 col-rdr) i))
      (getDouble [v i]
        (.read (typecast/datatype->reader :float64 col-rdr) i))
      (stream [v]
        (.parallelStream ^Collection col-rdr)))))


(defn column->smile
  ^BaseVector [col]
  (let [col-base-dtype (dtype/get-datatype col)
        col-rdr (if (dtype-dt/packed-datatype? col-base-dtype)
                  (dtype-dt/unpack col)
                  (dtype/->reader col))
        col-dtype (dtype/get-datatype col-rdr)]
    (case (casting/safe-flatten col-dtype)
      :boolean (implement-smile-vector :boolean col col-rdr)
      :int8 (implement-smile-vector :int8 col col-rdr)
      :int16 (implement-smile-vector :int16 col col-rdr)
      :int32 (implement-smile-vector :int32 col col-rdr)
      :int64 (implement-smile-vector :int64 col col-rdr)
      :float32 (implement-smile-vector :float32 col col-rdr)
      :float64 (implement-smile-vector :float64 col col-rdr)
      ))
  (let [field (column->field col)
        col-rdr (dtype/->reader col)
        ^RoaringBitmap missing (ds-col/missing col)]

    (reify BaseVector
      (name [v] (.name field))
      (type [v] (.type field))
      (measure [v] (.measure field))
      (size [v] (count col))
      ;;Data is not always backed by array
      (array [v] (dtype/->array col))
      (toDoubleArray [v] (dtype/make-container :java-array :float64 col))
      (toIntArray [v] (dtype/make-container :java-array :int32 col))
      (toStringArray [v] (dtype/make-container :java-array :string col))
      (get [v ^int i]
        (when-not (.contains missing i)
          (col-rdr i)))
      (^BaseVector get [v ^ints indexes]
       (column->smile (ds-col/select col indexes)))
      (getByte [v i]
        (.read (typecast/datatype->reader :int8 col-rdr) i))
      (getShort [v i]
        (.read (typecast/datatype->reader :int16 col-rdr) i))
      (getInt [v i]
        (.read (typecast/datatype->reader :int32 col-rdr) i))
      (getLong [v i]
        (.read (typecast/datatype->reader :int64 col-rdr) i))
      (getFloat [v i]
        (.read (typecast/datatype->reader :float32 col-rdr) i))
      (getDouble [v i]
        (.read (typecast/datatype->reader :float64 col-rdr) i))
      (stream [v]
        (.parallelStream ^Collection col-rdr)))))


(declare dataset->smile)


(deftype SmileDataFrame [ds ^RoaringBitmap ds-missing
                         ^List missing
                         ^List readers
                         ^StructType schema]
  DataFrame
  (schema [this] schema)
  (stream [this]
    (-> (dtype/make-reader
         :object
         (ds/row-count ds)
         (.get this idx))
        (.parallelStream)))
  ;;Dataset<Tuple> Implementation
  (size [this] (int (ds/row-count ds)))
  (ncols [this] (int (ds/column-count ds)))
  (toArray [this])
  (toMatrix [this])
  (^boolean isNullAt [this ^int row-num ^int col-num]
   (.contains ^RoaringBitmap (.get missing col-num)
              row-num))
  (get [this ^int row-num ^int col-num]
    ;;Do not duplicate is nil check; code that cares will do above.
    ((.get readers col-num) row-num))

  (^boolean getBoolean [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :boolean (.get readers col-num))
          row-num))
  (^byte getByte [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :int8 (.get readers col-num))
          row-num))
  (^short getShort [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :int16 (.get readers col-num))
          row-num))
  (^int getInt [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :int32 (.get readers col-num))
          row-num))
  (^long getLong [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :int64 (.get readers col-num))
          row-num))
  (^float getFloat [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :float32 (.get readers col-num))
          row-num))
  (^double getDouble [tuple ^int row-num ^int col-num]
   (.read (typecast/datatype->reader :float64 (.get readers col-num))
          row-num))
  (get [this row-num]
    (let [col-count (ds/column-count ds)]
      (reify Tuple
        (schema [tuple] schema)
        (hasNull [tuple]
          (.contains ds-missing row-num))
        (toArray [tuple]
          (let [retval (double-array col-count)]
            (dotimes [col-num col-count]
              (if (.contains ^RoaringBitmap (.get missing col-num) col-num)
                (aset retval col-num (double
                                      ((.get readers col-num) row-num)))
                Double/NaN))
            retval))
        (^boolean isNullAt [tuple ^int col-num]
         (.isNullAt this row-num col-num))
        (^Object get [tuple ^int col-num]
         (.get this row-num col-num))
        (^boolean getBoolean [tuple ^int col-num]
         (.getBoolean this row-num col-num))
        (^byte getByte [tuple ^int col-num]
         (.getByte this row-num col-num))
        (^short getShort [tuple ^int col-num]
         (.getShort this row-num col-num))
        (^int getInt [tuple ^int col-num]
         (.getInt this row-num col-num))
        (^long getLong [tuple ^int col-num]
         (.getLong this row-num col-num))
        (^float getFloat [tuple ^int col-num]
         (.getFloat this row-num col-num))
        (^double getDouble [tuple ^int col-num]
         (.getDouble this row-num col-num)))))
  (columnIndex [this name]
    (.fieldIndex schema name))
  (^BaseVector column [this ^int idx]
   (column->smile (nth ds idx)))
  (^Vector vector [this ^int idx]

   )
  (^DataFrame of [this ^ints indexes]
      (dataset->smile (ds/select-rows ds indexes)))

  Iterable
  (iterator [this]
    (->> ds
         (map column->smile)
         (#(.iterator ^Iterable %))))
  Object
  (toString [ds] (.toString ^Object ds)))


(defn dataset->smile
  "Convert a dataset to a smile.data DataFrame"
  [ds]
  )
