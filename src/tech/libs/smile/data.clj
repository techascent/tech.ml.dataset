(ns tech.libs.smile.data
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.string-table :as str-table]
            [clojure.set :as set])
  (:import [tech.ml.dataset.impl.dataset Dataset]
           [smile.data DataFrame Tuple]
           [java.time.format DateTimeFormatter]
           [java.time LocalDate LocalTime LocalDateTime]
           [smile.math.matrix Matrix]
           [smile.data.type StructType StructField DataType DataType$ID DataTypes]
           [smile.data.vector BaseVector Vector BooleanVector ByteVector ShortVector
            IntVector LongVector FloatVector DoubleVector StringVector
            BooleanVectorImpl ByteVectorImpl ShortVectorImpl
            IntVectorImpl LongVectorImpl FloatVectorImpl DoubleVectorImpl
            StringVectorImpl VectorImpl]
           [smile.data.measure NominalScale DiscreteMeasure]
           [org.roaringbitmap RoaringBitmap]
           [java.util Collection List ArrayList]))


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


(defn column->field
  ^StructField [col]
  (StructField. (colname->str (ds-col/column-name col))
                (datatype->smile (dtype/get-datatype col))))


(defn dataset->schema
  ^StructType [ds]
  (->> ds
       (mapv column->field)
       (#(StructType. ^List %))))

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
    :object 'Vector))


(defn- as-string-array ^"[Ljava.lang.String;"[item] item)


(defn datatype->getter-name
  [datatype]
  (case datatype
    :boolean `getBoolean
    :int8 `getByte
    :int16 `getShort
    :int32 `getInt
    :int64 `getLong
    :float32 `getFloat
    :float64 `getDouble))


(declare column->smile)


(defmacro implement-smile-vector
  [datatype column col-rdr]
  `(let [column# ~column
         field# (column->field column#)
         col-rdr# (typecast/datatype->reader ~datatype ~col-rdr)
         ^RoaringBitmap missing# (ds-col/missing column#)]
     (reify
       ~(datatype->smile-vec-type datatype)
       (name [v#] (.name field#))
       (type [v#] (.type field#))
       (measure [v#] (.measure field#))
       (size [v#] (count column#))
       ;;Data is not always backed by array
       (array [v#] (typecast/datatype->array-cast-fn
                    ~datatype
                    (dtype/->array column#)))
       (toDoubleArray [v#] (typecast/as-double-array
                           (dtype/make-container :java-array :float64 column#)))
       (toIntArray [v#] (typecast/as-int-array
                        (dtype/make-container :java-array :int32 column#)))
       (toStringArray [v#] (as-string-array
                           (dtype/make-container :java-array :string column#)))
       (get [v# ^int i#]
         (when-not (.contains missing# i#)
           (col-rdr# i#)))
       (~(datatype->getter-name datatype) [v# i#] (.read col-rdr# i#))
       (^BaseVector get [v# ^"[I" indexes#]
        (column->smile (ds-col/select column# indexes#)))
       (stream [v#] (.typedStream col-rdr#)))))

(defn factorize-string-column
  ([column ^tech.v2.datatype.ObjectReader col-rdr ^DiscreteMeasure scale]
   (let [scale-size (.size scale)
         ^RoaringBitmap missing (ds-col/missing column)]
     (cond
       (< scale-size Byte/MAX_VALUE)
       (dtype/make-reader :int8 (dtype/ecount column)
                          (byte
                           (if (.contains missing idx)
                             -1
                             (.valueOf scale (.read col-rdr idx)))))

       (< scale-size Short/MAX_VALUE)
       (dtype/make-reader :int16 (dtype/ecount column)
                          (short
                           (if (.contains missing idx)
                             -1
                             (.valueOf scale (.read col-rdr idx)))))
       (< scale-size Integer/MAX_VALUE)
       (dtype/make-reader :int32 (dtype/ecount column)
                          (int
                           (if (.contains missing idx)
                             -1
                             (.valueOf scale (.read col-rdr idx)))))
       :else
       (dtype/make-reader :int64 (dtype/ecount column)
                          (long
                           (if (.contains missing idx)
                             -1
                             (.valueOf scale (.read col-rdr idx))))))))
  ([column]
   (let [rdr (dtype/->reader column)]
     (factorize-string-column column rdr (->> (ds-col/unique column)
                                              (into-array String)
                                              (as-string-array)
                                              (NominalScale.))))))

(defn string-vector
  ^StringVector [column col-rdr]
  (let [field (column->field column)
        col-rdr (typecast/datatype->reader :string col-rdr)
        ^RoaringBitmap missing (ds-col/missing column)]
    (reify
      StringVector
      (name [v] (.name field))
      (type [v] (.type field))
      (measure [v] (.measure field))
      (size [v] (count column))
      (anyNull [v] (not (.isEmpty missing)))
      (isNullAt [v i] (.contains missing i))
      (toArray [v] (typecast/datatype->array-cast-fn
                    :string
                    (dtype/->array column)))
      (toDoubleArray [v] (throw (Exception. "Unimplemented")))
      (toIntArray [v] (throw (Exception. "Unimplemented")))
      (toStringArray [v] (as-string-array
                          (dtype/make-container :java-array :string column)))
      (get [v ^int i]
        (when-not (.contains missing i)
          (col-rdr i)))
      (^BaseVector get [v ^"[I" indexes]
       (column->smile (ds-col/select column indexes)))
      (^Vector toDate [v ^DateTimeFormatter formatter]
       (-> (ds-col/parse-column [:local-date #(LocalDate/parse ^String % formatter)]
                                column)
           (column->smile)))
      (^Vector toTime [v ^DateTimeFormatter formatter]
       (-> (ds-col/parse-column [:local-time #(LocalTime/parse ^String % formatter)]
                                column)
           (column->smile)))
      (^Vector toDateTime [v ^DateTimeFormatter formatter]
       (-> (ds-col/parse-column [:local-date-time
                                 #(LocalDateTime/parse ^String % formatter)]
                                column)
           (column->smile)))
      (nominal [v]
        (->> (ds-col/unique column)
             (into-array String)
             (as-string-array)
             (NominalScale.)))
      (factorize [v scale]
        (factorize-string-column column col-rdr scale))
      (stream [v] (.typedStream col-rdr)))))


(defn object-vector
  ^Vector [column col-rdr]
  (let [field (column->field column)
        col-rdr (typecast/datatype->reader :object col-rdr)
        ^RoaringBitmap missing (ds-col/missing column)]
    (reify
      Vector
      (name [v] (.name field))
      (type [v] (.type field))
      (measure [v] (.measure field))
      (size [v] (count column))
      (anyNull [v] (not (.isEmpty missing)))
      (isNullAt [v i] (.contains missing i))
      ;;Data is not always backed by array
      (toArray [v] (dtype/->array-copy column))
      (toDoubleArray [v] (throw (Exception. "Unimplemented")))
      (toIntArray [v] (throw (Exception. "Unimplemented")))
      (toStringArray [v] (as-string-array
                          (dtype/make-container :java-array :string column)))
      (get [v ^int i]
        (when-not (.contains missing i)
          (col-rdr i)))
      (^BaseVector get [v ^"[I" indexes]
       (column->smile (ds-col/select column indexes)))
      (^Vector toDate [v]
       (when-not (= :local-date (dtype/get-datatype col-rdr))
         (throw (Exception. "Unimplemented")))
       v)
      (^Vector toTime [v]
       (when-not (= :local-time (dtype/get-datatype col-rdr))
         (throw (Exception. "Unimplemented")))
       v)
      (^Vector toDateTime [v]
       (when-not (= :local-date-time (dtype/get-datatype col-rdr))
         (throw (Exception. "Unimplemented"))))
      (stream [v] (.typedStream col-rdr)))))


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
      :string (string-vector col col-rdr)
      (object-vector col col-rdr))))


(declare dataset->smile smile-dataframe->dataset)


(defn factorize-dataset
  [ds]
  (->> ds
       (map (fn [col]
              (let [col-dtype (dtype/get-datatype col)]
                (cond
                  (= :string col-dtype)
                  (factorize-string-column col)
                  (or (= :local-time col-dtype)
                      (= :packed-local-time col-dtype))
                  (dtype-dt-ops/get-milliseconds col)
                  (dtype-dt/datetime-datatype? col-dtype)
                  (dtype-dt-ops/get-epoch-milliseconds col)
                  :else
                  col))))))


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
  (toArray [this]
    (->> (factorize-dataset ds)
         (map #(dtype/make-container :java-array :float64 %))
         (into-array)))
  (toMatrix [this]
    (let [retval (Matrix/of (ds/row-count ds) (ds/column-count ds) 0)]
      ;;Given the matrix is row major, we can just copy successive columns directly
      ;;into the data portion.
      (dtype/copy-raw->item! (factorize-dataset ds) (.data retval))
      retval))
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
   (let [retval (column->smile (nth ds idx))]
     (when-not (instance? Vector retval)
       (throw (Exception. "Datatype is not converted to vector")))
     retval))
  (^BooleanVector booleanVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :boolean (dtype/get-datatype col))
        (throw (Exception. "Column is not a boolean column")))
      (column->smile col)))
  (^ByteVector byteVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :int8 (dtype/get-datatype col))
        (throw (Exception. "Column is not a byte column")))
      (column->smile col)))
  (^ShortVector shortVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :int16 (dtype/get-datatype col))
        (throw (Exception. "Column is not a short column")))
      (column->smile col)))
  (^IntVector intVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :int32 (dtype/get-datatype col))
        (throw (Exception. "Column is not an int column")))
      (column->smile col)))
  (^LongVector longVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :int64 (dtype/get-datatype col))
        (throw (Exception. "Column is not a long column")))
      (column->smile col)))
  (^FloatVector floatVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :float32 (dtype/get-datatype col))
        (throw (Exception. "Column is not a float column")))
      (column->smile col)))
  (^DoubleVector doubleVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :float64 (dtype/get-datatype col))
        (throw (Exception. "Column is not a double column")))
      (column->smile col)))
  (^StringVector stringVector [this ^int col-num]
    (let [col (nth ds col-num)]
      (when-not (= :string (dtype/get-datatype col))
        (throw (Exception. "Column is not a string column")))
      (column->smile col)))
  (^DataFrame select [this ^ints col-indexes]
   (-> (ds/select-columns-by-index ds col-indexes)
       (dataset->smile)))
  (^DataFrame drop [this ^ints col-indexes]
   (let [col-indexes (set (map long col-indexes))]
     (->> (range (ds/column-count ds))
          (remove col-indexes)
          (ds/select-columns-by-index ds)
          (dataset->smile))))
  (^DataFrame merge [this ^"[Lsmile.data.DataFrame;" dataframes]
   (->> dataframes
        (map smile-dataframe->dataset)
        (reduce (fn [ds other-ds]
                  (ds/append-columns ds other-ds))
                ds)
        (dataset->smile)))
  (^DataFrame union [this ^"[Lsmile.data.DataFrame;" dataframes]
   (->> dataframes
        (map smile-dataframe->dataset)
        (apply ds/concat ds)))

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
  ^DataFrame [ds]
  (if (instance? DataFrame ds)
    ds
    (let [^RoaringBitmap ds-missing (if (== 0 (ds/column-count ds))
                                      (RoaringBitmap.)
                                      (reduce dtype/set-or
                                              (map ds-col/missing ds)))
          missing (mapv ds-col/missing ds)
          readers (mapv dtype/->reader ds)
          schema (dataset->schema ds)]
      (.runOptimize ds-missing)
      (SmileDataFrame. ds ds-missing missing readers schema))))


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
      (throw (Exception. "Unrecognized datatype"))))
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



(defn smile-dataframe->dataset
  ([df {:keys [unify-strings?]
        :or {unify-strings? true}}]
   (if (instance? SmileDataFrame df)
     (.ds ^SmileDataFrame df)
     (->> df
          (map (fn [smile-vec]
                 (if (and unify-strings?
                          (= :string (dtype/get-datatype smile-vec)))
                   (let [str-t (str-table/make-string-table (dtype/ecount smile-vec))]
                     (dtype/copy! smile-vec str-t)
                     str-t)
                   smile-vec)))
          (ds/new-dataset "_unnamed"))))
  ([df]
   (smile-dataframe->dataset df {})))
