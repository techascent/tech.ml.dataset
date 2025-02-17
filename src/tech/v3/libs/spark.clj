(ns tech.v3.libs.spark
  (:require [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.utils :as ds-utils]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.emap :as emap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [clojure.set :as set])
  (:import [org.apache.spark.sql Row RowFactory Dataset SparkSession]
           [org.apache.spark.sql.types StructType StructField
            DataTypes DataType]
           [tech.v3.datatype ObjectReader]
           [java.time LocalDate Instant]
           [java.util List]))


(set! *warn-on-reflection* true)


(def ^:private datatype->sql-map
  {:boolean DataTypes/BooleanType
   :int8 DataTypes/ByteType
   :int16 DataTypes/ShortType
   :int32 DataTypes/IntegerType
   :int64 DataTypes/LongType
   :float32 DataTypes/FloatType
   :float64 DataTypes/DoubleType
   :sql-date DataTypes/DateType
   :string DataTypes/StringType
   :text DataTypes/StringType
   :sql-timestamp DataTypes/TimestampType})


(defn datatype->sql-type
  ^DataType [datatype]
  (let [datatype (packing/unpack-datatype datatype)
        datatype (if-not (= datatype :epoch-days)
                   (let [temp-dt (casting/un-alias-datatype datatype)]
                     (if (casting/numeric-type? temp-dt)
                       (casting/safe-flatten temp-dt)
                       temp-dt))
                   datatype)
        sql-type (get datatype->sql-map datatype)
        _ (errors/when-not-errorf
           sql-type
           "Failed to convert datatype %s"
           datatype)]
    sql-type))


(def ^:private sql->datatype-map
  (set/map-invert datatype->sql-map))


(defn sql-type->datatype
  [sql-type]
  (if-let [retval (get sql->datatype-map sql-type)]
    retval
    (errors/throwf "Failed to find datatype for sql type %s"
                   sql-type)))

(defn sql-date->local-date
  ^java.time.LocalDate [^java.sql.Date date]
  (dtype-dt/milliseconds-since-epoch->local-date
   (.getTime date)))


(defn local-date->sql-date
  ^java.sql.Date [^LocalDate ld]
  (java.sql.Date/valueOf ld))


(casting/add-object-datatype! :sql-date java.sql.Date)


(defn sql-timestamp->instant
  ^Instant [^java.sql.Timestamp date]
  (dtype-dt/milliseconds-since-epoch->instant
   (.getTime date)))


(defn instant->sql-timestamp
  ^java.sql.Timestamp [^Instant inst]
  (java.sql.Timestamp.
   (dtype-dt/instant->milliseconds-since-epoch inst)))


(casting/add-object-datatype! :sql-timestamp java.sql.Timestamp)


(defn- prepare-ds-for-spark
  [ds]
  (reduce
   (fn [ds col]
     (let [cname (ds-utils/column-safe-name (ds-col/column-name col))
           col-dtype (packing/unpack-datatype
                      (dtype-base/elemwise-datatype col))
           missing (ds-col/missing col)
           epoch-types #{:epoch-milliseconds :epoch-seconds
                         :epoch-microseconds}
           coldata (cond
                     (epoch-types col-dtype)
                     (->> (dtype-dt/epoch->datetime :instant col)
                          (emap/emap instant->sql-timestamp :sql-timestamp))
                     (= col-dtype :local-date)
                     (emap/emap local-date->sql-date :sql-date
                                (packing/unpack col))
                     (= col-dtype :epoch-days)
                     (->> (dtype-dt/epoch->datetime :local-date col)
                          (emap/emap instant->sql-timestamp :sql-date))
                     (= col-dtype :text)
                     (emap/emap str :string col)
                     (and (casting/numeric-type? col-dtype)
                          (not= col-dtype (casting/safe-flatten col-dtype)))
                     (dtype-base/elemwise-cast col (casting/safe-flatten col-dtype))
                     :else
                     col)]
       (assoc ds cname (ds-col/new-column cname coldata (meta col) missing))))
   ds
   (vals ds)))


(defn ds-schema->spark-schema
  ^StructType [ds-schema]
  (let [retval (StructType.)]
    (reduce
     (fn [retval col]
       (let [{:keys [datatype name]} col
             name (ds-utils/column-safe-name name)
             nullable? (boolean (not (.isEmpty (ds-col/missing col))))]
         (.add ^StructType retval name (datatype->sql-type datatype) nullable?)))
     retval
     (:columns ds-schema))))


(defn ds-schema
  [ds]
  (assoc (meta ds)
         :columns (mapv meta (vals ds))))


(defn- dataset->row-list
  ^List [ds]
  (let [val-rdr (ds-readers/value-reader ds)
        n-elems (.lsize val-rdr)]
    (reify ObjectReader
      (lsize [rdr] n-elems)
      (readObject [rdr idx]
        (RowFactory/create
         (object-array (.readObject val-rdr idx)))))))


(defn ds->spark-dataset
  (^Dataset [ds ^SparkSession spark-session _options]
   ;;Prepare the dataset datatypes
   (let [ds (prepare-ds-for-spark ds)]
     (.createDataFrame spark-session (dataset->row-list ds)
                       (-> (ds-schema ds)
                           (ds-schema->spark-schema)))))
  (^Dataset [ds session]
   (ds->spark-dataset ds session nil)))





(defn collect-spark-dataset->ds
  [^Dataset dataset]
  ;;Make sure we are dealing with rows, not drama
  ;;This system will not be able to deal with missing
  ;;values without scanning each column.
  (let [dataset (.toDF dataset)
        schema (.schema dataset)
        ;;data is an array of rows
        ^objects data (.collect dataset)
        n-rows (alength data)]
    (->> (.fields schema)
         (map-indexed
          (fn [^long col-idx ^StructField field]
            (let [colname (.name field)
                  dtype (sql-type->datatype (.dataType field))]
              (ds-col/new-column colname
                                 (reify ObjectReader
                                   (elemwiseDatatype [rdr] dtype)
                                   (lsize [rdr] n-rows)
                                   (readObject [rdr row-idx]
                                     (let [^Row row (aget data row-idx)]
                                       (.get row col-idx))))
                                 nil
                                 []))))
         (ds-impl/new-dataset))))



(comment
  ;; databricks-connect specific classes
  ;; should work similar for spark-connect

;;Tested with hese deps
;;org.scala-lang/scala-reflect {:mvn/version "2.12.18"}
;;com.databricks/databricks-connect {:mvn/version "16.1.0"}
    

  (import
   '[com.databricks.connect DatabricksSession]
   '[com.databricks.sdk.core DatabricksConfig])
  
  (def config (.. (DatabricksConfig.) (setProfile "adb-xxxxx")))
  (def spark (.. (DatabricksSession/builder) (sdkConfig config) getOrCreate))

  (->
    (.sql spark "show catalogs;")
    collect-spark-dataset->ds)

  )