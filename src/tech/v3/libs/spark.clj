(ns tech.v3.libs.spark
  (:require [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.utils :as ds-utils]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [clojure.set :as set])
  (:import [org.apache.spark.sql Row]
           [org.apache.spark.sql.types StructType
            DataTypes]))

(comment
  (require '[zero-one.geni.core :as g])
  (require '[zero-one.geni.defaults :as geni-defaults])
  (def dataframe (-> (g/read-csv! "test/data/stocks.csv")
                     (g/limit 100)))
  (require '[tech.v3.dataset :as ds])
  (def stocks (ds/->dataset "test/data/stocks.csv"))
  (def session @geni-defaults/spark)
  )


(def ^:private datatype->sql-map
  {:boolean DataTypes/BooleanType
   :int8 DataTypes/ByteType
   :int16 DataTypes/ShortType
   :int32 DataTypes/IntegerType
   :int64 DataTypes/LongType
   :float32 DataTypes/FloatType
   :float64 DataTypes/DoubleType
   :local-date DataTypes/DateType
   :epoch-days DataTypes/DateType
   :string DataTypes/StringType
   :text DataTypes/StringType
   :instant DataTypes/TimestampType})


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


(defonce sql->datatype-map
  (set/map-invert datatype->sql-map))





(defn ds->schema
  ^StructType [ds]
  (let [retval (StructType.)]
    (reduce (fn [retval col]
              (let [{:keys [datatype name]} (meta col)
                    name (ds-utils/column-safe-name name)
                    nullable? (boolean (not (.isEmpty (ds-col/missing col))))]
                (.add ^StructType retval name (datatype->sql-type datatype) nullable?)))
            retval
            (vals ds))))
