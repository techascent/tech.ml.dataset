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
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.roaringbitmap RoaringBitmap]
           [java.util Map]
           [tech.ml.dataset.impl.column Column]
           [tech.ml.dataset.string_table StringTable]
           [tech.ml.dataset.dynamic_int_list DynamicIntList]))


(set! *warn-on-reflection* true)


(defn make-field
  ^Field [^String name ^FieldType field-type]
  (Field. name field-type nil))


(defn field-type
  ^FieldType
  ([nullable? ^FieldType datatype ^DictionaryEncoding dict ^Map str-str-meta]
   (FieldType. (boolean nullable?) datatype dict str-str-meta))
  ([nullable? datatype str-meta]
   (field-type nullable? datatype nil str-meta))
  ([nullable? datatype]
   (field-type nullable? datatype nil)))


(defn datatype->field-type
  (^FieldType [datatype & [nullable? metadata extra-data]]
   (let [nullable? (or nullable? (= :object (casting/flatten-datatype datatype)))
         metadata (->> metadata
                       (map (fn [[k v]] [(pr-str k) (pr-str v)]))
                       (into {}))
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
                           str-table-width 32}} extra-data]
                 (when-not dict-id
                   (throw (Exception. "String tables must have a dictionary id")))
                 (ft-fn (ArrowType$Utf8.)
                        (DictionaryEncoding. (int dict-id) (boolean ordered?)
                                             (ArrowType$Int.
                                              (int str-table-width) true))))
       :text (ft-fn (ArrowType$Utf8.))))))


(defmulti metadata->field-type
  "Convert column metadata into an arrow field"
  (fn [meta any-missing?]
    :datatype))


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
        extra-data
        (when (= :string col-dtype)
          (let [^StringTable str-table (.data ^Column col)]
            (when-not (instance? StringTable str-table)
              (throw (Exception.
                      "string column types must have string tables")))
            {:dict-id (.hashCode ^Object (:name colmeta))
             :str-table-width (-> (.data str-table)
                                  (int-list/int-list->data)
                                  (dtype/get-datatype)
                                  (casting/int-width))}))]
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
