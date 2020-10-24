(ns tech.v3.libs.arrow.schema
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [clojure.core.protocols :as clj-proto]
            [clojure.datafy :refer [datafy]])
  (:import  [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
            [org.apache.arrow.vector.types.pojo FieldType ArrowType Field Schema
             ArrowType$Int ArrowType$FloatingPoint ArrowType$Bool
             ArrowType$Utf8 ArrowType$LargeUtf8 ArrowType$Date ArrowType$Time
             ArrowType$Timestamp ArrowType$Duration DictionaryEncoding]
            [java.util Map]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


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
         datatype (packing/unpack-datatype datatype)]
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
       :epoch-milliseconds (ft-fn (ArrowType$Timestamp. TimeUnit/MILLISECOND
                                                        (str (:timezone extra-data))))
       :local-time (ft-fn (ArrowType$Time. TimeUnit/MILLISECOND (int 8)))
       :duration (ft-fn (ArrowType$Duration. TimeUnit/MICROSECOND))
       :instant (ft-fn (ArrowType$Timestamp. TimeUnit/MILLISECOND
                                             (str (:timezone extra-data))))
       :string (if-let [^DictionaryEncoding encoding (:encoding extra-data)]
                 (ft-fn (.getIndexType encoding) encoding)
                 ;;If no encoding is provided then just save the string as text
                 (ft-fn (ArrowType$Utf8.)))
       :text (ft-fn (ArrowType$Utf8.))
       :encoded-text (ft-fn (ArrowType$Utf8.))))))

(extend-protocol clj-proto/Datafiable
  ArrowType$Int
  (datafy [this]
    (let [signed? (.getIsSigned this)
          bit-width (.getBitWidth this)]
      {:datatype (if signed?
                   (case bit-width
                     8 :int8
                     16 :int16
                     32 :int32
                     64 :int64)
                   (case bit-width
                     8 :uint8
                     16 :uint16
                     32 :uint32
                     64 :uint64))}))
  ArrowType$Utf8
  (datafy [this]
    {:datatype :string
     :encoding :utf-8
     :offset-buffer-datatype :uint32})
  ArrowType$LargeUtf8
  (datafy [this]
    {:datatype :string
     :encoding :utf-8
     :offset-buffer-datatype :int64})
  ArrowType$FloatingPoint
  (datafy [this]
    {:datatype (condp = (.getPrecision this)
                 FloatingPointPrecision/HALF :float16
                 FloatingPointPrecision/SINGLE :float32
                 FloatingPointPrecision/DOUBLE :float64)})
  ArrowType$Timestamp
  (datafy [this]
    (merge
     (if (= (.getUnit this) TimeUnit/MILLISECOND)
       {:datatype :epoch-milliseconds}
       {:datatype :int64 :time-unit (condp = (.getUnit this)
                                      TimeUnit/MICROSECOND :microsecond
                                      TimeUnit/NANOSECOND :nanosecond
                                      TimeUnit/SECOND :second)})
     (when (and (.getTimezone this)
                (not= 0 (count (.getTimezone this))))
       {:timezone (.getTimezone this)})))
  ArrowType$Bool
  (datafy [this] {:datatype :boolean})
  DictionaryEncoding
  (datafy [this] {:id (.getId this)
                  :ordered? (.isOrdered this)
                  :index-type (datafy (.getIndexType this))}))
