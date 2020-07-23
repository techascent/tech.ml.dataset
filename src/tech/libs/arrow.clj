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
           [org.apache.arrow.vector.types TimeUnit FloatingPointPrecision DateUnit]
           [org.apache.arrow.vector.dictionary DictionaryProvider Dictionary]
           [org.apache.arrow.vector VarCharVector]
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
       (throw (Exception. "No allocator provided.  See ")))))
  (^BufferAllocator [options]
   (or (:allocator options) (allocator))))


(defmacro with-allocator
  "Bind a new allocator.  alloc* must be either an instance of
  org.apache.arrow.memory.BaseAllocator or an instance of IDeref that resolves to an
  instance of BaseAllocator."
  [alloc* & body]
  `(with-bindings {#'*allocator* alloc*}
     ~@body))


(defn arrow-varchar
  (^VarCharVector [& [{:keys [name minor-type nullable?]}]]
   (VarCharVector. ^String (or name "unnamed")
                   ^FieldType (or minor-type
                                  (datatype->field-type :text nullable?))
                   (allocator))))


(defn strings->arrow-varchar
  "take a reader of strings and produce an arrow varchar."
  ^VarCharVector [rdr & [missing-bitmap]]
  (let [n-elems (dtype/ecount rdr)
        rdr (dtype/->reader rdr)
        vec (doto (arrow-varchar)
              (.setInitialCapacity (dtype/ecount rdr))
              (.setValueCount (dtype/ecount rdr)))
        ^RoaringBitmap missing (or missing-bitmap (dtype/->bitmap-set))]
    (dotimes [idx n-elems]
      (if (.contains missing idx)
        (.setNull vec idx)
        (do
          (.setIndexDefined vec idx)
          (.setSafe vec idx (.getBytes ^String (rdr idx) "UTF-8")))))
    vec))


(defn reader->arrow
  "Copy a reader into an arrow vector type"
  [rdr & [missing]]
  (let [^RoaringBitmap missing (or missing (dtype/->bitmap-set))
        rdr (dtype/->reader (dtype-dt/unpack rdr))
        n-elems (dtype/ecount rdr)]
    (case (dtype/get-datatype rdr)

      )))


(defn string-column->dict-indices
  "Given a string column, return a map of {:dictionary :indices} which
  will be encoded according to the data in string-col->dict-id-table-width"
  [col]
  (let [str-t (ds-base/ensure-column-string-table col)
        indices (str-table/indices str-t)
        int->str (str-table/int->string str-t)
        arrow-int->str (strings->arrow-varchar int->str)]


    ))
