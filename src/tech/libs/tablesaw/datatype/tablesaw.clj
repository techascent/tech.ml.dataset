(ns tech.libs.tablesaw.datatype.tablesaw
  "Bindings so that you can do math on tablesaw columns."
  (:require [tech.datatype.base :as base]
            [tech.datatype.java-primitive :as primitive]
            [clojure.core.matrix.protocols :as mp]
            [tech.libs.tablesaw.datatype.fastutil :as dtype-fastutil])
  (:import [tech.tablesaw.api ShortColumn IntColumn LongColumn
            FloatColumn DoubleColumn StringColumn BooleanColumn]
           [tech.tablesaw.columns Column]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]
           [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [java.lang.reflect Field]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare make-column)


(defn short-col-cast ^ShortColumn [item] item)
(def short-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField ShortColumn "data")
       (.setAccessible true)))))
(defn short-col-data
  ^ShortArrayList [^ShortColumn col]
  (when-not (instance? ShortColumn col)
    (throw (ex-info "Short data requested on object that is not a short column"
                    {:object-type (type col)})))
  (.get ^Field (short-data-field) col))

(defn int-col-cast ^IntColumn [item] item)
(def int-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField IntColumn "data")
         (.setAccessible true)))))
(defn int-col-data
  ^IntArrayList [^IntColumn col]
  (when-not (instance? IntColumn col)
    (throw (ex-info "Int data requested on object that is not a int column"
                    {:object-type (type col)})))
  (.get ^Field (int-data-field) col))

(defn long-col-cast ^LongColumn [item] item)
(def long-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField LongColumn "data")
         (.setAccessible true)))))
(defn long-col-data
  ^LongArrayList [^LongColumn col]
  (when-not (instance? LongColumn col)
    (throw (ex-info "Long data requested on object that is not a long column"
                    {:object-type (type col)})))
  (.get ^Field (long-data-field) col))

(defn float-col-cast ^FloatColumn [item] item)
(def float-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField FloatColumn "data")
         (.setAccessible true)))))
(defn float-col-data
  ^FloatArrayList [^FloatColumn col]
  (when-not (instance? FloatColumn col)
    (throw (ex-info "Float data requested on object that is not a float column"
                    {:object-type (type col)})))
  (.get ^Field (float-data-field) col))

(defn double-col-cast ^DoubleColumn [item] item)
(def double-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField DoubleColumn "data")
         (.setAccessible true)))))
(defn double-col-data
  ^DoubleArrayList [^DoubleColumn col]
  (when-not (instance? DoubleColumn col)
    (throw (ex-info "Double data requested on object that is not a double column"
                    {:object-type (type col)})))
  (.get ^Field (double-data-field) col))






(defmacro datatype->column-cast-fn
  [datatype item]
  (case datatype
    :int16 `(short-col-cast ~item)
    :int32 `(int-col-cast ~item)
    :int64 `(long-col-cast ~item)
    :float32 `(float-col-cast ~item)
    :float64 `(double-col-cast ~item)))


(defmacro datatype->column-data-cast-fn
  [datatype item]
  (case datatype
    :int16 `(short-col-data ~item)
    :int32 `(int-col-data ~item)
    :int64 `(long-col-data ~item)
    :float32 `(float-col-data ~item)
    :float64 `(double-col-data ~item)))



(defmacro extend-tablesaw-type
  [typename datatype]
  `(clojure.core/extend
       ~typename
     base/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     base/PContainerType
     {:container-type (fn [_#] :typed-buffer)}
     base/PAccess
     {:get-value (fn [item# ^long idx#]
                   (.get (datatype->column-cast-fn ~datatype item#) idx#))
      :set-value! (fn [item# ^long offset# value#]
                    (.set (datatype->column-cast-fn ~datatype item#)
                          (int offset#)
                          (primitive/datatype->cast-fn :unused ~datatype value#))
                    item#)
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (base/set-constant! (primitive/->buffer-backing-store item#)
                                           offset# value# elem-count#))}

     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/copy-raw->item! raw-data# (primitive/->buffer-backing-store ary-target#)
                                               target-offset# options#))}
     base/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (when-not (= 1 (count shape#))
                          (throw (ex-info "Base containers cannot have complex shapes"
                                          {:shape shape#})))
                        (make-column datatype# (base/shape->ecount shape#)
                                     {:column-name (.name (datatype->column-cast-fn ~datatype src-ary#))}))}

     primitive/PToBuffer
     {:->buffer-backing-store (fn [item#]
                                (let [item# (datatype->column-cast-fn ~datatype item#)]
                                  (when (> (.countMissing item#) 0)
                                    (throw (ex-info "Datatype operations only work on columns with no missing data"
                                                    {:missing-count (.countMissing item#)})))
                                  (let [nio-buf# (primitive/datatype->buffer-cast-fn
                                                  ~datatype
                                                  (primitive/->buffer-backing-store
                                                   (datatype->column-data-cast-fn ~datatype item#)))]
                                    (.limit nio-buf# (+ (.position nio-buf#) (.size item#))))))}

     primitive/POffsetable
     {:offset-item (fn [src-buf# offset#]
                     (primitive/offset-item (primitive/->buffer-backing-store src-buf#)
                                            offset#))}

     primitive/PToArray
     {:->array (fn [item#]
                 (primitive/->array
                  (primitive/->buffer-backing-store item#)))
      :->array-copy (fn [item#]
                      (primitive/->array-copy
                       (primitive/->buffer-backing-store item#)))}

     mp/PElementCount
     {:element-count (fn [item#]
                       (.size (datatype->column-cast-fn ~datatype item#)))}))


(extend-tablesaw-type ShortColumn :int16)
(extend-tablesaw-type IntColumn :int32)
(extend-tablesaw-type LongColumn :int64)
(extend-tablesaw-type FloatColumn :float32)
(extend-tablesaw-type DoubleColumn :float64)

(defmacro extend-non-numeric-tablesaw-column
  [typename datatype coerce-fn]
  `(clojure.core/extend
       ~typename
     base/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     base/PContainerType
     {:container-type (fn [_#] :tablesaw-column)}
     base/PAccess
     {:get-value (fn [item# ^long idx#]
                   (.get ^Column item# idx#))
      :set-value! (fn [item# ^long offset# value#]
                    (.set ^Column item#
                          (int offset#)
                          (~coerce-fn value#))
                    item#)
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (doseq [idx# elem-count#]
                         (base/set-value! item# (+ (int offset#) (int idx#)) value#)))}
     base/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (when-not (= 1 (base/shape->ecount shape#))
                          (throw (ex-info "Base containers cannot have complex shapes"
                                          {:shape shape#})))
                        (make-column datatype# (base/shape->ecount shape#)
                                     {:column-name (.name ^Column src-ary#)}))}
     primitive/PToArray
     {:->array (fn [item#]
                 nil)
      :->array-copy (fn [item#]
                      (.asObjectArray ^Column item#))}

     mp/PElementCount
     {:element-count (fn [item#]
                       (.size ^Column item#))}))

(extend-non-numeric-tablesaw-column StringColumn :string str)
(extend-non-numeric-tablesaw-column BooleanColumn :boolean #(boolean %))


(defn generic-make-array
  [item-cls item-array-cls coerce-fn elem-count-or-seq]
  (cond
    (sequential? elem-count-or-seq)
    (into-array item-cls (map coerce-fn elem-count-or-seq))
    (number? elem-count-or-seq)
    (make-array item-cls (int elem-count-or-seq))
    (instance? item-array-cls elem-count-or-seq)
    elem-count-or-seq
    (.isArray ^Class (type elem-count-or-seq))
    (into-array item-cls (map coerce-fn elem-count-or-seq))))


(defn column-safe-name
  ^String [item-name]
  (if (and (or (keyword? item-name)
               (symbol? item-name))
           (namespace item-name))
    (str (namespace item-name) "/" (name item-name))
    (str item-name)))


(defn make-column
  "Make a new tablesaw column.  Note that this does not make
  columns with missing values.  For that, use make-empty-column."
  ([datatype elem-count-or-seq {:keys [column-name]
                                :or {column-name "_unnamed"}
                                :as options}]
   (let [^String column-name (column-safe-name column-name)]
     ;;If numeric column, then use this pathway.
     (if ((set primitive/datatypes) datatype)
       (let [src-data (if (and (satisfies? base/PDatatype elem-count-or-seq)
                               (satisfies? primitive/PToArray elem-count-or-seq)
                               (= (base/get-datatype elem-count-or-seq) datatype))
                        (or (primitive/->array elem-count-or-seq)
                            (primitive/->array-copy elem-count-or-seq))
                        (primitive/make-array-of-type datatype elem-count-or-seq options))]
         (case datatype
           :int16 (ShortColumn/create column-name ^shorts src-data)
           :int32 (IntColumn/create column-name ^ints src-data)
           :int64 (LongColumn/create column-name ^longs src-data)
           :float32 (FloatColumn/create column-name ^floats src-data)
           :float64 (DoubleColumn/create column-name ^doubles src-data)))
       (case datatype
         :string (let [str-data (generic-make-array String (Class/forName "[Ljava.lang.String;")
                                                    str elem-count-or-seq)]
                   (StringColumn/create ^String column-name ^"[Ljava.lang.String;" str-data))
         :boolean (let [bool-data (generic-make-array Boolean/TYPE (Class/forName "[Z")
                                                      boolean elem-count-or-seq)]
                    (BooleanColumn/create ^String column-name ^"[Z" bool-data))))))
  ([datatype elem-count-or-seq]
   (make-column datatype elem-count-or-seq {})))


(defn make-empty-column
  ([datatype elem-count {:keys [column-name]
                         :or {column-name "_unnamed"}}]
   (let [^String column-name (column-safe-name column-name)
         elem-count (int elem-count)]
     (case datatype
       :int16 (ShortColumn/create column-name elem-count)
       :int32 (IntColumn/create column-name elem-count)
       :int64 (LongColumn/create column-name elem-count)
       :float32 (FloatColumn/create column-name elem-count)
       :float64 (DoubleColumn/create column-name elem-count)
       :string (StringColumn/create column-name elem-count)
       :boolean (BooleanColumn/create column-name elem-count))))
  ([datatype elem-count]
   (make-empty-column datatype elem-count {}))
  ([datatype]
   (make-empty-column datatype 0 {})))
