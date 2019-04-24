(ns tech.libs.tablesaw.tablesaw-column
  "Bindings so that you can do math on tablesaw columns."
  (:require [tech.v2.datatype.base :as base]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.nio-buffer :as nio-buffer]
            [tech.v2.datatype.list :as dtype-list]
            [tech.ml.protocols.column :as col-proto]
            [clojure.core.matrix.protocols :as mp]
            [tech.ml.utils :refer [column-safe-name]]
            [tech.jna :as jna])
  (:import [tech.tablesaw.api ShortColumn IntColumn LongColumn
            FloatColumn DoubleColumn StringColumn BooleanColumn
            NumericColumn]
           [tech.v2.datatype ObjectReader ObjectWriter ObjectMutable]
           [tech.tablesaw.columns Column]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectArrayList]
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

(defn string-col-cast ^StringColumn [item] item)
(def string-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField StringColumn "data")
       (.setAccessible true)))))
(defn string-col-data
  ^ObjectArrayList [^StringColumn col]
  (when-not (instance? StringColumn col)
    (throw (ex-info "String data requested on object that is not a string column"
                    {:object-type (type col)})))
  (.get ^Field (string-data-field) col))

(defn boolean-col-cast ^BooleanColumn [item] item)
(def boolean-data-field
  (memoize
   (fn []
     (doto (.getDeclaredField BooleanColumn "data")
       (.setAccessible true)))))
(defn boolean-col-data
  ^ObjectArrayList [^BooleanColumn col]
  (when-not (instance? BooleanColumn col)
    (throw (ex-info "Boolean data requested on object that is not a boolean column"
                    {:object-type (type col)})))
  (.get ^Field (boolean-data-field) col))


(defmacro datatype->column-cast-fn
  [datatype item]
  (case datatype
    :int16 `(short-col-cast ~item)
    :int32 `(int-col-cast ~item)
    :int64 `(long-col-cast ~item)
    :float32 `(float-col-cast ~item)
    :float64 `(double-col-cast ~item)
    :string `(string-col-cast ~item)
    :boolean `(boolean-col-cast ~item)))


(defmacro datatype->column-data-cast-fn
  [datatype item]
  (case datatype
    :int16 `(short-col-data ~item)
    :int32 `(int-col-data ~item)
    :int64 `(long-col-data ~item)
    :float32 `(float-col-data ~item)
    :float64 `(double-col-data ~item)
    :boolean `(boolean-col-data ~item)
    :string `(string-col-data ~item)))


(def available-stats
  (set [:mean
        :variance
        :median
        :min
        :max
        :skew
        :kurtosis
        :geometric-mean
        :sum-of-squares
        :sum-of-logs
        :quadratic-mean
        :standard-deviation
        :population-variance
        :sum
        :product
        :quartile-1
        :quartile-3]))


(extend-type Column
  col-proto/PIsColumn
  (is-column? [this] true)
  col-proto/PColumn
  (column-name [col] (.name col))
  (supported-stats [col] available-stats)
  (metadata [col] {:name (col-proto/column-name col)
                   :size (mp/element-count col)
                   :datatype (base/get-datatype col)})

  (cache [this] {})
  (missing [col]
    (-> (.isMissing ^Column col)
        (.toArray)))
  (unique [col]
    (-> (.unique col)
        (.asList)
        set))

  (stats [col stats-set]
    (when-not (instance? NumericColumn col)
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (base/get-datatype col)
                       :column-name (col-proto/column-name col)})))
    (let [stats-set (set (if-not (seq stats-set)
                           available-stats
                           stats-set))
          missing-stats stats-set
          ^NumericColumn col col]
      (->> missing-stats
           (map (fn [skey]
                  [skey
                   (case skey
                     :mean (.mean col)
                     :variance (.variance col)
                     :median (.median col)
                     :min (.min col)
                     :max (.max col)
                     :skew (.skewness col)
                     :kurtosis (.kurtosis col)
                     :geometric-mean (.geometricMean col)
                     :sum-of-squares (.sumOfSquares col)
                     :sum-of-logs (.sumOfLogs col)
                     :quadratic-mean (.quadraticMean col)
                     :standard-deviation (.standardDeviation col)
                     :population-variance (.populationVariance col)
                     :sum (.sum col)
                     :product (.product col)
                     :quartile-1 (.quartile1 col)
                     :quartile-3 (.quartile3 col)
                     )]))
           (into {}))))

  (correlation
    [col other-column correlation-type]
    (let [^NumericColumn column (jna/ensure-type NumericColumn col)
          ^NumericColumn other-column (jna/ensure-type NumericColumn
                                                       (:col other-column))]
      (case correlation-type
        :pearson (.pearsons column other-column)
        :spearman (.spearmans column other-column)
        :kendall (.kendalls column other-column))))

  (column-values [this]
    (when-not (= 0 (base/ecount this))
      (or (dtype-proto/->array this)
          (dtype-proto/->array-copy this))))

  (is-missing? [col idx]
    (-> (.isMissing col)
        (.contains (int idx))))

  (select [col idx-seq]
    (let [^ints int-data (if (instance? (Class/forName "[I") idx-seq)
                           idx-seq
                           (int-array idx-seq))]
      ;;We can't cache much metadata now as we don't really know.
      (.subset col int-data)))


  (empty-column [this datatype elem-count metadata]
    (dtype-proto/make-container :tablesaw-column datatype
                                elem-count (merge metadata
                                                  {:empty? true})))


  (new-column [this datatype elem-count-or-values metadata]
    (dtype-proto/make-container :tablesaw-column datatype
                                elem-count-or-values metadata))


  (clone [this]
    (dtype-proto/make-container :tablesaw-column
                                (base/get-datatype this) (col-proto/column-values this)
                                {:column-name (col-proto/column-name this)}))


  (to-double-array [col error-missing?]
    (when (and error-missing?
               (> (.countMissing col) 0))
      (throw (ex-info (format "Missing values detected: %s - %s"
                              (col-proto/column-name col)
                              (.countMissing col)))))
    (base/copy! col (double-array (base/ecount col))))

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! (dtype/->reader raw-data)
                          ary-target
                          target-offset options))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [col options]
    (-> (reify ObjectReader
          (getDatatype [reader] (dtype-proto/get-datatype col))
          (lsize [reader] (base/ecount col))
          (read [reader idx] (.get col idx)))
        (dtype-proto/->reader options)))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [col options]
    (-> (reify ObjectWriter
          (getDatatype [writer] (dtype-proto/get-datatype col))
          (lsize [writer] (base/ecount col))
          (write [writer idx value] (.set col idx value)))
        (dtype-proto/->writer options)))


  dtype-proto/PToMutable
  (convertible-to-mutable? [item] true)
  (->mutable [col options]
    (-> (reify ObjectMutable
          (getDatatype [mut] (dtype-proto/get-datatype col))
          (lsize [mut] (base/ecount col))
          (insert [mut idx value]
            (when-not (= idx (.lsize mut))
              (throw (ex-info "Only insertion at the end of a column is supported." {})))
            (.append col value)))
        (dtype-proto/->mutable options)))


  dtype-proto/PPrototype
  (from-prototype [col datatype shape]
    (when-not (= 1 (count shape))
      (throw (ex-info "Base containers cannot have complex shapes"
                      {:shape shape})))
    (dtype-proto/make-container :tablesaw-column
                                datatype
                                (base/shape->ecount shape)
                                {:column-name (.name col)}))

  dtype-proto/PToArray
  (->sub-array [col] nil)
  (->array-copy [col] (.asObjectArray col))

  mp/PElementCount
  (element-count [col]
    (.size col)))


(defmacro extend-tablesaw-type
  [typename datatype]
  `(clojure.core/extend
       ~typename
     dtype-proto/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}

     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/raw-dtype-copy! (dtype-proto/as-list raw-data#)
                                               ary-target#
                                               target-offset# options#))}

     dtype-proto/PToNioBuffer
     {:convertible-to-nio-buffer? (fn [item#] (casting/numeric-type?
                                               (base/get-datatype item#)))
      :->buffer-backing-store (fn [item#]
                                (dtype-proto/->buffer-backing-store
                                 (dtype-proto/as-list item#)))}

     dtype-proto/PToList
     {:convertible-to-fastutil-list? (fn [item#] true)
      :->list-backing-store (fn [item#]
                              (datatype->column-data-cast-fn ~datatype item#))}


     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     (dtype-proto/->sub-array (dtype-proto/as-list item#)))
      :->array-copy (fn [item#]
                      (dtype-proto/->array-copy (dtype-proto/as-list item#)))}


     dtype-proto/PToReader
     {:convertible-to-reader? (constantly true)
      :->reader (fn [item# options#]
                  (dtype-proto/->reader (dtype-proto/as-list item#)
                                        options#))}

     dtype-proto/PToWriter
     {:convertible-to-reader? (constantly true)
      :->writer (fn [item# options#]
                  (dtype-proto/->writer (dtype-proto/as-list item#)
                                        options#))}

     dtype-proto/PToMutable
     {:convertible-to-reader? (constantly true)
      :->mutable (fn [item# options#]
                   (dtype-proto/->mutable (dtype-proto/as-list item#)
                                          options#))}

     dtype-proto/PBuffer
     {:sub-buffer
      (fn [item# offset# length#]
        (dtype-proto/sub-buffer (dtype-proto/as-list item#) offset# length#))}


     dtype-proto/PSetConstant
     {:set-constant! (fn [item# offset# value# elem-count#]
                       (dtype-proto/set-constant! (dtype-proto/as-list item#) offset#
                                                  value# elem-count#))}))


(extend-tablesaw-type ShortColumn :int16)
(extend-tablesaw-type IntColumn :int32)
(extend-tablesaw-type LongColumn :int64)
(extend-tablesaw-type FloatColumn :float32)
(extend-tablesaw-type DoubleColumn :float64)
(extend-tablesaw-type BooleanColumn :boolean)

(extend-type StringColumn
  dtype-proto/PDatatype
  (get-datatype [col] :string))



(defn make-column
  "Make a new tablesaw column.  Note that this does not make
  columns with missing values.  For that, use make-empty-column."
  ([datatype elem-count-or-seq {:keys [column-name]
                                :or {column-name "_unnamed"}
                                :as options}]
   (let [^String column-name (column-safe-name column-name)]
     ;;If numeric column, then use this pathway.
     (let [src-data (dtype-proto/make-container
                     :java-array datatype
                     elem-count-or-seq options)]
       (case datatype
         :int16 (ShortColumn/create column-name ^shorts src-data)
         :int32 (IntColumn/create column-name ^ints src-data)
         :int64 (LongColumn/create column-name ^longs src-data)
         :float32 (FloatColumn/create column-name ^floats src-data)
         :float64 (DoubleColumn/create column-name ^doubles src-data)
         :boolean (BooleanColumn/create column-name ^booleans src-data)
         :string (StringColumn/create column-name ^"[Ljava.lang.String;" src-data)))))
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


(defmethod dtype-proto/make-container :tablesaw-column
  [container-type datatype elem-count-or-seq
   {:keys [empty?] :as options}]
  (when (and empty?
             (not (number? elem-count-or-seq)))
    (throw (ex-info "Empty columns must have colsize argument." {})))
  (if empty?
    (make-empty-column datatype elem-count-or-seq options)
    (make-column datatype elem-count-or-seq options)))
