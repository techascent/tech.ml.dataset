(ns tech.ml.dataset.impl.column
  (:require [tech.ml.protocols.column :as ds-col-proto]
            [tech.ml.dataset.seq-of-maps :as ds-seq-of-maps]
            [tech.ml.dataset.string-table :refer [make-string-table]]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.parallel.for :as parallel-for]
            [clojure.set :as set])
  (:import [java.util Set SortedSet HashSet ArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [tech.v2.datatype ObjectReader DoubleReader]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def dtype->missing-val-map
  (atom
   {:boolean false
    :int16 Short/MIN_VALUE
    :int32 Integer/MIN_VALUE
    :int64 Long/MIN_VALUE
    :float32 Float/NaN
    :float64 Double/NaN
    :string ""
    :text ""}))


(defn make-container
  ([dtype n-elems]
   (case dtype
     :string (make-string-table n-elems)
     :text (let [list-data (ArrayList.)]
             (dotimes [iter n-elems]
               (.add list-data "")))
     (dtype/make-container :list dtype n-elems)))
  ([dtype]
   (make-container dtype 0)))


(defmacro create-missing-reader
  [datatype missing data n-elems options]
  `(let [rdr# (typecast/datatype->reader ~datatype ~data (:unchecked ~options))
         missing# ~missing
         missing-val# (casting/datatype->cast-fn :unknown ~datatype
                                                 (get @dtype->missing-val-map ~datatype))
         n-elems# (long ~n-elems)]
     (reify ~(typecast/datatype->reader-type datatype)
       (getDatatype [this#] (.getDatatype rdr#))
       (lsize [this#] n-elems#)
       (read [this# idx#]
         (if (contains? missing# idx#)
           missing-val#
           (.read rdr# idx#))))))


(defn create-string-text-missing-reader
  [missing data n-elems options]
  (let [rdr (typecast/datatype->reader :object data)
         missing-val ""
         n-elems (long n-elems)]
    (reify ObjectReader
      (getDatatype [this] (.getDatatype rdr))
      (lsize [this] n-elems)
      (read [this idx]
        (if (contains? missing idx)
          missing-val
          (.read rdr idx))))))


(deftype Column
    [^Set missing
     data
     ^long n-elems
     metadata]
  dtype-proto/PDatatype
  (get-datatype [this] (dtype-proto/get-datatype data))
  dtype-proto/PCountable
  (ecount [this] (dtype-proto/ecount data))
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (let [missing-policy (get options :missing-policy :include)
          any-missing? (not= 0 (.size missing))
          missing-policy (if-not any-missing?
                            :include
                            missing-policy)
          unchecked? (:unchecked? options)
          col-reader (if (or (= :elide missing-policy)
                             (not any-missing?))
                       (dtype/->reader data options)
                       (case (or (:datatype options)
                                 (dtype/get-datatype this))
                         :int16 (create-missing-reader :int16 missing data n-elems options)
                         :int32 (create-missing-reader :int32 missing data n-elems options)
                         :int64 (create-missing-reader :int64 missing data n-elems options)
                         :float32 (create-missing-reader :float32 missing data n-elems options)
                         :float64 (create-missing-reader :float64 missing data n-elems options)
                         :string (create-string-text-missing-reader missing data n-elems options)
                         :text (create-string-text-missing-reader missing data n-elems options)
                         (let [src-obj-rdr (typecast/datatype->reader :object data)]
                           (reify ObjectReader
                             (getDatatype [rdr] (.getDatatype src-obj-rdr))
                             (lsize [rdr] (.lsize src-obj-rdr))
                             (read [rdr idx]
                               (if (.contains missing idx)
                                 nil
                                 (.read src-obj-rdr idx)))))))
          new-reader (case missing-policy
                       :elide
                       (let [n-missing (.size missing)
                             valid-indexes (->> (range (dtype/ecount data))
                                                (remove #(.contains missing (long %)))
                                                long-array)]
                         (indexed-rdr/make-indexed-reader valid-indexes
                                                          col-reader
                                                          {}))
                       :include
                       col-reader
                       :error
                       (if (== 0 (.size missing))
                         col-reader
                         (throw (ex-info (format "Column has missing indexes: %s"
                                                 (vec missing))
                                         {}))))]
      (dtype-proto/->reader new-reader options)))
  dtype-proto/PBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len (long len)]
      (let [new-missing (->> missing
                             (filter #(let [arg (long %)]
                                        (or (< arg offset)
                                            (>= (- arg offset) len))))
                             (map #(+ (long %) offset))
                             (into #{}))
            new-data (dtype-proto/sub-buffer data offset len)]
        (Column. new-missing new-data len metadata))))
  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (and (== 0 (.size missing))
         (dtype-proto/convertible-to-nio-buffer? data)))
  (->buffer-backing-store [item]
    (dtype-proto/->buffer-backing-store data))

  dtype-proto/PToArray
  (->sub-array [col]
    (when (dtype-proto/convertible-to-nio-buffer? col)
      (let [col-buf (dtype-proto/->buffer-backing-store col)]
        (when (= (dtype/get-datatype col)
                 (dtype/get-datatype col-buf))
          (dtype-proto/->sub-array col-buf)))))
  (->array-copy [col] (dtype-proto/->array-copy col))

  ds-col-proto/PIsColumn
  (is-column? [this] true)
  ds-col-proto/PColumn
  (column-name [col] (:name metadata))
  (set-name [col name] (Column. missing data n-elems (assoc metadata :name name)))
  (supported-stats [col] dtype-fn/supported-descriptive-stats)
  (metadata [col] metadata)
  (set-metadata [col data-map] (Column. missing data n-elems data-map))
  (missing [col] missing)
  (is-missing? [col idx] (.contains missing (long idx)))
  (set-missing [col long-rdr]
    (Column. (set (dtype/->reader long-rdr))
             data
             n-elems
             metadata))
  (unique [this] (set (dtype-proto/->reader this {:missing-policy :elide})))
  (stats [col stats-set]
    (when-not (casting/numeric-type? (dtype-proto/get-datatype col))
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (dtype/get-datatype col)
                       :column-name (:name metadata)})))
    (dtype-fn/descriptive-stats (dtype/->reader
                                 col
                                 (dtype/get-datatype col)
                                 {:elide-missing? true})
                                stats-set))
  (correlation [col other-column correlation-type]
    (case correlation-type
      :pearson (dtype-fn/pearsons-correlation col other-column)
      :spearman (dtype-fn/spearmans-correlation col other-column)
      :kendall (dtype-fn/kendalls-correlation col other-column)))
  (select [col idx-rdr]
    (if (== 0 (.size missing))
      ;;common case
      (Column. missing (dtype/indexed-reader idx-rdr data) n-elems metadata)
      ;;Uggh.  Construct a new missing set
      (let [new-list (LongArrayList.)
            idx-rdr (typecast/datatype->reader :int64 idx-rdr)
            n-idx-elems (.lsize idx-rdr)
            result-set (HashSet.)]
        (parallel-for/serial-for
         idx
         n-idx-elems
         (when (.contains missing (.read idx-rdr idx))
           (.add result-set idx)))
        (Column. result-set (dtype/indexed-reader idx-rdr data) n-idx-elems metadata))))
  (new-column [col datatype elem-count-or-values missing-set metadata]
    (let [new-container (dtype/make-container :java-array datatype elem-count-or-values)]
      (Column. missing-set
               new-container
               (dtype/ecount new-container)
               metadata)))
  (clone [col]
    (Column. (set missing) (dtype/clone data) n-elems metadata))
  (to-double-array [col error-on-missing?]
    (when (and (not= 0 (.size missing))
               error-on-missing?)
      (throw (Exception. "Missing values detected and error-on-missing set")))
    (when-not (casting/numeric-type? (dtype/get-datatype col))
      (throw (Exception. "Non-numeric columns do not convert to doubles.")))
    (if (== 0 (.size missing))
      (dtype/make-container :java-array :float64 col)
      (let [d-reader (typecast/datatype->reader :float64 data)]
        (dtype/make-container :java-array :float64
                              (dtype/->reader col :float64)))))
  Object
  (toString [item]
    (let [format-str (if (> n-elems 20)
                       "#tech.ml.dataset.column<%s>%s\n%s\n[%s...]"
                       "#tech.ml.dataset.column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/get-datatype item))
              [n-elems]
              (ds-col-proto/column-name item)
              (-> (dtype/->reader item)
                  (dtype-proto/sub-buffer 0 (min 20 n-elems))
                  (dtype-pp/print-reader-data))))))


(defmethod print-method Column
  [col ^java.io.Writer w]
  (.write w (.toString ^Object col)))


(defn new-column
  "Given a map of (something convertible to a long reader) missing indexes,
  (something convertible to a reader) data
  and a (string or keyword) name, return an implementation of enough of the
  column and datatype protocols to allow efficient columnwise operations of
  the rest of tech.ml.dataset"
  ([name data metadata missing]
   (let [missing (if (instance? Set missing)
                   missing
                   (set missing))]
     (Column. missing data (dtype/ecount data) (assoc metadata :name name))))
  ([name data metadata]
   (new-column name data metadata #{}))
  ([name data]
   (new-column name data {} #{})))
