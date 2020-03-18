(ns tech.ml.dataset.impl.column
  (:require [tech.ml.protocols.column :as ds-col-proto]
            [tech.ml.dataset.string-table :refer [make-string-table]]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.parallel.for :as parallel-for])
  (:import [java.util ArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IPersistentMap IMeta]
           [tech.v2.datatype ObjectReader DoubleReader ObjectWriter]))

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
    :text ""
    :keyword nil
    :symbol nil}))


(defn make-container
  ([dtype n-elems]
   (case dtype
     :string (make-string-table n-elems "")
     :text (let [list-data (ArrayList.)]
             (dotimes [iter n-elems]
               (.add list-data ""))
             list-data)
     (dtype/make-container :list dtype n-elems)))
  ([dtype]
   (make-container dtype 0)))


(defmacro create-missing-reader
  [datatype missing data n-elems options]
  `(let [rdr# (typecast/datatype->reader ~datatype ~data (:unchecked ~options))
         missing# ~missing
         missing-val# (casting/datatype->cast-fn
                       :unknown ~datatype
                       (get @dtype->missing-val-map ~datatype))
         n-elems# (long ~n-elems)]
     (reify ~(typecast/datatype->reader-type datatype)
       (getDatatype [this#] (.getDatatype rdr#))
       (lsize [this#] n-elems#)
       (read [this# idx#]
         (if (.contains missing# idx#)
           missing-val#
           (.read rdr# idx#))))))


(defn create-string-text-missing-reader
  [^RoaringBitmap missing data n-elems options]
  (let [rdr (typecast/datatype->reader :object data)
         missing-val ""
         n-elems (long n-elems)]
    (reify ObjectReader
      (getDatatype [this] (.getDatatype rdr))
      (lsize [this] n-elems)
      (read [this idx]
        (if (.contains missing idx)
          missing-val
          (.read rdr idx))))))


(defn create-object-missing-reader
  [^RoaringBitmap missing data n-elems options]
  (let [rdr (typecast/datatype->reader :object data)
        missing-val nil
        n-elems (long n-elems)]
    (reify ObjectReader
      (getDatatype [this] (.getDatatype rdr))
      (lsize [this] n-elems)
      (read [this idx]
        (if (.contains missing idx)
          missing-val
          (.read rdr idx))))))


(defn ->persistent-map
  ^IPersistentMap [item]
  (if (instance? IPersistentMap item)
    item
    (into {} item)))


(deftype Column
    [^RoaringBitmap missing
     data
     ^IPersistentMap metadata]
  dtype-proto/PDatatype
  (get-datatype [this] (dtype-proto/get-datatype data))
  dtype-proto/PCountable
  (ecount [this] (dtype-proto/ecount data))
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (let [missing-policy (get options :missing-policy :include)
          n-missing (dtype/ecount missing)
          any-missing? (not= 0 n-missing)
          missing-policy (if-not any-missing?
                            :include
                            missing-policy)
          n-elems (dtype/ecount data)
          col-reader
          (if (or (= :elide missing-policy)
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
              (create-object-missing-reader missing data n-elems options)))
          new-reader (case missing-policy
                       :elide
                       (let [valid-indexes (->> (range (dtype/ecount data))
                                                (remove #(.contains missing (long %)))
                                                long-array)]
                         (indexed-rdr/make-indexed-reader valid-indexes
                                                          col-reader
                                                          {}))
                       :include
                       col-reader
                       :error
                       (if (not any-missing?)
                         col-reader
                         (throw (ex-info (format "Column has missing indexes: %s"
                                                 (vec missing))
                                         {}))))]
      (dtype-proto/->reader new-reader options)))
  dtype-proto/PToWriter
  (convertible-to-writer? [this] (dtype-proto/convertible-to-writer? data))
  (->writer [this options]
    (let [col-dtype (dtype/get-datatype data)
          options (update options :datatype
                          #(or % (dtype/get-datatype data)))
          data-writer (typecast/datatype->writer :object data)
          missing-val (get @dtype->missing-val-map col-dtype)
          n-elems (dtype/ecount data)]
      ;;writing to columns like this is inefficient due to the necessity to
      ;;keep the missing set accurate.  In most cases you are better off
      ;;simply creating a new column of some sort.
      (-> (reify ObjectWriter
            (lsize [this] n-elems)
            (write [this idx val]
              (locking this
                (if (or (nil? val)
                        (and (not (boolean? val))
                             (.equals ^Object val missing-val)))
                  (do
                    (.add missing idx)
                    (.write data-writer idx missing-val))
                  (do
                    (.remove missing idx)
                    (.write data-writer idx val))))))
          (dtype-proto/->writer options))))
  dtype-proto/PBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len (long len)]
      (let [new-missing (->> missing
                             (filter #(let [arg (long %)]
                                        (or (< arg offset)
                                            (>= (- arg offset) len))))
                             (map #(+ (long %) offset))
                             (->bitmap))
            new-data (dtype-proto/sub-buffer data offset len)]
        (Column. new-missing new-data metadata))))
  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (and (== 0 (dtype/ecount missing))
         (dtype-proto/convertible-to-nio-buffer? data)))
  (->buffer-backing-store [item]
    (dtype-proto/->buffer-backing-store data))
  ;;This also services to make concrete definitions of the data so this must
  ;;store the result realized.
  dtype-proto/PClone
  (clone [col datatype]
    (when-not (= datatype (dtype/get-datatype col))
      (throw (Exception. "Columns cannot clone to different types")))
    (let [new-data (if (dtype/writer? data)
                     (dtype/clone data)
                     ;;It is important that the result of this operation be writeable.
                     (dtype/make-container :java-array
                                           (dtype/get-datatype data) data))]
      (Column. (dtype/clone missing)
               new-data
               metadata)))
  dtype-proto/PPrototype
  (from-prototype [col datatype shape]
    (let [n-elems (long (apply * shape))]
      (Column. (->bitmap)
               (make-container datatype n-elems)
               {})))
  dtype-proto/PToArray
  (->sub-array [col]
    (when-let [data-ary (when (== 0 (dtype/ecount missing))
                          (dtype-proto/->sub-array data))]
      data-ary))
  (->array-copy [col] (dtype-proto/->array-copy (dtype/->reader col)))
  Iterable
  (iterator [col]
    (.iterator ^Iterable (dtype/->reader col)))

  ds-col-proto/PIsColumn
  (is-column? [this] true)
  ds-col-proto/PColumn
  (column-name [col] (:name metadata))
  (set-name [col name] (Column. missing data (assoc metadata :name name)))
  (supported-stats [col] dtype-fn/supported-descriptive-stats)
  (metadata [col]
    (merge metadata
           {:size (dtype/ecount col)
            :datatype (dtype/get-datatype col)}))
  (set-metadata [col data-map] (Column. missing data (->persistent-map data-map)))
  (missing [col] missing)
  (is-missing? [col idx] (.contains missing (long idx)))
  (set-missing [col long-rdr]
    (Column. (->bitmap long-rdr)
             data
             metadata))
  (unique [this] (set (dtype/->reader this)))
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
    (if (== 0 (dtype/ecount missing))
      ;;common case
      (Column. (->bitmap) (dtype/indexed-reader idx-rdr data) metadata)
      ;;Uggh.  Construct a new missing set
      (let [idx-rdr (typecast/datatype->reader :int64 idx-rdr)
            n-idx-elems (.lsize idx-rdr)
            ^RoaringBitmap result-set (->bitmap)]
        (parallel-for/serial-for
         idx
         n-idx-elems
         (when (.contains missing (.read idx-rdr idx))
           (.add result-set idx)))
        (Column. result-set
                 (dtype/indexed-reader idx-rdr data)
                 metadata))))
  (clone [col]
    (Column. (dtype/clone missing)
             (dtype/clone data)
             metadata))
  (to-double-array [col error-on-missing?]
    (let [n-missing (dtype/ecount missing)
          any-missing? (not= 0 n-missing)]
      (when (and any-missing? error-on-missing?)
        (throw (Exception. "Missing values detected and error-on-missing set")))
      (when-not (casting/numeric-type? (dtype/get-datatype col))
        (throw (Exception. "Non-numeric columns do not convert to doubles.")))
      (if (not any-missing?)
        (dtype/make-container :java-array :float64 col)
        (let [d-reader (typecast/datatype->reader :float64 data)]
          (dtype/make-container :java-array :float64
                                (dtype/->reader col :float64))))))
  IMeta
  (meta [this] metadata)
  Object
  (toString [item]
    (let [n-elems (dtype/ecount data)
          format-str (if (> n-elems 20)
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
   (when-not (or (nil? metadata)
                 (map? metadata))
     (throw (Exception. "Metadata must be a persistent map")))
   (let [missing (->bitmap missing)
         metadata (if (and (not (contains? metadata :categorical?))
                           (#{:string :keyword :symbol} (dtype/get-datatype data)))
                    (assoc metadata :categorical? true)
                    metadata)]
     (Column. missing data (assoc metadata :name name))))
  ([name data metadata]
   (new-column name data metadata (->bitmap)))
  ([name data]
   (new-column name data {} (->bitmap))))
