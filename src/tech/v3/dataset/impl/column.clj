(ns ^:no-doc tech.v3.dataset.impl.column
  (:require [tech.v3.protocols.column :as ds-col-proto]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.const-reader :as const-rdr]
            [tech.v3.dataset.string-table :refer [make-string-table]]
            [tech.v3.dataset.parallel-unique :refer [parallel-unique]]
            [tech.v3.parallel.for :as parallel-for])
  (:import [java.util ArrayList HashSet Collections Set List Map]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IPersistentMap IMeta Counted IFn IObj Indexed]
           [tech.v3.datatype PrimitiveIO ListPersistentVector]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare new-column)


(def ^Map dtype->missing-val-map
  {:boolean false
   :int8 Byte/MIN_VALUE
   :int16 Short/MIN_VALUE
   :int32 Integer/MIN_VALUE
   :int64 Long/MIN_VALUE
   :float32 Float/NaN
   :float64 Double/NaN
   :packed-instant (packing/pack (dtype-dt/milliseconds-since-epoch->instant 0))
   :packed-local-date (packing/pack (dtype-dt/milliseconds-since-epoch->local-date 0))
   :packed-duration 0
   :instant (dtype-dt/milliseconds-since-epoch->instant 0)
   :zoned-date-time (dtype-dt/milliseconds-since-epoch->zoned-date-time 0)
   :local-date-time (dtype-dt/milliseconds-since-epoch->local-date-time 0)
   :local-date (dtype-dt/milliseconds-since-epoch->local-date 0)
   :local-time (dtype-dt/milliseconds->local-time 0)
   :duration (dtype-dt/milliseconds->duration 0)
   :string ""
   :text ""
   :keyword nil
   :symbol nil})


(defn datatype->missing-value
  [dtype]
  (let [dtype (if (packing/packed-datatype? dtype)
                dtype
                (casting/un-alias-datatype dtype))]
    (if (contains? dtype->missing-val-map dtype)
      (get dtype->missing-val-map dtype)
      nil)))


(defn make-container
  ([dtype n-elems]
   (case dtype
     :string (make-string-table n-elems "")
     :text (let [^List list-data (dtype/make-container :list :string 0)]
             (dotimes [iter n-elems]
               (.add list-data ""))
             list-data)
     (dtype/make-container :list dtype n-elems)))
  ([dtype]
   (make-container dtype 0)))


(defn ->persistent-map
  ^IPersistentMap [item]
  (if (instance? IPersistentMap item)
    item
    (into {} item)))


(defn- ->efficient-reader
  [item]
  (cond
    (instance? RoaringBitmap item)
    (bitmap/bitmap->efficient-random-access-reader item)
    (dtype-proto/convertible-to-reader? item)
    item
    :else
    (long-array item)))


(defn- make-primitive-io
  [^RoaringBitmap missing data]
  (let [^PrimitiveIO src (dtype-proto/->primitive-io data)
        dtype (.elemwiseDatatype src)]
    (if (.isEmpty missing)
      src
      (reify PrimitiveIO
        (elemwiseDatatype [this] dtype)
        (lsize [this] (.lsize src))
        (allowsRead [this] (.allowsRead src))
        (allowsWrite [this] (.allowsWrite src))
        (readBoolean [this idx]
          (if (.contains missing idx)
            false
            (.readBoolean src idx)))
        (readByte [this idx]
          (if (.contains missing idx)
            Byte/MIN_VALUE
            (.readByte src idx)))
        (readShort [this idx]
          (if (.contains missing idx)
            Short/MIN_VALUE
            (.readShort src idx)))
        (readChar [this idx]
          (if (.contains missing idx)
            Character/MAX_VALUE
            (.readChar src idx)))
        (readInt [this idx]
          (if (.contains missing idx)
            Integer/MIN_VALUE
            (.readInt src idx)))
        (readLong [this idx]
          (if (.contains missing idx)
            Long/MIN_VALUE
            (.readLong src idx)))
        (readFloat [this idx]
          (if (.contains missing idx)
            Float/NaN
            (.readFloat src idx)))
        (readDouble [this idx]
          (if (.contains missing idx)
            Double/NaN
            (.readDouble src idx)))
        (readObject [this idx]
          (if (.contains missing idx)
            (get dtype->missing-val-map dtype)
            (.readObject src idx)))
        (writeBoolean [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeBoolean src idx val))
        (writeByte [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeByte src idx val))
        (writeShort [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeShort src idx val))
        (writeChar [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeChar src idx val))
        (writeInt [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeInt src idx val))
        (writeLong [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeLong src idx val))
        (writeFloat [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeFloat src idx val))
        (writeDouble [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeDouble src idx val))
        (writeObject [this idx val]
          (.remove missing (unchecked-inc idx))
          (.writeObject src idx val))))))


(defmacro cached-vector! []
  `(or ~'cached-vector
       (do (set! ~'cached-vector
                 (ListPersistentVector.
                  (make-primitive-io ~'missing ~'data)))
           ~'cached-vector)))

(deftype Column
    [^RoaringBitmap missing
     data
     ^IPersistentMap metadata
     ^:unsynchronized-mutable ^ListPersistentVector cached-vector]
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (and (.isEmpty missing)
         (dtype-proto/convertible-to-array-buffer? data)))
  (->array-buffer [this]
    (dtype-proto/->array-buffer data))

  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (and (.isEmpty missing)
         (dtype-proto/convertible-to-native-buffer? data)))
  (->native-buffer [this]
    (dtype-proto/->native-buffer data))
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [this] (dtype-proto/elemwise-datatype data))
  dtype-proto/PElemwiseCast
  (elemwise-cast [this new-dtype]
    (Column. missing
             (dtype-proto/elemwise-cast data new-dtype)
             metadata
             nil))
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [this] true)
  (->primitive-io [this]
    (cached-vector!)
    (.data cached-vector))
  dtype-proto/PToReader
  (convertible-to-reader? [this]
    (dtype-proto/convertible-to-reader? data))
  (->reader [this]
    (dtype-proto/->primitive-io this))
  dtype-proto/PToWriter
  (convertible-to-writer? [this]
    (dtype-proto/convertible-to-writer? data))
  (->writer [this]
    (dtype-proto/->primitive-io this))
  dtype-proto/PBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len (long len)]
      ;;TODO - use bitmap operations to perform this calculation
      (let [new-missing (->> missing
                             (filter #(let [arg (long %)]
                                        (or (< arg offset)
                                            (>= (- arg offset) len))))
                             (map #(+ (long %) offset))
                             (->bitmap))
            new-data (dtype-proto/sub-buffer data offset len)]
        (Column. new-missing new-data metadata nil))))
  dtype-proto/PClone
  (clone [col]
    (let [new-data (if (or (dtype/writer? data)
                           (= :string (dtype/get-datatype data))
                           (= :encoded-text (dtype/get-datatype data)))
                     (dtype/clone data)
                     ;;It is important that the result of this operation be writeable.
                     (dtype/make-container :jvm-heap
                                           (dtype/get-datatype data) data))]
      (Column. (dtype/clone missing)
               new-data
               metadata
               nil)))
  Iterable
  (iterator [this]
    (.iterator ^PrimitiveIO (dtype-proto/->primitive-io this)))
  ds-col-proto/PIsColumn
  (is-column? [this] true)
  ds-col-proto/PColumn
  (column-name [col] (:name metadata))
  (set-name [col name] (Column. missing data (assoc metadata :name name)
                                cached-vector))
  (supported-stats [col] stats/all-descriptive-stats-names)
  (missing [col] missing)
  (is-missing? [col idx] (.contains missing (long idx)))
  (set-missing [col long-rdr]
    (let [long-rdr (if (dtype/reader? long-rdr)
                     long-rdr
                     ;;handle infinite seq's
                     (take (dtype/ecount data) long-rdr))
          bitmap (->bitmap long-rdr)]
      (.runOptimize bitmap)
      (Column. bitmap
               data
               metadata
               nil)))
  (unique [this]
    (->> (parallel-unique this)
         (into #{})))
  (stats [col stats-set]
    (when-not (casting/numeric-type? (dtype-proto/elemwise-datatype col))
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (dtype/get-datatype col)
                       :column-name (:name metadata)})))
    (dfn/descriptive-statistics (dtype/->reader
                                 col
                                 (dtype/get-datatype col)
                                 {:missing-policy :elide})
                                stats-set))
  (correlation [col other-column correlation-type]
    (case correlation-type
      :pearson (dfn/pearsons-correlation col other-column)
      :spearman (dfn/spearmans-correlation col other-column)
      :kendall (dfn/kendalls-correlation col other-column)))
  (select [col idx-rdr]
    (let [idx-rdr (->efficient-reader idx-rdr)]
      (if (== 0 (dtype/ecount missing))
        ;;common case
        (Column. (->bitmap) (dtype/indexed-buffer idx-rdr data)
                 metadata
                 nil)
        ;;Uggh.  Construct a new missing set
        (let [idx-rdr (dtype/->reader idx-rdr)
              n-idx-elems (.lsize idx-rdr)
              ^RoaringBitmap result-set (->bitmap)]
          (dotimes [idx n-idx-elems]
           (when (.contains missing (.readLong idx-rdr idx))
             (.add result-set idx)))
          (Column. result-set
                   (dtype/indexed-buffer idx-rdr data)
                   metadata
                   nil)))))
  (to-double-array [col error-on-missing?]
    (let [n-missing (dtype/ecount missing)
          any-missing? (not= 0 n-missing)
          col-dtype (dtype/get-datatype col)]
      (when (and any-missing? error-on-missing?)
        (throw (Exception. "Missing values detected and error-on-missing set")))
      (when-not (or (= :boolean col-dtype)
                    (casting/numeric-type? (dtype/get-datatype col)))
        (throw (Exception. "Non-numeric columns do not convert to doubles.")))
      (dtype/make-container :java-array :float64 col)))
  IObj
  (meta [this] metadata)
  (withMeta [this new-meta] (Column. missing data new-meta cached-vector))
  Counted
  (count [this] (int (dtype/ecount data)))
  Indexed
  (nth [this idx]
    (.get (.data (cached-vector!)) (long idx)))
  (nth [this idx def-val]
    (.nth (cached-vector!) idx def-val))
  IFn
  (invoke [this idx]
    ((cached-vector!) idx))
  (applyTo [this args]
    (when-not (= 1 (count args))
      (throw (Exception. "Too many arguments to column")))
    (.invoke this (first args)))
  Object
  (toString [item]
    (let [n-elems (dtype/ecount data)
          format-str (if (> n-elems 20)
                       "#tech.ml.dataset.column<%s>%s\n%s\n[%s...]"
                       "#tech.ml.dataset.column<%s>%s\n%s\n[%s]")
          ;;Make the data printable
          src-rdr (dtype-pp/reader-converter item)
          data-rdr (dtype/make-reader
                    :object
                    (dtype/ecount src-rdr)
                    (if (.contains missing idx)
                      nil
                      (src-rdr idx)))]
      (format format-str
              (name (dtype/get-datatype item))
              [n-elems]
              (ds-col-proto/column-name item)
              (-> (dtype-proto/sub-buffer data-rdr 0 (min 20 n-elems))
                  (dtype-pp/print-reader-data)))))

  ;;Delegates to ListPersistentVector, which caches results for us.
  (hashCode [this] (.hashCode (cached-vector!)))

  clojure.lang.IHashEq
  ;;should be the same as using hash-unorded-coll, effectively.
  (hasheq [this]   (.hasheq (cached-vector!)))

  (equals [this o] (or (identical? this o)
                       (.equals (cached-vector!) o)))

  clojure.lang.Sequential
  clojure.lang.IPersistentCollection
  (seq   [this]
    (.seq (cached-vector!)))
  (cons  [this o]
    ;;can revisit this later if it makes sense.  For now, read only.
    (throw (ex-info "conj/.cons is not supported on columns" {})))
  (empty [this]
    (Column. (->bitmap)
             (make-container (dtype-proto/elemwise-datatype this) 0)
             {}
             nil))
  (equiv [this o] (or (identical? this o)
                      (.equiv (cached-vector!) o))))


(defmethod print-method Column
  [col ^java.io.Writer w]
  (.write w (.toString ^Object col)))


(def object-primitive-array-types
  {(Class/forName "[Ljava.lang.Boolean;") :boolean
   (Class/forName "[Ljava.lang.Byte;") :int8
   (Class/forName "[Ljava.lang.Short;") :int16
   (Class/forName "[Ljava.lang.Integer;") :int32
   (Class/forName "[Ljava.lang.Long;") :int64
   (Class/forName "[Ljava.lang.Float;") :float32
   (Class/forName "[Ljava.lang.Double;") :float64})


(defn process-object-primitive-array-data
  [obj-data]
  (let [dst-container-type (get object-primitive-array-types (type obj-data))
        ^objects obj-data obj-data
        n-items (dtype/ecount obj-data)
        sparse-val (get dtype->missing-val-map dst-container-type)
        ^List dst-data (dtype/make-container :list dst-container-type n-items)
        sparse-indexes (LongArrayList.)]
    (parallel-for/parallel-for
     idx
     n-items
     (let [obj-data (aget obj-data idx)]
       (if (not (nil? obj-data))
         (.set dst-data idx obj-data)
         (locking sparse-indexes
           (.add sparse-indexes idx)
           (.set dst-data idx sparse-val)))))
    {:data dst-data
     :missing sparse-indexes}))


(defn scan-object-data-for-missing
  [container-dtype ^List obj-data]
  (let [n-items (dtype/ecount obj-data)
        ^List dst-data (make-container container-dtype n-items)
        sparse-indexes (LongArrayList.)
        sparse-val (get dtype->missing-val-map container-dtype)]
    (parallel-for/parallel-for
     idx
     n-items
     (let [obj-data (.get obj-data idx)]
       (if (not (nil? obj-data))
         (.set dst-data idx obj-data)
         (locking sparse-indexes
           (.add sparse-indexes idx)
           (.set dst-data idx sparse-val)))))
    {:data dst-data
     :missing sparse-indexes}))


(defn scan-object-numeric-data-for-missing
  [container-dtype ^List obj-data]
  (let [n-items (dtype/ecount obj-data)
        ^List dst-data (make-container container-dtype n-items)
        sparse-indexes (LongArrayList.)
        sparse-val (get dtype->missing-val-map container-dtype)]
    (parallel-for/parallel-for
     idx
     n-items
     (let [obj-data (.get obj-data idx)]
       (if (and (not (nil? obj-data))
                (or (= container-dtype :boolean)
                    (not= sparse-val obj-data)))
         (.set dst-data idx (casting/cast obj-data container-dtype))
         (locking sparse-indexes
           (.add sparse-indexes idx)
           (.set dst-data idx sparse-val)))))
    {:data dst-data
     :missing sparse-indexes}))


(defn ensure-column-reader
  [values-seq]
  (if (dtype/reader? values-seq)
    values-seq
    (dtype/make-container :list
                          (dtype/get-datatype values-seq)
                          values-seq)))


(defn scan-data-for-missing
  [coldata]
  (if (contains? object-primitive-array-types coldata)
    (process-object-primitive-array-data coldata)
    (let [data-reader (dtype/->reader (ensure-column-reader coldata))]
      (if (= :object (dtype/get-datatype data-reader))
        (let [target-dtype (reduce casting/widest-datatype
                                   (->> data-reader
                                        (remove nil?)
                                        (take 20)
                                        (map dtype/get-datatype)))]
          (scan-object-numeric-data-for-missing target-dtype data-reader))
        {:data data-reader}))))


(defn ensure-column
  "Convert an item to a column if at all possible.  Currently columns either implement
  the required protocols "
  [item]
  (cond
    (ds-col-proto/is-column? item)
    item
    (map? item)
    (let [{:keys [name data missing metadata force-datatype?] :as cdata} item]
      (when-not (and (contains? cdata :name) data)
        (throw (Exception. "Column data map must have name and data")))
      (new-column
       name
       (if-not force-datatype?
         (:data (scan-data-for-missing data))
         data)
       metadata missing))
    :else
    (throw (ex-info "item is not convertible to a column without further information"
                    {:item item}))))


(defn ensure-column-seq
  [item-seq]
  ;;mapv to force errors here instead of later
  (mapv (fn [idx item]
          (cond
            (ds-col-proto/is-column? item)
            item
            (map? item)
            (ensure-column (update item :name
                                   #(if (nil? %)
                                      idx
                                      %)))
            (dtype/reader? item)
            (let [{:keys [data missing]} (scan-data-for-missing item)]
              (new-column idx data {} missing))
            (instance? Iterable item)
            (let [{:keys [data missing]} (scan-data-for-missing item)]
              (new-column idx data {} missing))
            :else
            (throw (ex-info "Item does not appear to be either randomly accessable or iterable"
                            {:item item}))))
        (range (count item-seq))
        item-seq))


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
     (.runOptimize missing)
     (Column. missing data (assoc metadata :name name) nil)))
  ([name data metadata]
   (new-column name data metadata (->bitmap)))
  ([name data]
   (new-column name data {} (->bitmap))))


(defn extend-column-with-empty
  [column ^long n-empty]
  (if (== 0 (long n-empty))
    column
    (let [^Column column column
          col-dtype (dtype/get-datatype column)
          n-elems (dtype/ecount column)]
      (new-column
       (ds-col-proto/column-name column)
       (dtype/concat-buffers
        [(.data column)
         (dtype/make-reader
          (get dtype->missing-val-map col-dtype)
          col-dtype
          n-empty)])
       (.metadata column)
       (dtype/set-add-range! (dtype/clone (.missing column))
             (unchecked-int n-elems)
             (unchecked-int (+ n-elems n-empty)))))))


(defn prepend-column-with-empty
  [column ^long n-empty]
  (if (== 0 (long n-empty))
    column
    (let [^Column column column
          col-dtype (dtype/get-datatype column)]
      (new-column
       (ds-col-proto/column-name column)
       (dtype/concat-concat-rdr/concat-readers
        [(dtype/const-reader
          (get dtype->missing-val-map col-dtype)
          n-empty)
         (.data column)])
       (.metadata column)
       (dtype/set-add-range!
        (dtype/set-offset (.missing column) n-empty)
        (unchecked-int 0)
        (unchecked-int n-empty))))))
