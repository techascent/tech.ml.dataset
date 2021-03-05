(ns ^:no-doc tech.v3.dataset.impl.column
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.tensor :as dtt]
            [tech.v3.dataset.parallel-unique :refer [parallel-unique]]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.dataset.impl.column-data-process :as column-data-process])
  (:import [java.util ArrayList HashSet Collections Set List Map]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IPersistentMap IMeta Counted IFn IObj Indexed]
           [tech.v3.datatype Buffer ListPersistentVector]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare new-column)


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


(defn- make-buffer
  (^Buffer [^RoaringBitmap missing data dtype]
   (let [^Buffer src (dtype-proto/->buffer data)
         {:keys [unpacking-read packing-write]}
         (packing/buffer-packing-pair dtype)
         missing-value (column-base/datatype->missing-value dtype)]
     ;;Sometimes we can utilize a pure passthrough.
     (if (and (.isEmpty missing)
              (not unpacking-read))
       src
       (reify Buffer
         (elemwiseDatatype [this] dtype)
         (lsize [this] (.lsize src))
         (allowsRead [this] (.allowsRead src))
         (allowsWrite [this] (.allowsWrite src))
         (readBoolean [this idx]
           (if (.contains missing idx)
             (casting/datatype->boolean :unknown missing-value)
             (.readBoolean src idx)))
         (readByte [this idx]
           (if (.contains missing idx)
             (unchecked-byte missing-value)
             (.readByte src idx)))
         (readShort [this idx]
           (if (.contains missing idx)
             (unchecked-short missing-value)
             (.readShort src idx)))
         (readChar [this idx]
           (if (.contains missing idx)
             (char missing-value)
             (.readChar src idx)))
         (readInt [this idx]
           (if (.contains missing idx)
             (unchecked-int missing-value)
             (.readInt src idx)))
         (readLong [this idx]
           (if (.contains missing idx)
             (unchecked-long missing-value)
             (.readLong src idx)))
         (readFloat [this idx]
           (if (.contains missing idx)
             (float missing-value)
             (.readFloat src idx)))
         (readDouble [this idx]
           (if (.contains missing idx)
             (double missing-value)
             (.readDouble src idx)))
         (readObject [this idx]
           (when-not (.contains missing idx)
             (if unpacking-read
               (unpacking-read this idx)
               (.readObject src idx))))
         (writeBoolean [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeBoolean src idx val))
         (writeByte [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeByte src idx val))
         (writeShort [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeShort src idx val))
         (writeChar [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeChar src idx val))
         (writeInt [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeInt src idx val))
         (writeLong [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeLong src idx val))
         (writeFloat [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeFloat src idx val))
         (writeDouble [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeDouble src idx val))
         (writeObject [this idx val]
           (if val
             (do
               (.remove missing (unchecked-int idx))
               (if packing-write
                 (packing-write this idx val)
                 (.writeObject src idx val)))
             (.add missing (unchecked-int idx))))))))
  (^Buffer [missing data]
   (make-buffer missing data (dtype-proto/elemwise-datatype data))))


(defmacro cached-vector! []
  `(or ~'cached-vector
       (do (set! ~'cached-vector
                 (ListPersistentVector.
                  (make-buffer ~'missing ~'data)))
           ~'cached-vector)))


(defmulti make-index-structure
  "Returns an index structure based on the type of data in the column."
  (fn [data _] (-> data
                   dtype/elemwise-datatype
                   casting/datatype->object-class)))

;; When tech.datatype does not know what something is it describes it
;; as an object (see tech.v3.datatype.casting/elemwise-datatype). This
;; dispatch method then serves as a default unless someone has extended
;; this multimethod to catch a more specific datatype.
(defmethod make-index-structure java.lang.Object
  [data missing]
  (let [idx-map (arggroup data)]
    (java.util.TreeMap. ^java.util.Map idx-map)))


(defprotocol PHasIndexStructure
  (index-structure [this]))


(deftype Column
    [^RoaringBitmap missing
     data
     ^IPersistentMap metadata
     ^:unsynchronized-mutable ^ListPersistentVector cached-vector
     *index-structure]

  PHasIndexStructure
  ;; This index-structure returned by this function can be invalid if
  ;; the column's reader is based on a non-deterministic computation.
  ;; For now, we think this may be okay because it's a unique edge-case.
  ;; What value could an index have on data that is random and changing?
  ;; We think it is reasonable to expect the user of tech.ml.dataset, which
  ;; is a somewhat low-level library, to know that it wouldn't make sense
  ;; to request the index structure on a column consisting of such data.
  ;; For more, see this discussion on Clojurians Zulip: https://bit.ly/3dRa9MY
  ;;
  ;; TODO: Considering validating by checking index values against column data (traversal or hashing)
  (index-structure [this]
    @*index-structure)

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
    (let [new-data (dtype-proto/elemwise-cast data new-dtype)]
      (Column. missing
               new-data
               metadata
               nil
               (delay (make-index-structure new-data missing)))))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype]
    (if (= new-dtype (dtype-proto/elemwise-datatype data))
      (do
        (cached-vector!)
        (.data cached-vector))
      (make-buffer missing (dtype-proto/elemwise-reader-cast data new-dtype)
                   new-dtype)))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [this] true)
  (->buffer [this]
    (cached-vector!)
    (.data cached-vector))
  dtype-proto/PToReader
  (convertible-to-reader? [this]
    (dtype-proto/convertible-to-reader? data))
  (->reader [this]
    (dtype-proto/->buffer this))
  dtype-proto/PToWriter
  (convertible-to-writer? [this]
    (dtype-proto/convertible-to-writer? data))
  (->writer [this]
    (dtype-proto/->buffer this))
  dtype-proto/PSubBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len (long len)]
      (if (and (== offset 0)
               (== len (dtype/ecount this)))
        this
        ;;TODO - use bitmap operations to perform this calculation
        (let [new-missing (dtype-proto/set-and
                           missing
                           (bitmap/->bitmap (range offset (+ offset len))))
              new-data    (dtype-proto/sub-buffer data offset len)]
          (Column. new-missing
                   new-data
                   metadata
                   nil
                   (delay (make-index-structure new-data new-missing)))))))
  dtype-proto/PClone
  (clone [col]
    (let [new-data (if (or (dtype/writer? data)
                           (= :string (dtype/get-datatype data))
                           (= :encoded-text (dtype/get-datatype data)))
                     (dtype/clone data)
                     ;;It is important that the result of this operation be writeable.
                     (dtype/make-container :jvm-heap
                                           (dtype/get-datatype data) data))
          cloned-missing (dtype/clone missing)]
      (Column. cloned-missing
               new-data
               metadata
               nil
               (delay (make-index-structure new-data cloned-missing)))))
  Iterable
  (iterator [this]
    (.iterator (dtype-proto/->buffer this)))
  col-proto/PIsColumn
  (is-column? [this] true)
  col-proto/PColumn
  (column-name [col] (:name metadata))
  (set-name [col name] (Column. missing
                                data
                                (assoc metadata :name name)
                                cached-vector
                                *index-structure))
  (supported-stats [col] stats/all-descriptive-stats-names)
  (missing [col] missing)
  (is-missing? [col idx] (.contains missing (long idx)))
  (set-missing [col long-rdr]
    (let [long-rdr (if (dtype/reader? long-rdr)
                     long-rdr
                     ;;handle infinite seq's
                     (take (dtype/ecount data) long-rdr))
          bitmap (->bitmap long-rdr)]
      (let [max-value (long (if-not (.isEmpty bitmap)
                              (dtype-proto/constant-time-max bitmap)
                              0))]
        ;;trim the bitmap to fit the column
        (when (>= max-value (dtype/ecount col))
          (.andNot bitmap (bitmap/->bitmap (range (dtype/ecount col)
                                                  (unchecked-inc max-value))))
          (assert (< (long (dtype-proto/constant-time-max bitmap))
                     (dtype/ecount col)))))
      (.runOptimize bitmap)
      (Column. bitmap
               data
               metadata
               nil
               (delay (make-index-structure data bitmap)))))
  (unique [this]
    (->> (parallel-unique this)
         (into #{})))
  (stats [col stats-set]
    (when-not (casting/numeric-type? (dtype-proto/elemwise-datatype col))
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (dtype/get-datatype col)
                       :column-name (:name metadata)})))
    (dfn/descriptive-statistics stats-set col))
  (correlation [col other-column correlation-type]
    (case correlation-type
      :pearson (dfn/pearsons-correlation col other-column)
      :spearman (dfn/spearmans-correlation col other-column)
      :kendall (dfn/kendalls-correlation col other-column)))
  (select [col idx-rdr]
    (let [idx-rdr (->efficient-reader idx-rdr)]
      (if (== 0 (dtype/ecount missing))
        ;;common case
        (let [bitmap (->bitmap)
              new-data (dtype/indexed-buffer idx-rdr data)]
          (Column. bitmap
                   new-data
                   metadata
                   nil
                   (delay (make-index-structure data bitmap))))
        ;;Uggh.  Construct a new missing set
        (let [idx-rdr (dtype/->reader idx-rdr)
              n-idx-elems (.lsize idx-rdr)
              ^RoaringBitmap result-set (->bitmap)]
          (dotimes [idx n-idx-elems]
            (when (.contains missing (.readLong idx-rdr idx))
              (.add result-set idx)))
          (let [new-data (dtype/indexed-buffer idx-rdr data)]
            (Column. result-set
                     new-data
                     metadata
                     nil
                     (delay (make-index-structure new-data result-set))))))))
  (to-double-array [col error-on-missing?]
    (let [n-missing (dtype/ecount missing)
          any-missing? (not= 0 n-missing)
          col-dtype (dtype/get-datatype col)]
      (when (and any-missing? error-on-missing?)
        (throw (Exception. "Missing values detected and error-on-missing? set")))
      (when-not (or (= :boolean col-dtype)
                    (= :object col-dtype)
                    (casting/numeric-type? (dtype/get-datatype col)))
        (throw (Exception. "Non-numeric columns do not convert to doubles.")))
      (dtype/->double-array (dtype-proto/elemwise-reader-cast col :float64)))))
  IObj
  (meta [this]
    (assoc metadata
           :datatype (dtype-proto/elemwise-datatype this)
           :n-elems (dtype-proto/ecount this)))
  (withMeta [this new-meta] (Column. missing
                                     data
                                     new-meta
                                     cached-vector
                                     *index-structure))
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
                       "#tech.v3.dataset.column<%s>%s\n%s\n[%s...]"
                       "#tech.v3.dataset.column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/elemwise-datatype item))
              [n-elems]
              (col-proto/column-name item)
              (-> (dtype-proto/sub-buffer item 0 (min 20 n-elems))
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
             (column-base/make-container (dtype-proto/elemwise-datatype this) 0)
             {}
             nil
             (delay nil)))
  (equiv [this o] (or (identical? this o)
                      (.equiv (cached-vector!) o))))


(dtype-pp/implement-tostring-print Column)


(defn ensure-column
  [item]
  (cond
    (col-proto/is-column? item)
    item
    (map? item)
    (let [{:keys [name data missing metadata force-datatype?] :as cdata} item]
      (when-not (and (contains? cdata :name) data)
        (throw (Exception. "Column data map must have name and data")))
      (let [{data :data
             new-missing :missing}
            (if-not force-datatype?
              (column-data-process/scan-data-for-missing data)
              {:data data
               :missing (bitmap/->bitmap)})]
        (new-column
         name
         data
         metadata (or missing new-missing))))
    :else
    (throw (ex-info "item is not convertible to a column without further information"
                    {:item item}))))


(defn ensure-column-seq
  [item-seq]
  ;;mapv to force errors here instead of later
  (mapv (fn [idx item]
          (cond
            (col-proto/is-column? item)
            item
            (map? item)
            (ensure-column (update item :name
                                   #(if (nil? %)
                                      idx
                                      %)))
            (dtype/reader? item)
            (let [{:keys [data missing]} (column-data-process/scan-data-for-missing item)]
              (new-column idx data {} missing))
            (instance? Iterable item)
            (let [{:keys [data missing]} (column-data-process/scan-data-for-missing item)]
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
     (Column. missing
              data
              (assoc metadata :name name)
              nil
              (delay (make-index-structure data missing)))))
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
       (col-proto/column-name column)
       (dtype/concat-buffers
        col-dtype
        [(.data column)
         (dtype/const-reader
          (get column-base/dtype->missing-val-map col-dtype)
          n-empty)])
       (.metadata column)
       (dtype-proto/set-add-range! (dtype/clone (.missing column))
             (unchecked-int n-elems)
             (unchecked-int (+ n-elems n-empty)))))))


(defn prepend-column-with-empty
  [column ^long n-empty]
  (if (== 0 (long n-empty))
    column
    (let [^Column column column
          col-dtype (dtype/get-datatype column)]
      (new-column
       (col-proto/column-name column)
       (dtype/concat-buffers
        col-dtype
        [(dtype/const-reader
          (get column-base/dtype->missing-val-map col-dtype)
          n-empty)
         (.data column)])
       (.metadata column)
       (dtype-proto/set-add-range!
        (dtype-proto/set-offset (.missing column) n-empty)
        (unchecked-int 0)
        (unchecked-int n-empty))))))


;;Object defaults
(extend-type Object
  col-proto/PIsColumn
  (is-column? [this] false)
  col-proto/PColumn
  (column-name [this] :_unnamed)
  (supported-stats [this] nil)
  (missing [col] (bitmap/->bitmap))
  (select [col idx-seq]
    (dtt/select col [(->efficient-reader idx-seq)]))
  (to-double-array [col error-on-missing?]
    (dtype/->double-array col)))
