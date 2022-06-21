(ns ^:no-doc tech.v3.dataset.impl.column
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.unary-pred :as un-pred]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.tensor :as dtt]
            [tech.v3.dataset.parallel-unique :refer [parallel-unique]]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.dataset.impl.column-data-process :as column-data-process]
            [tech.v3.dataset.impl.column-index-structure :refer [make-index-structure]])
  (:import [java.util Arrays]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IPersistentMap Counted IFn IObj Indexed]
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
    ;;Sets are ordered when converted as index buffers.
    (instance? RoaringBitmap item)
    (bitmap/bitmap->efficient-random-access-reader item)
    (set? item)
    (let [ary (long-array item)]
      (Arrays/sort ary)
      ary)
    (dtype-proto/convertible-to-reader? item)
    item
    :else
    (long-array item)))


(defn- make-buffer
  (^Buffer [^RoaringBitmap missing data dtype]
   (let [^Buffer src (dtype-proto/->buffer data)
         {:keys [unpacking-read packing-write]}
         (packing/buffer-packing-pair dtype)
         missing-value (column-base/datatype->missing-value dtype)
         primitive-missing-value (column-base/datatype->packed-missing-value dtype)]
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
             (unchecked-byte primitive-missing-value)
             (.readByte src idx)))
         (readShort [this idx]
           (if (.contains missing idx)
             (unchecked-short primitive-missing-value)
             (.readShort src idx)))
         (readChar [this idx]
           (if (.contains missing idx)
             (char primitive-missing-value)
             (.readChar src idx)))
         (readInt [this idx]
           (if (.contains missing idx)
             (unchecked-int primitive-missing-value)
             (.readInt src idx)))
         (readLong [this idx]
           (if (.contains missing idx)
             (unchecked-long primitive-missing-value)
             (.readLong src idx)))
         (readFloat [this idx]
           (if (.contains missing idx)
             (float primitive-missing-value)
             (.readFloat src idx)))
         (readDouble [this idx]
           (if (.contains missing idx)
             (double primitive-missing-value)
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


(deftype Column
    [^RoaringBitmap missing
     data
     ^IPersistentMap metadata
     ^:unsynchronized-mutable ^ListPersistentVector cached-vector
     *index-structure]

    col-proto/PHasIndexStructure
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
      (if (empty? missing)
        @*index-structure
        (throw (Exception.
                (str "Cannot obtain an index for column `"
                     (col-proto/column-name this)
                     "` because it contains missing values.")))))
    (index-structure-realized? [_this]
      (realized? *index-structure))
    (with-index-structure [_this make-index-structure-fn]
      (Column. missing
               data
               metadata
               cached-vector
               (delay (make-index-structure-fn data metadata))))

    dtype-proto/PToArrayBuffer
    (convertible-to-array-buffer? [_this]
      (and (.isEmpty missing)
           (dtype-proto/convertible-to-array-buffer? data)))
    (->array-buffer [_this]
      (dtype-proto/->array-buffer data))

    dtype-proto/PToNativeBuffer
    (convertible-to-native-buffer? [_this]
      (and (.isEmpty missing)
           (dtype-proto/convertible-to-native-buffer? data)))
    (->native-buffer [_this]
      (dtype-proto/->native-buffer data))
    dtype-proto/PElemwiseDatatype
    (elemwise-datatype [_this] (dtype-proto/elemwise-datatype data))
    dtype-proto/POperationalElemwiseDatatype
    (operational-elemwise-datatype [this]
      (if (.isEmpty missing)
        (dtype-proto/elemwise-datatype this)
        (let [ewise-dt (dtype-proto/elemwise-datatype data)]
          (cond
            (packing/packed-datatype? ewise-dt)
            (packing/unpack-datatype ewise-dt)
            (casting/numeric-type? ewise-dt)
            (casting/widest-datatype ewise-dt :float64)
            (identical? :boolean ewise-dt)
            :object
            :else
            ewise-dt))))
    dtype-proto/PElemwiseCast
    (elemwise-cast [_this new-dtype]
      (let [new-data (dtype-proto/elemwise-cast data new-dtype)]
        (Column. missing
                 new-data
                 metadata
                 nil
                 (delay (make-index-structure new-data metadata)))))
    dtype-proto/PElemwiseReaderCast
    (elemwise-reader-cast [_this new-dtype]
      (if (= new-dtype (dtype-proto/elemwise-datatype data))
        (do
          (cached-vector!)
          (.data cached-vector))
        (make-buffer missing (dtype-proto/elemwise-reader-cast data new-dtype)
                     new-dtype)))
    dtype-proto/PToBuffer
    (convertible-to-buffer? [_this] true)
    (->buffer [_this]
      (cached-vector!)
      (.data cached-vector))
    dtype-proto/PToReader
    (convertible-to-reader? [_this]
      (dtype-proto/convertible-to-reader? data))
    (->reader [this]
      (dtype-proto/->buffer this))
    dtype-proto/PToWriter
    (convertible-to-writer? [_this]
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
                     (delay (make-index-structure new-data metadata)))))))
    dtype-proto/PClone
    (clone [_col]
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
                 (delay (make-index-structure new-data metadata)))))
    Iterable
    (iterator [this]
      (.iterator (dtype-proto/->buffer this)))
    col-proto/PIsColumn
    (is-column? [_this] true)
    col-proto/PColumn
    (column-name [_col] (:name metadata))
    (set-name [_col name] (Column. missing
                                     data
                                     (assoc metadata :name name)
                                     cached-vector
                                     *index-structure))
    (supported-stats [_col] stats/all-descriptive-stats-names)
    (missing [_col] missing)
    (is-missing? [_col idx] (.contains missing (long idx)))
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
                 (delay (make-index-structure data metadata)))))
    (buffer [_this] data)
    (as-map [_this] #:tech.v3.dataset{:name (:name metadata)
                                      :data data
                                      :missing missing
                                      :metadata metadata
                                      :force-datatype? true})
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
    (select [_col selection]
      (let [selection (if (= (dtype/elemwise-datatype selection) :boolean)
                          (->efficient-reader (un-pred/bool-reader->indexes
                                                  (dtype/ensure-reader selection)))
                          (->efficient-reader selection))]
        (if (== 0 (dtype/ecount missing))
          ;;common case
          (let [bitmap (->bitmap)
                new-data (dtype/indexed-buffer selection data)]
            (Column. bitmap
                     new-data
                     metadata
                     nil
                     (delay (make-index-structure data metadata))))
          ;;Uggh.  Construct a new missing set
          (let [idx-rrdr (dtype/->reader selection)
                n-idx-elems (.lsize idx-rrdr)
                ^RoaringBitmap result-set (->bitmap)]
            (dotimes [idx n-idx-elems]
              (when (.contains missing (.readLong idx-rrdr idx))
                (.add result-set idx)))
            (let [new-data (dtype/indexed-buffer selection data)]
              (Column. result-set
                       new-data
                       metadata
                       nil
                       (delay (make-index-structure new-data metadata))))))))
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
        (dtype/->double-array (dtype-proto/elemwise-reader-cast col :float64))))
    IObj
    (meta [this]
      (assoc metadata
             :datatype (dtype-proto/elemwise-datatype this)
             :n-elems (dtype-proto/ecount this)))
    (withMeta [_this new-meta] (Column. missing
                                          data
                                          new-meta
                                          cached-vector
                                          (delay (make-index-structure data new-meta))))
    Counted
    (count [_this] (int (dtype/ecount data)))
    Indexed
    (nth [_this idx]
      (let [idx (long idx)
            orig-idx idx
            n-elems (dtype/ecount data)
            idx (if (< idx 0) (+ n-elems idx) idx)]
        (if (and (>= idx 0)
                 (< idx n-elems))
          ((cached-vector!) idx)
          (throw (Exception. (format "Index %d is out of range [%d, %d]"
                                     orig-idx (- n-elems) (dec n-elems))))))
      ((cached-vector!) idx))
    (nth [_this idx def-val]
      (let [idx (long idx)
            n-elems (dtype/ecount data)
            idx (if (< idx 0) (+ n-elems idx) idx)]
        (if (and (>= idx 0)
                 (< idx n-elems))
          ((cached-vector!) idx)
          def-val)))
    IFn
    (invoke [_this idx]
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
        (format (str format-str "\n%s")
                (name (dtype/elemwise-datatype item))
                [n-elems]
                (col-proto/column-name item)
                (-> (dtype-proto/sub-buffer item 0 (min 20 n-elems))
                    (dtype-pp/print-reader-data))
                (-> item meta ((fn [o] (with-out-str (clojure.pprint/pprint o))))))))


    ;;Delegates to ListPersistentVector, which caches results for us.
    (hashCode [_this] (.hashCode (cached-vector!)))

    clojure.lang.IHashEq
    ;;should be the same as using hash-unorded-coll, effectively.
    (hasheq [_this]   (.hasheq (cached-vector!)))

    (equals [this o] (or (identical? this o)
                         (.equals (cached-vector!) o)))

    clojure.lang.Sequential
    clojure.lang.IPersistentCollection
    (seq   [_this]
      (.seq (cached-vector!)))
    (cons  [_this _o]
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


(defn new-column
  "Given a map of (something convertible to a long reader) missing indexes,
  (something convertible to a reader) data
  and a (string or keyword) name, return an implementation of enough of the
  column and datatype protocols to allow efficient columnwise operations of
  the rest of tech.ml.dataset"
  ([name data metadata missing]
   (new-column #:tech.v3.dataset{:name name
                                 :data data
                                 :metadata metadata
                                 :missing missing}))
  ([name data metadata]
   (new-column name data metadata nil))
  ([name data]
   (new-column name data {} nil))
  ([data-or-column-data-map]
   (let [coldata (column-data-process/prepare-column-data data-or-column-data-map)
         data (coldata :tech.v3.dataset/data)
         metadata (coldata :tech.v3.dataset/metadata)
         name (coldata :tech.v3.dataset/name)
         ;;Unless overidden, we now set the categorical? flag
         metadata (if (and (not (contains? metadata :categorical?))
                           (column-base/column-datatype-categorical?
                            (dtype/elemwise-datatype data)))
                    (assoc metadata :categorical? true)
                    metadata)
         missing (bitmap/->bitmap (coldata :tech.v3.dataset/missing))]
     ;;compress bitmaps
     (.runOptimize missing)
     (let [new-meta (assoc metadata :name name)]
       (Column. missing
                data
                new-meta
                nil
                (delay (make-index-structure data new-meta)))))))


(defn ensure-column-seq
  [coldata-seq]
  (->> (column-data-process/prepare-column-data-seq coldata-seq)
       ;;mapv so we get a real stacktrace when things go south.
       (mapv new-column)))


(defn extend-column-with-empty
  [column ^long n-empty]
  (if (== 0 (long n-empty))
    column
    (let [^Column column column
          col-dtype (dtype/elemwise-datatype column)
          n-elems (dtype/ecount column)
          container (dtype/make-container col-dtype (+ n-elems n-empty))]
      (dtype/copy! (.data column) (dtype/sub-buffer container 0 n-elems))
      (dtype/set-constant! container n-elems n-empty (get column-base/dtype->missing-val-map
                                                          col-dtype))
      (new-column
       (col-proto/column-name column)
       container
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
