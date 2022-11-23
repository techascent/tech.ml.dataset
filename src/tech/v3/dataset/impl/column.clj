(ns ^:no-doc tech.v3.dataset.impl.column
  (:require [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.unary-pred :as un-pred]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.tensor :as dtt]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.dataset.impl.column-data-process :as column-data-process]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.set :as set]
            [ham-fisted.protocols :as hamf-proto])
  (:import [java.util Arrays]
           [ham_fisted Reductions Casts ChunkedList IMutList]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IPersistentMap Counted IFn IObj Indexed ILookup IFn$OLO IFn$ODO]
           [tech.v3.datatype Buffer LongBuffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare new-column)


(defn ->persistent-map
  ^IPersistentMap [item]
  (if (instance? IPersistentMap item)
    item
    (into {} item)))


(defn- reduce-column-buffer
  [rfn acc src missing primitive-missing-value sidx eidx]
  (let [sidx (long sidx)
        eidx (long eidx)
        ^RoaringBitmap missing missing
        ^Buffer src (dtype-proto/->buffer src)]
    (cond
      (instance? IFn$OLO rfn)
      (let [primitive-missing-value (Casts/longCast primitive-missing-value)]
        (Reductions/serialReduction (hamf/long-accumulator
                                     acc idx
                                     (.invokePrim ^IFn$OLO rfn acc
                                                  (if (.contains missing idx)
                                                    primitive-missing-value
                                                    (.readLong src idx))))
                                    acc
                                    (hamf/range sidx eidx)))
      (instance? IFn$ODO rfn)
      (let [primitive-missing-value (Casts/doubleCast primitive-missing-value)]
        (Reductions/serialReduction (hamf/long-accumulator
                                     acc idx
                                     (.invokePrim ^IFn$ODO rfn acc
                                                  (if (.contains missing idx)
                                                    primitive-missing-value
                                                    (.readDouble src idx))))
                                    acc
                                    (hamf/range sidx eidx)))
      :else
      (Reductions/serialReduction (hamf/long-accumulator
                                   acc idx
                                   (rfn acc (if (.contains missing idx)
                                              nil
                                              (.readObject src idx))))
                                  acc
                                  (hamf/range sidx eidx)))))


(defn ^:no-doc make-column-buffer
  (^Buffer [^RoaringBitmap missing data dtype]
   (let [^Buffer src (dtype-proto/->buffer data)
         missing-value (column-base/datatype->missing-value dtype)
         primitive-missing-value (column-base/datatype->packed-missing-value dtype)]
     ;;Sometimes we can utilize a pure passthrough.
     (if (.isEmpty missing)
       src
       (reify Buffer
         (elemwiseDatatype [this] dtype)
         (lsize [this] (.lsize src))
         (allowsRead [this] (.allowsRead src))
         (allowsWrite [this] (.allowsWrite src))
         (subBuffer [this sidx eidx]
           (ChunkedList/sublistCheck sidx eidx (.lsize src))
           (cond
             (and (== sidx 0) (== eidx (.lsize src)))
             this
             (not (set/intersects-range? missing sidx eidx))
             (.subBuffer src sidx eidx)
             :else
             (let [ec (- eidx sidx)]
               (reify Buffer
                 (elemwiseDatatype [rdr] dtype)
                 (lsize [rdr] ec)
                 (subBuffer [rdr ssidx seidx]
                   (ChunkedList/sublistCheck ssidx seidx ec)
                   (.subBuffer this (+ sidx ssidx) (+ sidx seidx)))
                 (readLong [rdr idx] (.readLong this (+ idx sidx)))
                 (readDouble [rdr idx] (.readDouble this (+ idx sidx)))
                 (readObject [rdr idx] (.readObject this (+ idx sidx)))
                 (reduce [this rfn acc]
                   (reduce-column-buffer rfn acc src missing
                                         primitive-missing-value sidx eidx))
                 (doubleReduction [this rfn acc]
                   (reduce-column-buffer rfn acc src missing
                                         primitive-missing-value sidx eidx))
                 (longReduction [this rfn acc]
                   (reduce-column-buffer rfn acc src missing
                                         primitive-missing-value sidx eidx))))))
         (readLong [this idx]
           (if (.contains missing idx)
             (Casts/longCast primitive-missing-value)
             (.readLong src idx)))
         (readDouble [this idx]
           (if (.contains missing idx)
             (Casts/doubleCast primitive-missing-value)
             (.readDouble src idx)))
         (readObject [this idx]
           (when-not (.contains missing idx)
             (.readObject src idx)))
         (writeLong [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeLong src idx val))
         (writeDouble [this idx val]
           (.remove missing (unchecked-int idx))
           (.writeDouble src idx val))
         (writeObject [this idx val]
           (if val
             (do
               (.remove missing (unchecked-int idx))
               (.writeObject src idx val))
             (.add missing (unchecked-int idx))))
         (reduce [this rfn acc]
           (reduce-column-buffer rfn acc src missing primitive-missing-value
                                 0 (.lsize src)))
         (doubleReduction [this rfn acc]
           (reduce-column-buffer rfn acc src missing
                                 primitive-missing-value 0 (.lsize src)))
         (longReduction [this rfn acc]
           (reduce-column-buffer rfn acc src missing
                                 primitive-missing-value 0 (.lsize src)))))))
  (^Buffer [missing data]
   (make-column-buffer missing data (dtype-proto/elemwise-datatype data))))


(defmacro cached-buffer! []
  `(or ~'buffer
       (do (set! ~'buffer (make-column-buffer ~'missing ~'data))
           ~'buffer)))


(defn- neg-access-reader
  [^long n-elems data]
  (let [m (meta data)
        data (dtype/->reader data)]
    (with-meta
     (reify LongBuffer
       (lsize [rdr] (.lsize data))
       (readLong [rdr idx]
         (let [v (.readLong data idx)]
           (if (< v 0)
             (+ v n-elems)
             v)))
       (subBuffer [rdr sidx eidx]
         (neg-access-reader n-elems (.subBuffer data sidx eidx))))
      (assoc m :min 0 :max (unchecked-dec n-elems)))))


(defn- wrap-negative-access
  [^long n-elems data]
  (if (== 0 (dtype/ecount data))
    data
    (if (dtype-proto/has-constant-time-min-max? data)
      (let [minv (long (dtype-proto/constant-time-min data))]
        (if (>= minv 0)
          data
          (neg-access-reader n-elems data)))
      (neg-access-reader n-elems data))))


(defn simplify-row-indexes
  [^long n-elems selection]
  (if (:simplified? (meta selection))
    selection
    (vary-meta
     (cond
       (nil? selection) (hamf/range 0)
       ;;Cannot possibly be negative
        (= (dtype/elemwise-datatype selection) :boolean)
        (un-pred/bool-reader->indexes (dtype/ensure-reader selection))
        (set/bitset? selection)
        (set/->integer-random-access selection)
        (hamf-proto/set? selection)
        (->> (set/->integer-random-access selection)
             (wrap-negative-access n-elems))
        :else
        ;;Ensure the selection object has metadata
        (let [minv (long (if (dtype-proto/has-constant-time-min-max? selection)
                           (long (dtype-proto/constant-time-min selection))
                           -1))]
          (if (< (long minv) 0)
            (->> (hamf/preduce-reducer (un-pred/index-reducer n-elems) selection)
                 (wrap-negative-access n-elems))
            ;;if the data indicates it has no negative values
            (dtype/->reader selection))))
     assoc :simplified? true)))


(deftype Column
    [^RoaringBitmap missing
     data
     ^IPersistentMap metadata
     ^:unsynchronized-mutable ^Buffer buffer]

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
               nil)))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [_this new-dtype]
    (if (= new-dtype (dtype-proto/elemwise-datatype data))
      (cached-buffer!)
      (make-column-buffer missing (dtype-proto/elemwise-reader-cast data new-dtype)
                          new-dtype)))
  dtype-proto/PECount
  (ecount [this] (dtype-proto/ecount data))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [_this] true)
  (->buffer [_this]
    (cached-buffer!))
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
          len (long len)
          eidx (+ offset len)]
      (ChunkedList/sublistCheck offset (+ offset len) (dtype/ecount data))
      (if (and (== offset 0) (== len (dtype/ecount this)))
        this
        ;;TODO - use bitmap operations to perform this calculation
        (let [new-missing (RoaringBitmap/and
                           missing
                           (doto (RoaringBitmap.)
                             (.add offset (+ offset len))))
              new-data  (dtype-proto/sub-buffer data offset len)]
          (Column. new-missing
                   new-data
                   metadata
                   nil)))))
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
               nil)))
  ds-proto/PRowCount
  (row-count [this] (dtype/ecount data))
  ds-proto/PMissing
  (missing [this] missing)
  ds-proto/PSelectRows
  (select-rows [this rowidxs]
    (let [rowidxs (simplify-row-indexes (dtype/ecount this) rowidxs)
          new-missing (if (== 0 (set/cardinality missing))
                        (bitmap/->bitmap)
                        (argops/argfilter (set/contains-fn missing)
                                          {:storage-type :bitmap}
                                          rowidxs))
          new-data (dtype/indexed-buffer rowidxs data)]
      (Column. new-missing
               new-data
               metadata
               nil)))
  ds-proto/PColumn
  (is-column? [_this] true)
  (column-buffer [_this] data)
  IMutList
  (size [this] (.size (cached-buffer!)))
  (get [this idx] (.get (cached-buffer!) idx))
  (set [this idx v] (.set (cached-buffer!) idx v))
  (getLong [this idx] (.getLong (cached-buffer!) idx))
  (setLong [this idx v] (.setLong (cached-buffer!) idx v))
  (getDouble [this idx] (.getDouble (cached-buffer!) idx))
  (setDouble [this idx v] (.setDouble (cached-buffer!) idx v))
  (valAt [this idx] (.valAt (cached-buffer!) idx))
  (valAt [this idx def-val] (.valAt (cached-buffer!) idx def-val))
  (invoke [this idx] (.invoke (cached-buffer!) idx))
  (invoke [this idx def-val] (.invoke (cached-buffer!) idx def-val))
  (meta [this] (assoc metadata
                      :datatype (dtype-proto/elemwise-datatype this)
                      :n-elems (dtype-proto/ecount this)))
  (withMeta [_this new-meta] (Column. missing
                                     data
                                     new-meta
                                     buffer))
  (nth [_this idx] (nth (cached-buffer!) idx))
  (nth [_this idx def-val] (nth (cached-buffer!) idx def-val))
  ;;should be the same as using hash-unorded-coll, effectively.
  (hasheq [_this]   (.hasheq (cached-buffer!)))
  (equiv [this o] (if (identical? this o) true (.equiv (cached-buffer!) o)))
  (empty [this]
    (Column. (->bitmap)
             (column-base/make-container (dtype-proto/elemwise-datatype this) 0)
             {}
             nil))
  (reduce [this rfn init] (.reduce (cached-buffer!) rfn init))
  (longReduction [this rfn init] (.longReduction (cached-buffer!) rfn init))
  (doubleReduction [this rfn init] (.doubleReduction (cached-buffer!) rfn init))
  (parallelReduction [this init-val-fn rfn merge-fn options]
    (.parallelReduction (cached-buffer!) init-val-fn rfn merge-fn options))
  Object
  (toString [item]
    (let [n-elems (dtype/ecount data)
          format-str (if (> n-elems 20)
                       "#tech.v3.dataset.column<%s>%s\n%s\n[%s...]"
                       "#tech.v3.dataset.column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/elemwise-datatype item))
              [n-elems]
              (ds-proto/column-name item)
              (-> (dtype-proto/sub-buffer item 0 (min 20 n-elems))
                  (dtype-pp/print-reader-data)))))

  ;;Delegates to ListPersistentVector, which caches results for us.
  (hashCode [_this] (.hasheq (cached-buffer!)))
  (equals [this o] (.equiv this o)))


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
                nil)))))


(defn ensure-column-seq
  [coldata-seq]
  (->> coldata-seq
       (column-data-process/prepare-column-data-seq)
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
      (Column. (set/union (ds-proto/missing column)
                          (bitmap/->bitmap (hamf/range n-elems (+ n-elems n-empty))))
               container
               (meta column)
               nil))))


(defn prepend-column-with-empty
  [column ^long n-empty]
  (if (== 0 (long n-empty))
    column
    (let [^Column column column
          col-dtype (dtype/get-datatype column)
          n-elems (dtype/ecount column)
          container (dtype/make-container col-dtype (+ n-elems n-empty))]
      (dtype/copy! (.data column) (dtype/sub-buffer container n-empty n-elems))
      (dtype/set-constant! container 0 n-empty (get column-base/dtype->missing-val-map
                                                    col-dtype))
      (Column. (set/union (bitmap/->bitmap (hamf/range n-empty))
                          (bitmap/offset (.missing column) n-empty))
               container
               (meta column)
               nil))))
