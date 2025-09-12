(ns tech.v3.dataset.impl.sparse-column
  (:require [tech.v3.datatype :as dt]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.string-table :as str-t]
            [ham-fisted.set :as set]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.iterator :as hamf-iter])
  (:import [ham_fisted IMutList ArrayLists ITypedReduce ChunkedList]
           [tech.v3.datatype Buffer ObjectReader ElemwiseDatatype LongReader DoubleReader]
           [java.util Arrays Iterator Map]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)
(declare scol->reader reduce-scol kv-reduce-scol construct-sparse-col)

(defn- index-bin-search
  ^clojure.lang.IFn$LL [^IMutList indexes]
  (let [idx-ary (.toNativeArray indexes)
        nrc (- (long (.size indexes)))]
    (case (dt/elemwise-datatype idx-ary)
      :int8 (hamf-fn/long-unary-operator
                idx (Arrays/binarySearch ^bytes idx-ary (unchecked-byte idx)))
      :int16 (hamf-fn/long-unary-operator
                 idx (Arrays/binarySearch ^shorts idx-ary (unchecked-short idx)))
      :int32 (hamf-fn/long-unary-operator                 
                 idx (Arrays/binarySearch ^shorts idx-ary (unchecked-short idx)))
      (hamf-fn/long-unary-operator
          idx (Arrays/binarySearch ^longs idx-ary (unchecked-long idx))))))

;; Column wrapping csr format.
(deftype SparseCol [^IMutList indexes ;;sorted indexes
                    ^IMutList data ^long rc metadata
                    ^:unsynchronized-mutable msng ;;missing
                    ^:unsynchronized-mutable rdr]
  ElemwiseDatatype
  (elemwiseDatatype [this] (dt/elemwise-datatype data))
  dt-proto/POperationalElemwiseDatatype
  (operational-elemwise-datatype [this]
    (let [ewise-dt (dt-proto/elemwise-datatype data)]
      (cond
        (packing/packed-datatype? ewise-dt)
        (packing/unpack-datatype ewise-dt)
        (casting/numeric-type? ewise-dt)
        (casting/widest-datatype ewise-dt :float64)
        (identical? :boolean ewise-dt)
        :object
        :else
        ewise-dt)))
  tech.v3.datatype.ECount
  (lsize [this] rc)
  dt-proto/PToBuffer
  (convertible-to-buffer? [this] true)
  (->buffer [this]
    (if rdr
      rdr
      (let [rv (scol->reader this)]
        (set! rdr rv)
        rv)))
  dt-proto/PSubBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len (long len)
          eidx (+ offset len)]
      (ChunkedList/sublistCheck offset (+ offset len) (dt/ecount data))
      (if (and (== offset 0) (== len rc))
        this
        (let [sidx offset
              new-indexes (.subList indexes sidx eidx)
              new-data (.subList data sidx eidx)
              new-rc (- eidx sidx)]
          (SparseCol. new-indexes new-data new-rc metadata nil nil)))))
  dt-proto/PClone
  (clone [_col]
    (SparseCol. (dt/clone indexes) (dt/clone data) rc metadata nil nil))
  ds-proto/PMissing
  (missing [this]
    (if (.-msng this)
      (.-msng this)
      (do
        (let [miss-data (set/difference (bitmap/->bitmap (hamf/range rc)) (bitmap/->bitmap indexes))]
          (set! (.-msng this) miss-data)
          miss-data))))
  (num-missing [this] (- rc (.size indexes)))
  (any-missing? [this] (not (== rc (.size indexes))))
  ds-proto/PValidRows
  (valid-rows [this] indexes)
  ds-proto/PColumn
  (is-column? [this] true)
  (column-buffer [this] [indexes data])
  (empty-column? [this] (.isEmpty indexes))
  (column-data [this] data)
  (with-column-data [this new-data] (construct-sparse-col indexes new-data rc metadata))
  ds-proto/PColumnName
  (column-name [this] (get metadata :name))
  ds-proto/PRowCount
  (row-count [this] (dt/ecount data))
  ds-proto/PSelectRows
  (select-rows [this rowidxs]
    (let [rowidxs (col-impl/simplify-row-indexes rc rowidxs)
          res-rc (dt/ecount rowidxs)
          ^IMutList res-ixs (cond
                              (<= res-rc Byte/MAX_VALUE)
                              (hamf/byte-array-list)
                              (<= res-rc Short/MAX_VALUE)
                              (hamf/short-array-list)
                              (<= res-rc Integer/MAX_VALUE)
                              (hamf/int-array-list)
                              :else
                              (hamf/long-array-list))
          ^IMutList res-vals (dt/make-list (dt/elemwise-datatype data))
          local-indexes indexes
          bin-search (index-bin-search indexes)
          last-data (long-array [-1 (.getLong local-indexes 0) 0])
          dnidxs (dec (.size local-indexes))]
      (reduce (hamf-rf/indexed-long-accum
                  acc rel-idx row-idx
                (let [last-row-idx (aget last-data 0)
                      next-row-idx (aget last-data 1)
                      v (long (if (> row-idx last-row-idx)
                                (cond
                                  (== row-idx next-row-idx)
                                  (aget last-data 2)
                                  (< row-idx next-row-idx)
                                  -1
                                  :else
                                  (.invokePrim bin-search row-idx))
                                (.invokePrim bin-search row-idx)))]
                  (when (>= v 0)
                    (let [next-v (inc v)]
                      (aset last-data 0 row-idx)
                      (if (<= next-v dnidxs)
                        (do 
                          (aset last-data 1 (.getLong local-indexes next-v))
                          (aset last-data 2 next-v))
                        (do
                          (aset last-data 1 Long/MAX_VALUE)
                          (aset last-data 2 -1))))
                    (.add res-ixs rel-idx)
                    (.add res-vals (.get data v)))))
              nil rowidxs)
      (SparseCol. res-ixs res-vals res-rc metadata nil nil)))
  IMutList
  (size [this] (.size (dt-proto/->buffer this)))
  (get [this idx] (.get (dt-proto/->buffer this) idx))
  (getLong [this idx] (.getLong (dt-proto/->buffer this) idx))
  (getDouble [this idx] (.getDouble (dt-proto/->buffer this) idx))
  (valAt [this idx] (.valAt (dt-proto/->buffer this) idx))
  (valAt [this idx def-val] (.valAt (dt-proto/->buffer this) idx def-val))
  (invoke [this idx] (.invoke (dt-proto/->buffer this) idx))
  (invoke [this idx def-val] (.invoke (dt-proto/->buffer this) idx def-val))
  (meta [this] (assoc metadata
                      :datatype (dt-proto/elemwise-datatype data)
                      :n-elems rc))
  (withMeta [_this new-meta] (SparseCol. indexes data rc new-meta msng rdr))
  (nth [this idx] (nth (dt-proto/->buffer this) idx))
  (nth [this idx def-val] (nth (dt-proto/->buffer this) idx def-val))
  ;;should be the same as using hash-unorded-coll, effectively.
  (hasheq [this]   (.hasheq (dt-proto/->buffer this)))
  (equiv [this o] (if (identical? this o) true (.equiv (dt-proto/->buffer this) o)))
  (empty [this]
    (let [ba (ArrayLists/toList (byte-array 0))]
      (SparseCol. ba ba 0 {} nil nil)))
  ITypedReduce
  (reduce [this rfn acc] (reduce-scol this rfn acc 0 rc))
  (parallelReduction [this init-val-fn rfn merge-fn options]
    (.parallelReduction (dt-proto/->buffer this) init-val-fn rfn merge-fn options))
  clojure.lang.IKVReduce
  (kvreduce [this rfn acc]
    (kv-reduce-scol this rfn acc 0 rc))
  Object
  (toString [this]
    (let [format-str (if (> rc 20)
                       "#tech.v3.dataset.sparse-column<%s>%s\n%s\n[%s...]"
                       "#tech.v3.dataset.sparse-column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dt/elemwise-datatype this))
              [rc]
              (ds-proto/column-name this)
              (-> (dt-proto/sub-buffer this 0 (min 20 rc))
                  (dtype-pp/print-reader-data)))))
  (hashCode [this] (.hasheq (dt-proto/->buffer this)))
  (equals [this o] (.equiv this o)))

(defn- maybe-next-long
  ^long [^Iterator iter ^long not-found]
  (if (.hasNext iter)
    (long (.next iter))
    not-found))

(defn- maybe-next
  [^Iterator iter]
  (when (.hasNext iter) (.next iter)))

(defn scol-iter
  ^Iterator [^SparseCol scol ^long sidx ^long eidx]
  (let [src-idx-iter (.iterator ^Iterable (.-indexes scol))
        src-val-iter (.iterator ^Iterable (.-data scol))
        idx (long-array [sidx (maybe-next-long src-idx-iter Long/MAX_VALUE)])]
    (reify Iterator
      (hasNext [this] (< (aget idx 0) eidx))
      (next [this]
        (let [ii (aget idx 0)]
          (aset idx 0 (inc ii))
          (if (< ii eidx)
            (let [iv (aget idx 1)]
              (if (== iv ii)
                (do
                  (aset idx 1 (maybe-next-long src-idx-iter Long/MAX_VALUE))
                  (maybe-next src-val-iter))
                nil))
            (throw (java.util.NoSuchElementException.))))))))

(defn- reduce-scol
  [^SparseCol scol rfn acc sidx eidx]
  (let [^Iterator si (scol-iter scol sidx eidx)]
    (if (.hasNext si)
      (loop [acc (rfn acc (.next si))]
        (cond
          (reduced? acc) (deref acc)
          (.hasNext si) (recur (rfn acc (.next si)))
          :else acc))
      acc)))

(defn kv-reduce-scol
  [^SparseCol scol rfn acc sidx eidx]
  (let [^IMutList idxs (.-indexes scol)
        cmp (case (dt/elemwise-datatype idxs)
              :int8 it.unimi.dsi.fastutil.bytes.ByteComparators/NATURAL_COMPARATOR
              :int16 it.unimi.dsi.fastutil.shorts.ShortComparators/NATURAL_COMPARATOR
              :int32 it.unimi.dsi.fastutil.ints.IntComparators/NATURAL_COMPARATOR
              it.unimi.dsi.fastutil.longs.LongComparators/NATURAL_COMPARATOR)
        local-sidx (.binarySearch idxs sidx cmp)
        local-eidx (.binarySearch idxs eidx cmp)
        ^IMutList idxs (.subList idxs local-sidx local-eidx)
        ^IMutList data (.subList ^IMutList (.-data scol) local-sidx local-eidx)
        src-idx-iter (.iterator ^Iterable idxs)
        src-val-iter (.iterator ^Iterable data)]
    (if (.hasNext src-idx-iter)
      (let [next-i (maybe-next-long src-idx-iter Long/MAX_VALUE)]
        
        (loop [acc (rfn acc next-i (.next src-val-iter))]
          (cond
            (reduced? acc) (deref acc)
            (.hasNext src-val-iter) (let [next-i (maybe-next-long src-idx-iter Long/MAX_VALUE)]
                                      (recur (rfn acc next-i (.next src-val-iter))))
            :else acc)))
      acc)))

(defn ^:no-doc construct-sparse-col
  ^SparseCol [indexes data ^long rc metadata]
  (SparseCol. indexes data rc metadata nil nil))

(defn ->scol
  ^SparseCol [col]
  (cond
    (nil? col) (throw (RuntimeException. "Cannot create scol from nil."))
    (instance? SparseCol col) col
    :else
    (let [rc (dt/ecount col)
          valid-indexes (set/difference (bitmap/->bitmap (hamf/range rc)) (ds-proto/missing col))
          valid-indexes (ArrayLists/toList
                         (cond
                           (<= rc Byte/MAX_VALUE)
                           (hamf/byte-array valid-indexes)
                           (<= rc Short/MAX_VALUE)
                           (hamf/short-array valid-indexes)
                           (<= rc Integer/MAX_VALUE)
                           (hamf/int-array valid-indexes)
                           :else
                           (hamf/long-array valid-indexes)))
          col-dt (dt/elemwise-datatype col)
          buf-rdr (dt/->reader col)
          data (dt/make-container col-dt (.size valid-indexes))
          dst (dt/->buffer data)]
      (reduce (hamf-rf/indexed-long-accum _acc dst-idx src-idx
                (.writeObject dst dst-idx (.readObject buf-rdr src-idx)))
              nil valid-indexes)
      (SparseCol. valid-indexes data rc (meta col) nil nil))))

(defn scol->reader
  (^Buffer [^SparseCol scol] (scol->reader scol 0 (.-rc scol)))
  (^Buffer [^SparseCol scol ^long sidx ^long eidx]
   (let [rc (- eidx sidx)
         ^IMutList data (.-data scol)
         ^clojure.lang.IFn$LL idx-bin-search (index-bin-search (.-indexes scol))]
     (case (casting/simple-operation-space (dt/elemwise-datatype data))
       :int64
       (reify LongReader
         (lsize [this] rc)
         (readLong [this idx]
           (let [vidx (.invokePrim idx-bin-search (+ idx sidx))]
             (if (neg? vidx)
               Long/MIN_VALUE
               (.getLong data vidx))))
         (readObject [this idx]
           (let [vidx (.invokePrim idx-bin-search (+ idx sidx))]
             (if (neg? vidx)
               nil
               (.get data vidx))))
         (subBuffer [this ssidx seidx] (scol->reader scol (+ ssidx sidx) (+ seidx sidx)))
         (reduce [this rfn acc] (reduce-scol scol rfn acc sidx eidx)))
       :float64
       (reify DoubleReader
         (lsize [this] rc)
         (readDouble [this idx]
           (let [vidx (.invokePrim idx-bin-search (+ idx sidx))]
             (if (neg? vidx)
               Double/NaN
               (.getDouble data vidx))))
         (readObject [this idx]
           (let [vidx (.invokePrim idx-bin-search (+ idx sidx))]
             (if (neg? vidx)
               nil
               (.get data vidx))))
         (subBuffer [this ssidx seidx] (scol->reader scol (+ ssidx sidx) (+ seidx sidx)))
         (reduce [this rfn acc] (reduce-scol scol rfn acc sidx eidx)))
       (reify ObjectReader
         (lsize [this] rc)
         (readObject [this idx]
           (let [vidx (.invokePrim idx-bin-search (+ idx sidx))]
             (if (neg? vidx)
               nil
               (.get data vidx))))
         (subBuffer [this ssidx seidx] (scol->reader scol (+ ssidx sidx) (+ seidx sidx)))
         (reduce [this rfn acc] (reduce-scol scol rfn acc sidx eidx)))))))

(defn sparse-rows
  "Optimized implementation of ds/rows specifically assuming sparse columns"
  (^Buffer [ds] (sparse-rows ds 0 (ds-proto/row-count ds)))
  (^Buffer [ds ^long sidx ^long eidx]
   (let [rc (- eidx sidx)
         col-fns (mapv (fn [col]
                         (let [cbuf (dt/->buffer col)
                               cbuf (if (and (== sidx 0) (== eidx rc))
                                      cbuf
                                      (.subBuffer cbuf sidx eidx))
                               cname (ds-proto/column-name col)]
                           (fn [res  ^long row-idx]
                             (when-let [rv (.readObject cbuf row-idx)]
                               (.put ^Map res cname rv))
                             res)))
                       (.values ^Map ds))]
     (reify ObjectReader
       (lsize [this] rc)
       (readObject [this idx]
         (reduce (fn [acc cfn]
                   (.invokePrim ^clojure.lang.IFn$OLO cfn acc idx))
                 (hamf/linked-hashmap) col-fns))
       (subBuffer [this ssidx seidx]
         (ChunkedList/sublistCheck ssidx seidx rc)
         (sparse-rows ds (+ sidx ssidx) (+ sidx seidx)))))))

(defn ->sparse-ds
  ([ds] (->sparse-ds ds 0.5))
  ([ds missing-frac]
   (let [rc (long (ds-proto/row-count ds))
         missing-cutoff (long (* rc (double missing-frac)))]
     (reduce (fn [ds col]
               (if (>= (dt/ecount (ds-proto/missing col)) missing-cutoff)
                 (assoc ds (ds-proto/column-name col) (->scol col))
                 ds))
             ds (.values ^java.util.Map ds)))))

(defn is-sparse? [col] (instance? SparseCol col))

(comment
  (require '[tech.v3.dataset :as ds])
  (require '[tech.v3.libs.arrow])
  (def ds (ds/->dataset "../../nubank/dcm/mbrainz-big-final/6823d81f-ad0d-450e-90f9-efb1237575e8.arrow" {:key-fn keyword}))
  (def test-col (ds :release/name))
  (def test-scol (->scol test-col))
  (def sds (ds->sparse-ds ds))

  (def rr (ds/rows sds))
  (def sr (sparse-rows sds))
  (require '[criterium.core :as crit])
  (crit/quick-bench (reduce (fn [acc _] (inc acc)) 0 (ds/rows sds)))     ;;26ms
  (crit/quick-bench (reduce (fn [acc _] (inc acc)) 0 (ds/rows ds)))      ;;44ms
  (crit/quick-bench (reduce (fn [acc _] (inc acc)) 0 (sparse-rows sds))) ;;19ms

  (crit/quick-bench (-> (ds/select-rows ds (ds-proto/valid-rows (ds :release/name)))
                        (ds/remove-empty-columns)))
  (crit/quick-bench (-> (ds/select-rows sds (ds-proto/valid-rows (sds :release/name)))
                        (ds/remove-empty-columns)))

  (require '[clj-async-profiler.core :as prof])
  (prof/profile (dotimes [idx 10]
                  (-> (ds/select-rows sds (ds-proto/valid-rows (sds :release/name)))
                      (ds/remove-empty-columns))))
  (prof/serve-ui 8080)
  (dotimes [idx 1000]
    )
  
  )
