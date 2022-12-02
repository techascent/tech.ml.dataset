(ns tech.v3.dataset.reductions.impl
  "Helper namespace to help make reductions.clj more understandable."
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.sampling :as dt-sample]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.column :as ds-col]
            [ham-fisted.api :as hamf])
  (:import [java.util.function BiConsumer Function DoubleConsumer LongConsumer
            Consumer]
           [org.roaringbitmap RoaringBitmap]
           [java.util Map$Entry ArrayList List]
           [java.util.function LongSupplier LongPredicate]
           [java.util.concurrent ConcurrentHashMap]
           [tech.v3.datatype Buffer ObjectReader ECount UnaryPredicate]
           [ham_fisted IMutList Reducible]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


#_(defn ^:no-doc group-by-column-aggregate-impl
  "Perform a group by over a sequence of datasets where the reducer is handed each index
  and the context is stored in a map.  The reduction in this case is unordered so your
  indexes will arrive out of order.  The index-reductions's prepare-batch method will be
  called once with each dataset before iterating over that dataset's items.

  There are three possible return value types for this function.  Called with no options
  tuples of key to finalized value will be returned via a parallel java stream.  There is
  an option to pass in your own consumer so you your function will get called for every
  k,v tuple and finally there is an option to get the unfinalized ConcurrentHashMap.

  Options:

  * `:finalize-type` - One of three options, defaults to `:stream`.
       * `:stream` - The finalized results will be returned in the form of k,v tuples in a
          `java.util.stream.Stream`.
       * An instant of `clojure.lang.IFn` - This function, which must accept 2 arguments,
         will be called on each k,v pair and no value will be returned.
       * `:skip` - The entire java.util.ConcurrentHashMap will be returned with the value
         in an unfinalized state.  It will be up to the caller to call the reducer's
         finalize method on all the values.

  * `:map-initial-capacity` - initial capacity -- this can have a big effect on
    overall algorithm speed according to the
    [documentation](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html)."
  ([column-name
    ^IndexReduction reducer
    {:keys [finalize-type
            map-initial-capacity]
     :or {map-initial-capacity 10000
          finalize-type :stream}}
    ds-seq]
   (let [reduction (ConcurrentHashMap. (int map-initial-capacity))]
     (doseq [dataset ds-seq]
       (let [batch-data (.prepareBatch reducer dataset)]
         (dtype-reductions/unordered-group-by-reduce
          reducer batch-data (ds-base/column dataset column-name) reduction)))
     (cond
       (= :stream finalize-type)
       (-> (.entrySet reduction)
           (.parallelStream)
           (.map (reify Function
                   (apply [this v]
                     (let [v ^Map$Entry v]
                       [(.getKey v)
                        (.finalize reducer (.getValue v))])))))
       (= :skip finalize-type)
       reduction
       (instance? IFn finalize-type)
       (.forEach
        reduction
        1
        (reify BiConsumer
          (accept [this k v]
            (finalize-type k (.finalize reducer v)))))
       :else
       (errors/throwf "Unrecognized finalize-type: %s" finalize-type))))
  ([column-name ^IndexReduction reducer ds-seq]
   (group-by-column-aggregate-impl column-name reducer nil ds-seq)))


(defprotocol PReducerCombiner
  "Some reduces such as ones based on tdigest can be specified separately
  in the output map but actually for efficiency need be represented by 1
  concrete reducer."
  (reducer-combiner-key [reducer])
  (combine-reducers [reducer combiner-key]
    "Return a new reducer that has configuration indicated by the above
     combiner-key.")
  (finalize-combined-reducer [this ctx]))


(extend-protocol PReducerCombiner
  Object
  (reducer-combiner-key [reducer] nil)
  (finalize-combined-reducer [this ctx]
    (hamf-proto/finalize this ctx)))


(defrecord AggReducerContext [^objects red-ctx
                              ^UnaryPredicate filter-ctx])


#_(defn aggregate-reducer
  "Create a reducer that aggregates to several other reducers.  Reducers are provided
  in a map of reducer-name->reducer and the result is a map of `reducer-name` ->
  `finalized` reducer value.

  This algorithm allows multiple input reducers to be combined into a single
  functional reducer for the reduction transparently from the outside caller."
  ^IndexReduction [reducer-seq options]
  ;;We group reducers that can share a context.  In that case they mutably change
  ;;themselves such that they all share reduction state via the above
  ;;combine-reducers! API.
  (let [input-reducers (vec reducer-seq)
        combined-reducer-indexes
        (->> (map-indexed vector reducer-seq)
             (group-by (comp reducer-combiner-key second))
             (mapcat (fn [[red-key red-seq]]
                       (if (nil? red-key)
                         ;;Irreducable/non combinable
                         (map (fn [[red-idx red-obj]]
                                [red-obj [red-idx]])
                              red-seq)
                         ;;Combine all combinable into one reducer
                         (let [src-reducers (map second red-seq)
                               src-indexes (mapv first red-seq)]
                           [[(combine-reducers (first src-reducers) red-key)
                             src-indexes]])))))
        reducer-ary (object-array (map first combined-reducer-indexes))
        ;;map from reducer-idx->vector of input indexes
        reducer-indexes (map second combined-reducer-indexes)
        ;;get a vector of output indexes
        reverse-indexes (->>
                         (map-indexed (fn [out-idx in-idx-seq]
                                        (map vector in-idx-seq (repeat out-idx)))
                                      reducer-indexes)
                         (apply concat)
                         (sort-by first)
                         (mapv second))
        n-input-reducers (count input-reducers)
        n-reducers (alength reducer-ary)]
    (reify IndexReduction
      (prepareBatch [this dataset]
        (AggReducerContext.
         (object-array (map #(.prepareBatch ^IndexReduction % dataset) reducer-ary))
         (when-let [reduce-filter (get options :index-filter)]
           (argops/->unary-predicate (reduce-filter dataset)))))
      (indexFilter [this agg-ctx]
        (when-let [^UnaryPredicate ffn (.filter-ctx ^AggReducerContext agg-ctx)]
          (if (instance? LongPredicate ffn)
            ffn
            (reify LongPredicate
              (test [this idx] (.unaryLong ffn idx))))))
      (reduceIndex [this agg-ctx obj-ctx idx]
        (let [^AggReducerContext agg-ctx agg-ctx
              ^objects ds-ctx (.red-ctx agg-ctx)
              ^objects obj-ctx (if obj-ctx
                                 obj-ctx
                                 (object-array n-reducers))]
          (dotimes [r-idx n-reducers]
            (aset obj-ctx r-idx
                  (.reduceIndex ^IndexReduction (aget reducer-ary r-idx)
                                (aget ds-ctx r-idx)
                                (aget obj-ctx r-idx)
                                idx)))
          obj-ctx))
      (reduceReductions [this lhs-ctx rhs-ctx]
        (let [^objects lhs-ctx lhs-ctx
              ^objects rhs-ctx rhs-ctx]
          (dotimes [r-idx n-reducers]
            (aset lhs-ctx r-idx
                  (.reduceReductions ^IndexReduction (aget reducer-ary r-idx)
                                     (aget lhs-ctx r-idx)
                                     (aget rhs-ctx r-idx))))
          lhs-ctx))
      ;;reduce a list of reductions down to one reduction
      ;;the aggregate form of reduceReductions
      (reduceReductionList [this red-ctx-iterable]
        (let [iter (.iterator red-ctx-iterable)
              init-ctx (.next iter)]
          (if (not (.hasNext iter))
            init-ctx
            (let [retval (object-array n-reducers)
                  ^List red-ctx-list (hamf/->random-access red-ctx-iterable)
                  n-inputs (.size red-ctx-list)]
              (dotimes [r-idx n-reducers]
                (aset retval r-idx
                      (.reduceReductionList ^IndexReduction (aget reducer-ary r-idx)
                                            ;;in-place transpose the data
                                            (reify ObjectReader
                                              (lsize [this] n-inputs)
                                              (readObject [this idx]
                                                (aget ^objects (.get red-ctx-list idx)
                                                      r-idx))))))
              retval))))
      (finalize [this ctx]
        (let [^objects ctx ctx
              output-ary (object-array (count input-reducers))]
          (dotimes [idx n-input-reducers]
            (aset output-ary idx
                  (finalize-combined-reducer
                   (input-reducers idx)
                   (aget ctx (unchecked-int
                              (reverse-indexes idx))))))
          output-ary)))))


;; Reservoir dataset implementation
(defrecord ^:private ResDsCtx [^ArrayList new-columns
                               ^LongSupplier sampler])


(defrecord ^:private ResDsColData [col-data
                                   ^RoaringBitmap missing]
  ECount
  (lsize [_this] (.lsize ^ECount col-data)))


(defn- add-or-missing
  [^ResDsColData src-col ^long src-idx ^ResDsColData dst-col]
  (let [^Buffer dst-col-data (.col-data dst-col)]
    (if (.contains ^RoaringBitmap (.missing src-col) (unchecked-int src-idx))
      (do
        (.add ^RoaringBitmap (.missing dst-col) (.lsize dst-col-data))
        (.add dst-col-data
              (col-base/datatype->missing-value (.elemwiseDatatype dst-col-data))))
      (.add dst-col-data ((.col-data src-col) src-idx)))))


(defn- replace-or-missing
  [^ResDsColData src-col ^long src-idx
   ^ResDsColData dst-col ^long dst-idx]
  (let [^Buffer dst-col-data (.col-data dst-col)]
    (if (.contains ^RoaringBitmap (.missing src-col) (unchecked-int src-idx))
      (do
        (.add ^RoaringBitmap (.missing dst-col) (unchecked-int dst-idx))
        (.writeObject dst-col-data dst-idx
                      (col-base/datatype->missing-value
                       (.elemwiseDatatype dst-col-data))))
      (do
        (.remove ^RoaringBitmap (.missing dst-col) (unchecked-int dst-idx))
        (.writeObject dst-col-data dst-idx ((.col-data src-col) src-idx))))))


(defn reservoir-dataset
  "Create a new dataset consisting of these column names (or all the column names of
  the first dataset in the sequence).  New dataset will have at most reservoir-size
  rows.  There must be enough memory for N datasets during parallelized reduction
  step where N is at least n-cores or n-keys in the group-by map.

  Options are options for [double-reservoir](https://cnuernber.github.io/dtype-next/tech.v3.datatype.sampling.html#var-double-reservoir).

  Example:

```clojure
tech.v3.dataset.reductions> (group-by-column-agg
                             :symbol
                             {:symbol (first-value :symbol)
                              :n-elems (row-count)
                              :price-med (prob-median :price)
                              :stddev (reservoir-desc-stat :price 100 :standard-deviation)
                              :sub-stocks (reservoir-dataset 5)}
                             [stocks stocks stocks])
:symbol-aggregation [5 5]:

| :symbol | :n-elems |   :price-med |      :stddev |                         :sub-stocks |
|---------|----------|--------------|--------------|-------------------------------------|
|     IBM |      369 |  88.70468750 |  16.29366714 | _unnamed [5 3]:                     |
|         |          |              |              |                                     |
|         |          |              |              | \\| :symbol \\|   :date \\|  :price \\| |
|         |          |              |              | \\|---------\\|---------\\|---------\\| |
|         |          |              |              | \\|     IBM \\|   11992 \\|   79.16 \\| |
|         |          |              |              | \\|     IBM \\|   12022 \\|   70.58 \\| |
|         |          |              |              | \\|     IBM \\|   14610 \\|   121.9 \\| |
|         |          |              |              | \\|     IBM \\|   14214 \\|   82.15 \\| |
|         |          |              |              | \\|     IBM \\|   11292 \\|   76.47 \\| |
|    MSFT |      369 |  24.07277778 |   4.20544368 | _unnamed [5 3]:                     |
|         |          |              |              |                                     |
|         |          |              |              | \\| :symbol \\|   :date \\|  :price \\| |
|         |          |              |              | \\|---------\\|---------\\|---------\\| |
|         |          |              |              | \\|    MSFT \\|   12904 \\|   23.82 \\| |
|         |          |              |              | \\|    MSFT \\|   13604 \\|   28.30 \\| |
|         |          |              |              | \\|    MSFT \\|   12874 \\|   23.28 \\| |
|         |          |              |              | \\|    MSFT \\|   14610 \\|   28.05 \\| |
|         |          |              |              | \\|    MSFT \\|   12753 \\|   24.52 \\| |
|    AAPL |      369 |  37.05281250 |  61.61364337 | _unnamed [5 3]:                     |
|         |          |              |              |                                     |
|         |          |              |              | \\| :symbol \\|   :date \\|  :price \\| |
|         |          |              |              | \\|---------\\|---------\\|---------\\| |
|         |          |              |              | \\|    AAPL \\|   11566 \\|   7.760 \\| |
|         |          |              |              | \\|    AAPL \\|   14641 \\|   204.6 \\| |
|         |          |              |              | \\|    AAPL \\|   12173 \\|   8.980 \\| |
|         |          |              |              | \\|    AAPL \\|   14276 \\|   89.31 \\| |
|         |          |              |              | \\|    AAPL \\|   11869 \\|   7.630 \\| |
|    AMZN |      369 |  41.35142361 |  26.92738019 | _unnamed [5 3]:                     |
|         |          |              |              |                                     |
|         |          |              |              | \\| :symbol \\|   :date \\|  :price \\| |
|         |          |              |              | \\|---------\\|---------\\|---------\\| |
|         |          |              |              | \\|    AMZN \\|   13483 \\|   39.46 \\| |
|         |          |              |              | \\|    AMZN \\|   12570 \\|   54.40 \\| |
|         |          |              |              | \\|    AMZN \\|   14641 \\|   118.4 \\| |
|         |          |              |              | \\|    AMZN \\|   12935 \\|   33.09 \\| |
|         |          |              |              | \\|    AMZN \\|   11992 \\|   23.35 \\| |
|    GOOG |      204 | 422.69722222 | 132.23132957 | _unnamed [5 3]:                     |
|         |          |              |              |                                     |
|         |          |              |              | \\| :symbol \\|   :date \\|  :price \\| |
|         |          |              |              | \\|---------\\|---------\\|---------\\| |
|         |          |              |              | \\|    GOOG \\|   13787 \\|   707.0 \\| |
|         |          |              |              | \\|    GOOG \\|   14153 \\|   359.4 \\| |
|         |          |              |              | \\|    GOOG \\|   13634 \\|   497.9 \\| |
|         |          |              |              | \\|    GOOG \\|   12935 \\|   294.2 \\| |
|         |          |              |              | \\|    GOOG \\|   13269 \\|   371.8 \\| |
```

  "
  ([reservoir-size colnames options]
   (let [colnames-list (ArrayList.)
         columns (ArrayList.)
         _ (when (seq colnames)
             (.addAll colnames-list (vec (distinct colnames))))
         reservoir-size (long reservoir-size)]
     #_(reify IndexReduction
       (prepareBatch [this ds]
         (when (.isEmpty colnames-list)
           (.addAll colnames-list (ds-base/column-names ds)))
         (.clear columns)
         (doseq [colname colnames-list]
           (let [src-col (ds-base/column ds colname)]
             (.add columns (ResDsColData. (dtype/->reader src-col)
                                          (ds-col/missing src-col)))))
         columns)
       (reduceIndex [this ds ctx row-idx]
         (let [^ResDsCtx ctx
               (or ctx
                   (ResDsCtx. (let [new-columns (ArrayList.)]
                                (doseq [col columns]
                                  (.add new-columns
                                        (ResDsColData. (col-base/make-container
                                                        (dtype/elemwise-datatype col))
                                                       (RoaringBitmap.))))
                                new-columns)
                              (dt-sample/reservoir-sampler reservoir-size options)))
               ^ArrayList new-columns (.new-columns ctx)
               ^LongSupplier sampler (.sampler ctx)
               n-elems (.lsize ^ResDsColData (.get new-columns 0))]
           (if (< n-elems reservoir-size)
             (dotimes [col-idx (.size new-columns)]
               (add-or-missing (.get columns col-idx) row-idx
                               (.get new-columns col-idx)))
             (let [sample-idx (.getAsLong sampler)]
               (when (>= sample-idx 0)
                 (dotimes [col-idx (.size new-columns)]
                   (replace-or-missing (.get columns col-idx) row-idx
                                       (.get new-columns col-idx) sample-idx)))))
           ctx))
       (reduceReductions [this lhs-ctx rhs-ctx]
         (let [^ResDsCtx lhs-ctx lhs-ctx
               ^ResDsCtx rhs-ctx rhs-ctx]
           (let [^LongSupplier sampler (.sampler lhs-ctx)
                 ^ArrayList lhs-cols (.new-columns lhs-ctx)
                 ^ArrayList rhs-cols (.new-columns rhs-ctx)
                 n-cols (.size lhs-cols)
                 n-lhs (.lsize ^ResDsColData (.get lhs-cols (int 0)))
                 n-rhs (.lsize ^ResDsColData (.get rhs-cols (int 0)))
                 n-till-full (min (- reservoir-size n-lhs)
                                  n-rhs)
                 n-sample-rhs (- n-rhs n-till-full)
                 ^longs samples (let [samples (long-array n-sample-rhs)]
                                  (dotimes [sample-idx n-sample-rhs]
                                    (aset samples sample-idx (.getAsLong sampler)))
                                  samples)]
             (->> (range n-cols)
                  (pmap
                   ;;Merge the two datasets resampling as necessary.
                   (fn [col-idx]
                     (let [^ResDsColData lhs-col (.get lhs-cols (int col-idx))
                           ^ResDsColData rhs-col (.get rhs-cols (int col-idx))]
                       ;;Ensure lhs is full.
                       (dotimes [full-idx n-till-full]
                         (add-or-missing rhs-col full-idx lhs-col))
                       (dotimes [sample-idx n-sample-rhs]
                         ;;replace this index
                         (let [sidx (aget samples sample-idx)]
                           (when (>= sidx 0)
                             (replace-or-missing rhs-col (+ sample-idx n-till-full)
                                                 lhs-col sidx)))))))
                  (dorun)))
           lhs-ctx))
       (finalize [this ctx]
         (let [^ResDsCtx ctx ctx]
           (ds-impl/new-dataset options
                                (map (fn [colname ^ResDsColData col-data]
                                       #:tech.v3.dataset
                                       {:name colname
                                        :data (.col-data col-data)
                                        :missing (.missing col-data)
                                        ;;do *not* re-scan column to ascertain
                                        ;;datatype
                                        :force-datatype? true})
                                     colnames-list
                                     (.new-columns ctx))))))))
  ([reservoir-size colnames]
   (reservoir-dataset reservoir-size colnames nil))
  ([reservoir-size]
   (reservoir-dataset reservoir-size nil nil)))
