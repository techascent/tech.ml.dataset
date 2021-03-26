(ns tech.v3.dataset.reductions.impl
  "Helper namespace to help make reductions.clj more understandable."
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.base :as ds-base])
  (:import [java.util.function BiConsumer Function DoubleConsumer LongConsumer
            Consumer]
           [java.util Map$Entry List]
           [java.util.concurrent ConcurrentHashMap]
           [tech.v3.datatype IndexReduction Buffer Consumers$StagedConsumer
            ObjectReader]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ^:no-doc group-by-column-aggregate-impl
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
     :or {map-initial-capacity 100000
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


(defn as-double-consumer ^DoubleConsumer [item] item)
(defn as-long-consumer ^LongConsumer [item] item)
(defn as-consumer ^Consumer [item] item)
(defn as-buffer ^Buffer [item] item)
(defn as-staged-consumer ^Consumers$StagedConsumer [item] item)


(defmacro typed-accept
  [datatype consumer buffer idx]
  (case datatype
    :float64 `(.accept (as-double-consumer ~consumer)
                       (.readDouble (as-buffer ~buffer) ~idx))
    :int64 `(.accept (as-long-consumer ~consumer)
                     (.readLong (as-buffer ~buffer) ~idx))
    `(.accept (as-consumer ~consumer)
              (.readObject (as-buffer ~buffer) ~idx))))


(defmacro staged-consumer-reducer
  "Make an indexed reducer based on a staged-consumer."
  [datatype colname staged-consumer-constructor-fn finalize-fn]
  `(reify IndexReduction
     (prepareBatch [this# ds#]
       (dtype/->reader (ds-base/column ds# ~colname) ~datatype))
     (reduceIndex [this# reader# ctx# idx#]
       (let [ctx# (if ctx#
                    ctx#
                    (~staged-consumer-constructor-fn))]
         (typed-accept ~datatype ctx# reader# idx#)
         ctx#))
     (reduceReductions [this# lhs# rhs#]
       (let [lhs# (as-staged-consumer lhs#)
             rhs# (as-staged-consumer rhs#)]
         (.combine lhs# rhs#)))
     (reduceReductionList [this# item-list#]
       (if (== 1 (.size item-list#))
         (.get item-list# 0)
         (.combineList (as-staged-consumer (.get item-list# 0))
                       (dtype/sub-buffer item-list# 1))))
     (finalize [this# lhs#]
       (~finalize-fn
        (.value (as-staged-consumer lhs#))))))


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
    (.finalize ^IndexReduction this ctx)))


(defn aggregate-reducer
  "Create a reducer that aggregates to several other reducers.  Reducers are provided
  in a map of reducer-name->reducer and the result is a map of `reducer-name` ->
  `finalized` reducer value.

  This algorithm allows multiple input reducers to be combined into a single
  functional reducer for the reduction transparently from the outside caller."
  ^IndexReduction [reducer-seq]
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
        (object-array (map #(.prepareBatch ^IndexReduction % dataset) reducer-ary)))
      (reduceIndex [this ds-ctx obj-ctx idx]
        (let [^objects ds-ctx ds-ctx
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
      (reduceReductionList [this red-ctx-list]
        (let [n-inputs (.size red-ctx-list)]
          (if (== n-inputs 1)
            (first red-ctx-list)
            (let [retval (object-array n-reducers)]
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
