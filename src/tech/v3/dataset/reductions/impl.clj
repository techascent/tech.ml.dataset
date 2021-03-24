(ns tech.v3.dataset.reductions.impl
  "Helper namespace to help make reductions.clj more understandable."
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.dataset.base :as ds-base])
  (:import [java.util.function BiConsumer Function DoubleConsumer LongConsumer
            Consumer]
           [java.util Map$Entry List]
           [java.util.concurrent ConcurrentHashMap]
           [tech.v3.datatype IndexReduction Buffer Consumers$StagedConsumer]
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
         (.inplaceCombine lhs# rhs#)
         lhs#))
     (finalize [this# lhs#]
       (~finalize-fn
        (.value (as-staged-consumer lhs#))))))


(defprotocol ^:private PReducerCombiner
  "Some reduces such as ones based on tdigest can be specified separately
  in the output map but actually for efficiency need be represented by 1
  concrete reducer."
  (reducer-combiner-key [reducer])
  (combine-reducers! [reducer grouped-reducers]
    "Return a new reducer that these reducers will now rely on that
will produce the data that these reducers will use in their finalize calls.")
  (set-combined-reducer! [reducer new-combined-reducer])
  (finalize-combined-reducer [this ctx]))


(extend-protocol PReducerCombiner
  Object
  (reducer-combiner-key [reducer] nil)
  (finalize-combined-reducer [this ctx]
    (.finalize ^IndexReduction this ctx)))
