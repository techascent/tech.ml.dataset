(ns tech.v3.dataset.reductions
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.bitmap :as bitmap])
  (:import [tech.v3.datatype IndexReduction Buffer]
           [java.util Map Map$Entry HashMap List Set HashSet]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer Function DoubleConsumer
            LongConsumer Consumer]
           [java.util.stream Stream]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype DoubleReader Consumers$StagedConsumer]
           [tech.v3.datatype DoubleConsumers$Sum DoubleConsumers$MinMaxSum]
           [it.unimi.dsi.fastutil.ints Int2ObjectMap
            Int2ObjectOpenHashMap]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn group-by-column-aggregate
  "Perform a group by over a sequence of datasets where the reducer is handed each index and the
  context is stored in a map.  The reduction in this case is unordered so your indexes will
  arrive out of order.  The index-reductions's prepare-batch method will be called once with
  each dataset before iterating over that dataset's items.

  The return value is a parallel java stream made from the entrySet of the hash map.  It has
  an efficient further reduction to an array or you can process the stream yourself.

  Options:

  * `:skip-finalize?` - If skip-finalize? is true then the return value simply *is*
     the reduction map containing the non-finalized reducer data. It will be up the caller to call
     .finalize on the reducer for each java.util.Map$Entry value returned.   If finalize is true,
     then a parallel stream of tuples of k,v are returned."
  ([column-name ^IndexReduction reducer ds-seq {:keys [skip-finalize?]}]
   (let [reduction (ConcurrentHashMap.)
         _ (doseq [dataset ds-seq]
             (let [batch-data (.prepareBatch reducer dataset)]
               (dtype-reductions/unordered-group-by-reduce
                reducer batch-data (dataset column-name) reduction)))]
     (if skip-finalize?
       reduction
       (-> (.entrySet reduction)
           (.parallelStream)
           (.map (reify Function
                   (apply [this v]
                     (let [v ^Map$Entry v]
                       [(.getKey v)
                        (.finalize reducer (.getValue v))]))))))))
  ([column-name ^IndexReduction reducer ds-seq]
   (group-by-column-aggregate column-name reducer ds-seq nil)))


(defn stream->array
  "Convert a java stream into an object array."
  ^objects [^Stream data]
  (.toArray data))


(defn stream->vec
  "Convert a java stream into a persistent vector."
  ^List [^Stream data]
  (-> (.toArray data)
      vec))


(defn- as-double-consumer ^DoubleConsumer [item] item)
(defn- as-long-consumer ^LongConsumer [item] item)
(defn- as-consumer ^Consumer [item] item)
(defn- as-buffer ^Buffer [item] item)
(defn- as-staged-consumer ^Consumers$StagedConsumer [item] item)
(defn- as-list ^List [item] item)


(defmacro ^:private typed-accept
  [datatype consumer buffer idx]
  (case datatype
    :float64 `(.accept (as-double-consumer ~consumer)
                       (.readDouble (as-buffer ~buffer) ~idx))
    :int64 `(.accept (as-long-consumer ~consumer)
                     (.readLong (as-buffer ~buffer) ~idx))
    `(.accept (as-consumer ~consumer)
              (.readObject (as-buffer ~buffer) ~idx))))



(defmacro ^:private make-reducer
  [datatype colname-seq staged-consumer-constructor-fn]
  `(reify IndexReduction
    (prepareBatch [this# ds#]
      (mapv #(dtype/->reader (ds# %) ~datatype) ~colname-seq))
    (reduceIndex [this# readers# ctx# idx#]
      (let [readers# (as-list readers#)
            n-readers# (.size readers#)
            ctx# (typecast/as-object-array
                  (if ctx#
                    ctx#
                    (object-array (repeatedly n-readers# ~staged-consumer-constructor-fn))))]
        (dotimes [ary-idx# n-readers#]
          (typed-accept ~datatype
                        (aget ctx# ary-idx#)
                        (.get readers# ary-idx#)
                        idx#))
        ctx#))
    (reduceReductions [this# lhs# rhs#]
      (let [lhs# (typecast/as-object-array lhs#)
            rhs# (typecast/as-object-array rhs#)]
        (dotimes [idx# (alength lhs#)]
          (let [lhs-cons# (as-staged-consumer (aget lhs# idx#))
                rhs-cons# (as-staged-consumer (aget rhs# idx#))]
            (.inplaceCombine lhs-cons# rhs-cons#)))
        lhs#))
    (finalize [this# lhs#]
      (->> (map vector
                ~colname-seq
                (map (fn [data#]
                       (.value (as-staged-consumer data#)))
                     lhs#))
           (into {})))))


(defn double-reducer
  "Create an indexed reduction that uses double readers expects DoubleConsumers.
  constructor-fn will be used to create context-specific double consumers."
  ^IndexReduction [colname-seq staged-double-consumer-constructor-fn]
  (make-reducer :float64 colname-seq staged-double-consumer-constructor-fn))


(defn long-reducer
  "Create an indexed reduction that uses long readers expects LongConsumers.
  constructor-fn will be used to create context-specific long consumers."
  ^IndexReduction [colname-seq staged-long-consumer-constructor-fn]
  (make-reducer :int64 colname-seq staged-long-consumer-constructor-fn))


(defn object-reducer
  "Create an indexed reduction that uses object readers expects ObjectConsumers.
  constructor-fn will be used to create context-specific object consumers."
  ^IndexReduction [colname-seq staged-consumer-constructor-fn]
  (make-reducer :object colname-seq staged-consumer-constructor-fn))


(defn sum-consumer
  "Create a double consumer which will sum the values."
  []
  (DoubleConsumers$Sum.))


(defn min-max-sum-consumer
  "Create a double consumer which will perform min, max, and sum the values."
  []
  (DoubleConsumers$MinMaxSum.))


(deftype BitmapConsumer [^{:unsynchronized-mutable true
                           :tag RoaringBitmap} bitmap]
  LongConsumer
  (accept [this lval]
    (.add bitmap (unchecked-int lval)))
  Consumers$StagedConsumer
  (inplaceCombine [this other]
    (let [^BitmapConsumer other other]
      (.or bitmap (.bitmap other))))
  (value [this]
    bitmap))


(defn bitmap-consumer
  "Perform a consumer which aggregates to a RoaringBitmap."
  ^BitmapConsumer []
  (BitmapConsumer. (bitmap/->bitmap)))


(deftype SetConsumer [^{:unsynchronized-mutable true
                        :tag HashSet} data]
  Consumer
  (accept [this objdata]
    (.add data objdata))
  Consumers$StagedConsumer
  (inplaceCombine [this other]
    (let [^SetConsumer other other]
      (.addAll data (.data other))))
  (value [this] data))


(defn set-consumer
  "Create a consumer which aggregates to a HashSet."
  []
  ^Consumer (SetConsumer. (HashSet.)))


(defn aggregate-reducer
  "Create a reducer that aggregates to several other reducers.  Reducers are provided in a map
  of reducer-name->reducer and the result is a map of reducer-name -> finalized reducer value."
  ^IndexReduction [reducer-map]
  (let [reducer-names (keys reducer-map)
        reducer-seq (object-array (vals reducer-map))
        n-reducers (alength reducer-seq)]
    (reify IndexReduction
      (prepareBatch [this dataset]
        (object-array (map #(.prepareBatch ^IndexReduction % dataset) reducer-seq)))
      (reduceIndex [this ds-ctx obj-ctx idx]
        (let [^objects ds-ctx ds-ctx
              ^objects obj-ctx (if obj-ctx
                                 obj-ctx
                                 (object-array n-reducers))]
          (dotimes [r-idx n-reducers]
            (aset obj-ctx r-idx
                  (.reduceIndex ^IndexReduction (aget reducer-seq r-idx)
                                (aget ds-ctx r-idx)
                                (aget obj-ctx r-idx)
                                idx)))
          obj-ctx))
      (reduceReductions [this lhs-ctx rhs-ctx]
        (let [^objects lhs-ctx lhs-ctx
              ^objects rhs-ctx rhs-ctx]
          (dotimes [r-idx n-reducers]
            (aset lhs-ctx r-idx
                  (.reduceReductions ^IndexReduction (aget reducer-seq r-idx)
                                     (aget lhs-ctx r-idx)
                                     (aget rhs-ctx r-idx))))
          lhs-ctx))
      (finalize [this ctx]
        (let [^objects ctx ctx]
          (->> (map (fn [reducer-name reducer ctx]
                      [reducer-name
                       (.finalize ^IndexReduction reducer ctx)])
                    reducer-names
                    reducer-seq
                    ctx)
               (into {})))))))


(comment
  (require '[tech.v3.dataset :as ds])
  (require '[tech.v3.datatype.datetime :as dtype-dt])
  (def stocks (-> (ds/->dataset "test/data/stocks.csv")
                  (ds/update-column "date" #(dtype-dt/datetime->epoch :epoch-days %))))


  (-> (group-by-column-aggregate
       "symbol"
       (double-reducer ["price"] sum-consumer)
       [stocks stocks stocks])
      (stream->vec))


  (-> (group-by-column-aggregate
       "symbol"
       (long-reducer ["date"] bitmap-consumer)
       [stocks])
      (stream->vec))


  (-> (group-by-column-aggregate
       "symbol"
       (aggregate-reducer {:summations (double-reducer ["price"] min-max-sum-consumer)
                           :dates (long-reducer ["date"] bitmap-consumer)})
       [stocks stocks stocks])
      (stream->vec))


  )
