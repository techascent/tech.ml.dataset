(ns tech.v3.dataset.reductions
  "Specific high performance reductions intended to be performend over a sequence
  of datasets.

  Currently there is one major reduction, `group-by-column-agg` which does
  a group-by operation followed by a user-defined aggregation
  into a new dataset."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype IndexReduction Buffer]
           [java.util Map Map$Entry HashMap List Set HashSet ArrayList]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer Function DoubleConsumer
            LongConsumer Consumer]
           [java.util.stream Stream]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype LongReader BooleanReader ObjectReader DoubleReader
            Consumers$StagedConsumer]
           [tech.v3.datatype DoubleConsumers$Sum DoubleConsumers$MinMaxSum]
           [it.unimi.dsi.fastutil.ints Int2ObjectMap
            Int2ObjectOpenHashMap]
           [clojure.lang IFn])
  (:refer-clojure :exclude [distinct]))


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


(defn first-value
  [colname]
  (reify IndexReduction
     (prepareBatch [this ds]
       (dtype/->reader (ds-base/column ds colname)))
    (reduceIndex [this batch-ctx ctx idx]
      (or ctx (batch-ctx idx)))
    (reduceReductions [this lhs rhs]
      lhs)))


(defn sum
  "Create a double consumer which will sum the values."
  [colname]
  (make-reducer :float64 colname
                #(DoubleConsumers$Sum.)
                #(get % :sum)))


(defn mean
  "Create a double consumer which will produce a mean of the column."
  [colname]
  (make-reducer :float64 colname #(DoubleConsumers$Sum.)
                #(pmath// (double (get % :sum))
                          (double (get % :n-elems)))))

(defn row-count
  "Create a simple reducer that returns the number of times reduceIndex was called."
  []
  (reify IndexReduction
    (reduceIndex [this batch-ctx ctx idx]
      (unchecked-inc (long (or ctx 0))))
    (reduceReductions [this lhs rhs]
      (pmath/+ (long lhs) (long rhs)))))


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


(defn distinct-int32
  "Get the set of distinct items given you know the space is no larger than int32
  space.  The optional finalizer allows you to post-process the data."
  ([colname finalizer]
   (make-reducer :int64 colname #(BitmapConsumer. (bitmap/->bitmap))
                 (or finalizer identity)))
  ([colname]
   (distinct-int32 colname nil)))


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


(defn distinct
  "Create a reducer that will return a "
  ([colname finalizer]
   (make-reducer :object colname #(SetConsumer. (HashSet.))
                 (or finalizer identity)))
  ([colname]
   (distinct colname nil)))


(defn count-distinct
  ([colname op-space]
   (case op-space
     :int32 (distinct-int32 colname dtype/ecount)
     :object (distinct colname dtype/ecount)))
  ([colname]
   (count-distinct colname :object)))


(defn- aggregate-reducer
  "Create a reducer that aggregates to several other reducers.  Reducers are provided in a map
  of reducer-name->reducer and the result is a map of reducer-name -> finalized reducer value."
  ^IndexReduction [reducer-seq]
  (let [reducer-seq (object-array reducer-seq)
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
          (dotimes [idx (alength ctx)]
            (let [^IndexReduction reducer (aget reducer-seq idx)
                  fin-val (.finalize ^IndexReduction reducer (aget ctx idx))]
              (aset ctx idx fin-val)))
          ctx)))))


(defn group-by-column-agg
  "Group a sequence of datasets by a column and aggregate down into a new dataset.

  * agg-map - map of result column name to reducer.  All values in the agg map must be
    instances of `tech.v3.datatype.IndexReduction`.  Column values will be inferred from
    the finalized result of the first reduction with nil indicating an object column.

  Options:

  * `:map-initial-capacity` - initial hashmap capacity.  Resizing hash-maps is expensive so we
     would like to set this to something reasonable.  Defaults to 100000.

  Example:

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tech.v3.dataset.reductions :as ds-reduce])
nil
user> (def stocks (ds/->dataset \"test/data/stocks.csv\" {:key-fn keyword}))
#'user/stocks
user> (ds-reduce/group-by-column-agg
       :symbol
       {:symbol (ds-reduce/first-value :symbol)
        :price-avg (ds-reduce/mean :price)
        :price-sum (ds-reduce/sum :price)}
       [stocks stocks stocks])
:symbol-aggregation [5 3]:

| :symbol |   :price-avg | :price-sum |
|---------|--------------|------------|
|    MSFT |  24.73674797 |    9127.86 |
|     IBM |  91.26121951 |   33675.39 |
|    AAPL |  64.73048780 |   23885.55 |
|    GOOG | 415.87044118 |   84837.57 |
|    AMZN |  47.98707317 |   17707.23 |
```"
  ([colname agg-map options ds-seq]
   (let [map-initial-capacity (long (get options :map-initial-capacity 100000))
         results (ArrayList. 100000)
         cnames (vec (keys agg-map))
         ;;group by using this reducer followed by this consumer fn.
         _ (group-by-column-aggregate-impl
            colname
            (aggregate-reducer (vals agg-map))
            ;;This is recommended but I didn't see a large result by setting it.
            (assoc options
                   :finalize-type
                   (fn [_column-value reduce-data]
                     (locking results
                       (.add results reduce-data))))
            ds-seq)
         ary-data (.toArray results)
         n-elems (alength ary-data)]
     (if (== 0 n-elems)
       nil
       ;;Transpose results in-place.
       (->> (map
             (fn [^long col-idx colname colval]
               ;;With a binary record type this operation could be nicer.
               ;;We create 'virtual' columns that we can randomly address into.
               (col-impl/new-column
                colname
                (case (casting/simple-operation-space (dtype/datatype colval))
                  ;;IDX in the code below means row-idx
                  :boolean (reify BooleanReader
                             (lsize [rdr] n-elems)
                             (readBoolean [rdr row-idx]
                               (boolean (aget ^objects (aget ary-data row-idx) col-idx))))
                  :int64 (reify LongReader
                           (lsize [rdr] n-elems)
                           (readLong [rdr row-idx]
                             (unchecked-long (aget ^objects (aget ary-data row-idx) col-idx))))
                  :float64 (reify DoubleReader
                             (lsize [rdr] n-elems)
                             (readDouble [rdr row-idx]
                               (unchecked-double (aget ^objects (aget ary-data row-idx) col-idx))))
                  (reify ObjectReader
                    (lsize [rdr] n-elems)
                    (readObject [rdr row-idx]
                      (aget ^objects (aget ary-data row-idx) col-idx))))
                nil
                (bitmap/->bitmap)))
             (range n-elems) cnames (first results))
            (ds-impl/new-dataset {:dataset-name (str colname "-aggregation")})))))
  ([colname agg-map ds-seq]
   (group-by-column-agg colname agg-map nil ds-seq)))


(comment
  (require '[tech.v3.dataset :as ds])
  (require '[tech.v3.datatype.datetime :as dtype-dt])
  (def stocks (-> (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
                  (ds/update-column :date #(dtype-dt/datetime->epoch :epoch-days %))))


  (group-by-column-agg
   :symbol
   {:n-elems (row-count)
    :price-avg (mean :price)
    :price-sum (sum :price)
    :symbol (first-value :symbol)
    :n-dates (count-distinct :date :int32)}
   [stocks stocks stocks])


  )
