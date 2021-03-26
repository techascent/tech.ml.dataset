(ns tech.v3.dataset.reductions
  "Specific high performance reductions intended to be performend over a sequence
  of datasets.

  * `aggregate` - Perform a multi-dataset aggregation. Returns a dataset with row.
  * `group-by-column-agg` - Perform a multi-dataset group-by followed by
    an aggregation.  Returns a dataset with one row per key."
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
            [tech.v3.dataset.reductions.impl :as ds-reduce-impl]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype IndexReduction Buffer]
           [java.util Map Map$Entry HashMap List Set HashSet ArrayList]
           [java.util.concurrent ConcurrentHashMap ArrayBlockingQueue]
           [java.util.function BiFunction BiConsumer Function DoubleConsumer
            LongConsumer Consumer]
           [java.util.stream Stream]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype LongReader BooleanReader ObjectReader DoubleReader
            Consumers$StagedConsumer]
           [tech.v3.datatype DoubleConsumers$Sum DoubleConsumers$MinMaxSum]
           [it.unimi.dsi.fastutil.ints Int2ObjectMap
            Int2ObjectOpenHashMap]
           [clojure.lang IFn]
           [com.tdunning.math.stats TDigest])
  (:refer-clojure :exclude [distinct]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


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
  (ds-reduce-impl/staged-consumer-reducer :float64 colname
                #(DoubleConsumers$Sum.)
                #(get % :sum)))


(defn mean
  "Create a double consumer which will produce a mean of the column."
  [colname]
  (ds-reduce-impl/staged-consumer-reducer :float64 colname #(DoubleConsumers$Sum.)
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


(deftype BitmapConsumer [^RoaringBitmap bitmap]
  LongConsumer
  (accept [this lval]
    (.add bitmap (unchecked-int lval)))
  Consumers$StagedConsumer
  (combine [this other]
    (let [^BitmapConsumer other other]
      (-> (RoaringBitmap/or bitmap (.bitmap other))
          (BitmapConsumer.))))
  (value [this]
    bitmap))


(defn distinct-int32
  "Get the set of distinct items given you know the space is no larger than int32
  space.  The optional finalizer allows you to post-process the data."
  ([colname finalizer]
   (ds-reduce-impl/staged-consumer-reducer
    :int64 colname #(BitmapConsumer. (bitmap/->bitmap))
    (or finalizer identity)))
  ([colname]
   (distinct-int32 colname nil)))


(deftype SetConsumer [^{:unsynchronized-mutable true
                        :tag HashSet} data]
  Consumer
  (accept [this objdata]
    (.add data objdata))
  Consumers$StagedConsumer
  (combine [this other]
    (let [^SetConsumer other other]
      (.addAll ^HashSet (.clone data) (.data other))))
  (value [this] data))


(defn distinct
  "Create a reducer that will return a "
  ([colname finalizer]
   (ds-reduce-impl/staged-consumer-reducer
    :object colname #(SetConsumer. (HashSet.))
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


(defn- base-tdigest-reducer
  [colname compression]
  (reify
    IndexReduction
    (prepareBatch [this dataset]
      (dtype/->buffer (dataset colname)))
    (reduceIndex [this ds-ctx obj-ctx idx]
      (let [^Buffer ds-ctx ds-ctx
            ^TDigest ctx (or obj-ctx (TDigest/createMergingDigest compression))]
        (.add ctx (.readDouble ds-ctx idx))
        ctx))
    (reduceReductions [this lhs-ctx rhs-ctx]
      (.add ^TDigest lhs-ctx (java.util.Collections/singletonList rhs-ctx))
      lhs-ctx)
    (reduceReductionList [this list-data]
      (let [^TDigest digest (.get list-data 0)
            ^List rest-data (dtype/sub-buffer list-data 1)]
        (.add ^TDigest digest rest-data)
        digest))
    (finalize [this ctx]
      ctx)))


(deftype ^:private TDigestReducer
    [colname
     value-paths
     compression
     final-reduce-fn]
  ds-reduce-impl/PReducerCombiner
  (reducer-combiner-key [this]
    [colname :tdigest compression])
  (combine-reducers [reducer _combiner-key]
    (base-tdigest-reducer colname compression))
  (finalize-combined-reducer [this ctx]
    (let [ctx ^TDigest ctx]
      (final-reduce-fn (mapv (fn [path]
                               (case (first path)
                                 :cdf (.cdf ctx (double (second path)))
                                 :quantile (.quantile ctx (double (second path)))))
                             value-paths)))))


(defn prob-cdf
  "Probabilistic CDF using using tdunning/TDigest. Returns the fraction of all
  points added which are `<= cdf`.

  * `colname` - Column to run algorithm
  * `cdf` - cdf
  * `compression` - The compression parameter.  100 is a common value for normal uses.
  1000 is extremely large. The number of centroids retained will be a smallish (usually
  less than 10) multiple of this number."

  ([colname cdf compression]
   (TDigestReducer. colname [[:cdf (double cdf)]] compression first))
  ([colname cdf]
   (prob-cdf colname cdf 100)))


(defn prob-quantile
  "Probabilistic quantile using tdunning/TDigest. Returns an estimate of the cutoff
   such that a specified fraction of the data added to this TDigest would be less
   than or equal to the cutoff.

  * `colname` - Column to run algorithm
  * `quantile` - Specified fraction from 0.0-1.0.  0.5 returns the median.
  * `compression` - The compression parameter.  100 is a common value for normal uses.
  1000 is extremely large. The number of centroids retained will be a smallish (usually
  less than 10) multiple of this number."
  ([colname quantile compression]
   (TDigestReducer. colname [[:quantile (double quantile)]] compression first))
  ([colname quantile]
   (prob-quantile colname quantile 100)))


(defn prob-median
  "Probabilistic median using Use tdunning/TDigest.  See `prob-quartile`."
  ([colname compression]
   (prob-quantile colname 0.5 compression))
  ([colname] (prob-median colname 100)))


(defn prob-interquartile-range
    "Probabilistic interquartile range using tdunning/TDigest.  The interquartile
  range is defined as `(- (quartile 0.75) (quartile 0.25)).`

  See `prob-quartile`.
  "
  ([colname compression]
   (TDigestReducer. colname [[:quantile 0.75]
                             [:quantile 0.25]]
                    compression (fn [[third-q first-q]]
                                  (- (double third-q)
                                     (double first-q)))))
  ([colname]
   (prob-interquartile-range colname 100)))


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
         _ (ds-reduce-impl/group-by-column-aggregate-impl
            colname
            (ds-reduce-impl/aggregate-reducer (vals agg-map))
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
             (range (count cnames)) cnames (first results))
            (ds-impl/new-dataset {:dataset-name (str colname "-aggregation")})))))
  ([colname agg-map ds-seq]
   (group-by-column-agg colname agg-map nil ds-seq)))


(defn aggregate
  "Create a set of aggregate statistics over a sequence of datasets.  Returns a
  dataset with a single row and uses the same interface group-by-column-agg.

  Example:

```clojure
  (ds-reduce/aggregate
   {:n-elems (ds-reduce/row-count)
    :price-avg (ds-reduce/mean :price)
    :price-sum (ds-reduce/sum :price)
    :price-med (ds-reduce/prob-median :price)
    :price-iqr (ds-reduce/prob-interquartile-range :price)
    :n-dates (ds-reduce/count-distinct :date :int32)}
   ds-seq])
```"
  ([agg-map options ds-seq]
   (let [cnames (vec (keys agg-map))
         reducer (ds-reduce-impl/aggregate-reducer (vals agg-map))
         ctx-map (ConcurrentHashMap.)]
     (doseq [ds ds-seq]
       (let [batch-data (.prepareBatch reducer ds)]
         (parallel-for/indexed-map-reduce
          (ds-base/row-count ds)
          (fn [^long start-idx ^long group-len]
            (let [tid (.getId (Thread/currentThread))
                  end-idx (+ start-idx group-len)]
              (loop [idx start-idx
                     ctx (.get ctx-map tid)]
                (if (< idx end-idx)
                  (recur (unchecked-inc idx)
                         (.reduceIndex reducer batch-data ctx idx))
                  (.put ctx-map tid ctx))))))))
     (ds-io/->dataset [(->> (.values ctx-map)
                            (vec)
                            (.reduceReductionList reducer)
                            (.finalize reducer)
                            (map vector cnames)
                            (into {}))]
                      options)))
  ([agg-map ds-seq]
   (aggregate agg-map nil ds-seq)))


(comment
  (require '[tech.v3.dataset :as ds])
  (require '[tech.v3.datatype.datetime :as dtype-dt])
  (def stocks (-> (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
                  (ds/update-column :date #(dtype-dt/datetime->epoch :epoch-days %))))


  (aggregate
   {:n-elems (row-count)
    :price-avg (mean :price)
    :price-sum (sum :price)
    :price-med (prob-median :price)
    :price-iqr (prob-interquartile-range :price)
    :n-dates (count-distinct :date :int32)}
   [stocks stocks stocks])

  (group-by-column-agg
       :symbol
       {:symbol (first-value :symbol)
        :price-avg (mean :price)
        :price-sum (sum :price)
        :price-med (prob-median :price)}
       [stocks stocks stocks])

  )
