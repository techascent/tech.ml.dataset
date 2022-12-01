(ns tech.v3.dataset.reductions
  "Specific high performance reductions intended to be performend over a sequence
  of datasets.  This allows aggregations to be done in situations where the dataset is
  larger than what will fit in memory on a normal machine.  Due to this fact, summation
  is implemented using Kahan algorithm and various statistical methods are done in using
  statistical estimation techniques and thus are prefixed with `prob-` which is short
  for `probabilistic`.

  * `aggregate` - Perform a multi-dataset aggregation. Returns a dataset with row.
  * `group-by-column-agg` - Perform a multi-dataset group-by followed by
    an aggregation.  Returns a dataset with one row per key.

  Examples:

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tech.v3.datatype.datetime :as dtype-dt])
nil
user> (def stocks (-> (ds/->dataset \"test/data/stocks.csv\" {:key-fn keyword})
                      (ds/update-column :date #(dtype-dt/datetime->epoch :epoch-days %))))
#'user/stocks
user> (require '[tech.v3.dataset.reductions :as ds-reduce])
nil
user> (ds-reduce/group-by-column-agg
       :symbol
       {:symbol (ds-reduce/first-value :symbol)
        :price-avg (ds-reduce/mean :price)
        :price-sum (ds-reduce/sum :price)
        :price-med (ds-reduce/prob-median :price)}
       (repeat 3 stocks))
:symbol-aggregation [5 4]:

| :symbol |   :price-avg | :price-sum |   :price-med |
|---------|--------------|------------|--------------|
|     IBM |  91.26121951 |   33675.39 |  88.70468750 |
|    AAPL |  64.73048780 |   23885.55 |  37.05281250 |
|    MSFT |  24.73674797 |    9127.86 |  24.07277778 |
|    AMZN |  47.98707317 |   17707.23 |  41.35142361 |
|    GOOG | 415.87044118 |   84837.57 | 422.69722222 |
```

  * [zero-one benchmark winner](https://github.com/zero-one-group/geni-performance-benchmark/blob/da4d02e54de25a72214f72c4864ebd3d307520f8/dataset/src/dataset/optimised_by_chris.clj)"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.sampling :as dt-sample]
            [tech.v3.datatype.statistics :as dt-stats]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.reductions.impl :as ds-reduce-impl]
            [tech.v3.dataset.io.mapseq-colmap :as io-mapseq]
            [tech.v3.parallel.for :as parallel-for]
            [com.github.ztellman.primitive-math :as pmath]
            [ham-fisted.api :as hamf]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [tech.v3.datatype Buffer]
           [java.util List HashSet ArrayList LinkedHashMap Map]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function LongConsumer Consumer]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype LongReader BooleanReader ObjectReader DoubleReader
            FastStruct]
           [ham_fisted Sum Reducible Transformables Consumers$IncConsumer]
           [clojure.lang IDeref IFn$OLO IFn$ODO])
  (:refer-clojure :exclude [distinct]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn reducer->column-reducer
  ([reducer cname]
   (reducer->column-reducer reducer nil cname))
  ([reducer op-space cname]
   (fn [ds]
     (let [rfn (hamf-proto/->rfn reducer)
           op-space (or op-space
                        (cond
                          (instance? IFn$OLO rfn) :int64
                          (instance? IFn$ODO rfn) :float64
                          :else
                          :object))
           col (dtype/->reader (ds-base/column ds cname) op-space)
           merge-fn (hamf-proto/->merge-fn reducer)
           init-fn (hamf-proto/->init-val-fn reducer)]
       (case (casting/simple-operation-space op-space)
         :int64
         (let [rfn (Transformables/toLongReductionFn rfn)]
           (reify
             hamf-proto/Reducer
             (->init-val-fn [r] init-fn)
             (->rfn [r] (hamf/long-accumulator
                         acc v (.invokePrim rfn acc (.readLong col v))))
             (finalize [r v] (hamf-proto/finalize reducer v))
             hamf-proto/ParallelReducer
             (->merge-fn [r] merge-fn)))
         :float32
         (let [rfn (Transformables/toDoubleReductionFn rfn)]
           (reify
             hamf-proto/Reducer
             (->init-val-fn [r] init-fn)
             (->rfn [r] (hamf/long-accumulator
                         acc v (.invokePrim rfn acc (.readDouble col v))))
             (finalize [r v] (hamf-proto/finalize reducer v))
             hamf-proto/ParallelReducer
             (->merge-fn [r] merge-fn)))
         (reify
           hamf-proto/Reducer
           (->init-val-fn [r] init-fn)
           (->rfn [r] (hamf/long-accumulator
                       acc v (rfn acc (.readObject col v))))
           (finalize [r v] (hamf-proto/finalize reducer v))
           hamf-proto/ParallelReducer
           (->merge-fn [r] merge-fn)))))))

(defn first-value
  [colname]
  (fn [ds]
    (let [col (ds-base/column ds colname)]
      (reify
        hamf-proto/Reducer
        (->init-val-fn [r] (constantly nil))
        (->rfn [r] (hamf/long-accumulator
                    acc v (if (not acc) [(col v)] acc)))
        (finalize [r v] (first v) )
        hamf-proto/ParallelReducer
        (->merge-fn [r] (fn [l r] l))))))


(defn sum
  "Create a double consumer which will sum the values."
  [colname]
  (reducer->column-reducer (Sum.) colname))


(defn mean
  "Create a double consumer which will produce a mean of the column."
  [colname]
  (let [ds-fn (sum colname)]
    (fn [ds]
      (let [r (ds-fn ds)]
        (hamf/reducer-with-finalize
         r #(let [vv (deref %)] (/ (double (vv :sum)) (double (vv :n-elems)))))))))

(defn row-count
  "Create a simple reducer that returns the number of times reduceIndex was called."
  []
  (constantly (reify
                hamf-proto/Reducer
                (->init-val-fn [r] #(Consumers$IncConsumer.))
                (->rfn [r] hamf/consumer-accumulator)
                (finalize [r v] (deref v))
                hamf-proto/ParallelReducer
                (->merge-fn [r] hamf/reducible-merge))))


(deftype BitmapConsumer [^RoaringBitmap bitmap]
  LongConsumer
  (accept [_this lval]
    (.add bitmap (unchecked-int lval)))
  Reducible
  (reduce [this other]
    (let [^BitmapConsumer other other]
      (.or bitmap (.bitmap other))
      this))
  IDeref
  (deref [_this] bitmap))


(defn distinct-int32
  "Get the set of distinct items given you know the space is no larger than int32
  space.  The optional finalizer allows you to post-process the data."
  ([colname finalizer]
   (let [r (hamf/long-consumer-reducer #(BitmapConsumer. (bitmap/->bitmap)))
         r (if finalizer
             (hamf/reducer-with-finalize r (fn [b] (finalizer @b)))
             r)]
     (reducer->column-reducer r colname)))
  ([colname]
   (distinct-int32 colname nil)))


(deftype SetConsumer [^HashSet data]
  Consumer
  (accept [_this objdata]
    (.add data objdata))
  Reducible
  (reduce [this other]
    (let [^SetConsumer other other]
      (.addAll ^HashSet data (.data other))
      this))
  IDeref
  (deref [_this] data))


(defn distinct
  "Create a reducer that will return a set of values."
  ([colname finalizer]
   (let [r (hamf/consumer-reducer #(SetConsumer. (HashSet.)))
         r (if finalizer
             (hamf/reducer-with-finalize r (fn [b] (finalizer @b)))
             r)]
     (reducer->column-reducer r colname)))
  ([colname]
   (distinct colname nil)))


(defn count-distinct
  ([colname op-space]
   (case op-space
     :int32 (distinct-int32 colname dtype/ecount)
     :object (distinct colname dtype/ecount)))
  ([colname]
   (count-distinct colname :object)))


#_(defn prob-cdf
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


#_(defn prob-quantile
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


#_(defn prob-median
  "Probabilistic median using Use tdunning/TDigest.  See `prob-quartile`."
  ([colname compression]
   (prob-quantile colname 0.5 compression))
  ([colname] (prob-median colname 100)))


#_(defn prob-interquartile-range
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


#_(defn reservoir-desc-stat
  "Calculate a descriptive statistic using reservoir sampling.  A list of statistic
  names are found in `tech.v3.datatype.statistics/all-descriptive-stats-names`.
  Options are options used in
  [double-reservoir](https://cnuernber.github.io/dtype-next/tech.v3.datatype.sampling.html#var-double-reservoir).

  Note that this method will *not* convert datetime objects to milliseconds for you as
  in descriptive-stats."
  ([colname reservoir-size stat-name options]
   (reify
    ds-reduce-impl/PReducerCombiner
    (reducer-combiner-key [reducer] [colname :reservoir-stats reservoir-size options])
     (combine-reducers [reducer combiner-key]
       (let [reducer (dt-sample/reservoir-sampler reservoir-size
                                                  (assoc options :datatype :float64))]
         ;;reservoir sampling can sample any object type, so it produces a generic
         ;;consumer, not a double consumer
         (ds-reduce-impl/staged-consumer-reducer
          :object colname (hamf-proto/->init-val-fn reducer)
          identity)))
    (finalize-combined-reducer [this ctx]
      ((dt-stats/descriptive-statistics [stat-name] options @ctx) stat-name))))
  ([colname reservoir-size stat-name]
   (reservoir-desc-stat colname reservoir-size stat-name nil)))


#_(export-symbols tech.v3.dataset.reductions.impl
                reservoir-dataset)

#_(defn reducer
  "Make a group-by-agg reducer.

  * `column-name` - Single column name or multiple columns.
  * `per-elem-fn` - Called with the context as the first arg and each column's data
     as further arguments.
  * `finalize-fn` - finalize the result after aggregation.  Optional, will be replaced
     with identity of not provided."
  ([column-name per-elem-fn finalize-fn]
   (let [finalize-fn (or finalize-fn identity)
         column-names (if (= :scalar (argtypes/arg-type column-name))
                        [column-name]
                        (vec column-name))]
     (case (count column-names)
       1
       (reify IndexReduction
         (prepareBatch [this dataset]
           (dtype/->buffer (ds-base/column dataset (column-names 0))))
         (reduceIndex [this col obj-ctx idx]
           (per-elem-fn obj-ctx (.readObject ^Buffer col idx)))
         (reduceReductions [this lhs rhs]
           (throw (Exception. "Merge pathway not provided")))
         (finalize [this ctx]
           (finalize-fn ctx)))
       2
       (reify IndexReduction
         (prepareBatch [this dataset]
           (->> (map #(-> (ds-base/column dataset %)
                          (dtype/->buffer))
                     column-names)
                (object-array)))
         (reduceIndex [this columns obj-ctx idx]
           (let [^objects columns columns]
             (per-elem-fn obj-ctx
                          (.readObject ^Buffer (aget columns 0) idx)
                          (.readObject ^Buffer (aget columns 1) idx))))
         (reduceReductions [this lhs rhs]
           (throw (Exception. "Merge pathway not provided")))
         (finalize [this ctx]
           (finalize-fn ctx)))
       3
       (reify IndexReduction
         (prepareBatch [this dataset]
           (->> (map #(-> (ds-base/column dataset %)
                          (dtype/->buffer))
                     column-names)
                (object-array)))
         (reduceIndex [this columns obj-ctx idx]
           (let [^objects columns columns]
             (per-elem-fn obj-ctx
                          (.readObject ^Buffer (aget columns 0) idx)
                          (.readObject ^Buffer (aget columns 1) idx)
                          (.readObject ^Buffer (aget columns 2) idx))))
         (reduceReductions [this lhs rhs]
           (throw (Exception. "Merge pathway not provided")))
         (finalize [this ctx]
           (finalize-fn ctx)))
       (reify IndexReduction
         (prepareBatch [this dataset]
           (mapv #(-> (ds-base/column dataset %)
                      (dtype/->buffer))
                 column-names))
         (reduceIndex [this columns obj-ctx idx]
           (apply per-elem-fn obj-ctx
                  (sequence
                   (map #(.readObject ^Buffer % idx))
                   columns)))
         (reduceReductions [this lhs rhs]
           (throw (Exception. "Merge pathway not provided")))
         (finalize [this ctx]
           (finalize-fn ctx))))))
  ([column-name per-elem-fn]
   (reducer column-name per-elem-fn nil)))

(defn group-by-column-agg
  "Group a sequence of datasets by a column and aggregate down into a new dataset.

  * colname - Either a single scalar column name or a vector of column names to group by.

  * agg-map - map of result column name to reducer.  All values in the agg map must be
    functions from dataset to hamf (non-parallel) reducers.  Note that transducer-compatible
    rf's - such as kixi.mean, are valid hamf reducers.

  Options:

  * `:map-initial-capacity` - initial hashmap capacity.  Resizing hash-maps is expensive so we
     would like to set this to something reasonable.  Defaults to 10000.
  * `:index-filter` - A function that given a dataset produces a function from long index
    to boolean, ideally either nil or a java.util.function.LongPredicate.  Only indexes for
    which the index-filter returns true will be added to the aggregation.  For very large
    datasets, this is a bit faster than using filter before the aggregation.

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



tech.v3.dataset.reductions-test> (def tstds
                                   (ds/->dataset {:a [\"a\" \"a\" \"a\" \"b\" \"b\" \"b\" \"c\" \"d\" \"e\"]
                                                  :b [22   21  22 44  42  44   77 88 99]}))
#'tech.v3.dataset.reductions-test/tstds
tech.v3.dataset.reductions-test>  (ds-reduce/group-by-column-agg
                                   [:a :b] {:a (ds-reduce/first-value :a)
                                            :b (ds-reduce/first-value :b)
                                            :c (ds-reduce/row-count)}
                                   [tstds tstds tstds])
:tech.v3.dataset.reductions/_temp_col-aggregation [7 3]:

| :a | :b | :c |
|----|---:|---:|
|  a | 21 |  3 |
|  a | 22 |  6 |
|  b | 42 |  3 |
|  b | 44 |  6 |
|  c | 77 |  3 |
|  d | 88 |  3 |
|  e | 99 |  3 |
```"
  ([colname agg-map options ds-seq]
   (let [;;By default, we include the key-columns in the result.
         ;;Users can override this by passing in the column in agg-map
         agg-map (merge
                  (->> (if (sequential? colname)
                         colname
                         [colname])
                       (reduce (fn [agg-map cname]
                                 (assoc agg-map
                                        cname
                                        (first-value cname)))
                               {}))
                  agg-map)
         cnames (->> (hamf/keys agg-map)
                     (reduce (hamf/indexed-accum
                              acc idx v (.put ^Map acc v idx) acc)
                             (LinkedHashMap.)))
         reducers (hamf/vals agg-map)
         ;;allow a single dataset to be passed in without wrapping in seq notation
         ds-seq (if (ds-impl/dataset? ds-seq)
                  [ds-seq]
                  ds-seq)
         [colname ds-seq]
         (if (sequential? colname)
           (let [tmp-colname ::_temp_col]
             [tmp-colname
              (lznc/map
               #(assoc % tmp-colname
                       (ds-col/new-column
                        #:tech.v3.dataset{:name tmp-colname
                                          :data
                                          (-> (ds-base/select-columns % colname)
                                              (ds-readers/value-reader {:copying? true}))
                                          ;;no scanning for missing and no datatype
                                          ;;detection
                                          :missing (bitmap/->bitmap)
                                          :force-datatype? true}))
                   ds-seq)])
           [colname ds-seq])
         last-agg-reducer* (volatile! nil)
         result-map
         (reduce (fn [agg-map next-ds]
                   (let [group-col (dtype/->buffer (ds-base/column next-ds colname))
                         n-rows (ds-base/row-count next-ds)
                         idx-filter (when-let [filter-fn (get options :index-filter)]
                                      (filter-fn next-ds))
                         agg-reducer (vswap! last-agg-reducer*
                                             (constantly
                                              (->> (hamf/mapv #(% next-ds) reducers)
                                                   (hamf/compose-reducers
                                                    {:rfn-datatype :int64}))))
                         group-col (-> (ds-base/column next-ds colname)
                                       (dtype/->reader))]
                     (->> (if idx-filter
                            (lznc/filter idx-filter (hamf/range n-rows))
                            (hamf/range n-rows))
                          (hamf/group-by-reducer
                           (hamf/long->obj idx (.readObject group-col idx))
                           agg-reducer
                           {:map-fn (constantly agg-map)
                            :ordered? false
                            :skip-finalize? true
                            :min-n 1000}))))
                 (ConcurrentHashMap. (int (get options :map-initial-capacity 10000)))
                 ds-seq)
         finalize-fn (if-let [r @last-agg-reducer*]
                       (fn [map-val]
                         (FastStruct. cnames (hamf-proto/finalize r map-val)))
                       identity)
         ;;Create a parsing reducer
         ds-reducer (io-mapseq/mapseq-reducer nil)
         c ((hamf-proto/->init-val-fn ds-reducer))]
     ;;Also possible to parse N datasets in parallel and do a concat-copying
     ;;operation but in my experience this steps takes up nearly no time.
     (.forEach ^ConcurrentHashMap result-map 1000
               (hamf/bi-consumer
                k v
                (do
                  (let [vv (finalize-fn v)]
                      (locking c (.accept ^Consumer c vv))))))
     (hamf-proto/finalize ds-reducer c)))
  ([colname agg-map ds-seq]
   (group-by-column-agg colname agg-map nil ds-seq)))


#_(defn aggregate
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
         reducer (ds-reduce-impl/aggregate-reducer (vals agg-map) options)
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
    :n-dates (count-distinct :date :int32)
    :stddev (reservoir-desc-stat :price 100 :standard-deviation)
    :sub-stocks (reservoir-dataset 100)}
   [stocks stocks stocks])

  (group-by-column-agg
   :symbol
   {:symbol (first-value :symbol)
    :n-elems (row-count)
    :price-med (prob-median :price)
    :stddev (reservoir-desc-stat :price 100 :standard-deviation)
    :sub-stocks (reservoir-dataset 100)}
   [stocks stocks stocks])

  )
