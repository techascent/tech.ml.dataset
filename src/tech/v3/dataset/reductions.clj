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
            [tech.v3.parallel.for :as parallel-for]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype IndexReduction Buffer]
           [java.util List HashSet ArrayList]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function LongConsumer Consumer]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype LongReader BooleanReader ObjectReader DoubleReader
            Consumers$StagedConsumer]
           [tech.v3.datatype DoubleConsumers$Sum]
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
      (unchecked-inc (unchecked-long (or ctx 0))))
    (reduceReductions [this lhs rhs]
      (pmath/+ (unchecked-long lhs) (unchecked-long rhs)))))


(deftype BitmapConsumer [^RoaringBitmap bitmap]
  LongConsumer
  (accept [_this lval]
    (.add bitmap (unchecked-int lval)))
  Consumers$StagedConsumer
  (combine [_this other]
    (let [^BitmapConsumer other other]
      (-> (RoaringBitmap/or bitmap (.bitmap other))
          (BitmapConsumer.))))
  (value [_this]
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
  (accept [_this objdata]
    (.add data objdata))
  Consumers$StagedConsumer
  (combine [_this other]
    (let [^SetConsumer other other]
      (.addAll ^HashSet (.clone data) (.data other))))
  (value [_this] data))


(defn distinct
  "Create a reducer that will return a set of values."
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
  (reducer-combiner-key [_this]
    [colname :tdigest compression])
  (combine-reducers [_reducer _combiner-key]
    (base-tdigest-reducer colname compression))
  (finalize-combined-reducer [_this ctx]
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


(defn reservoir-desc-stat
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
      (ds-reduce-impl/staged-consumer-reducer
       :float64 colname #(dt-sample/double-reservoir reservoir-size options)
       identity))
    (finalize-combined-reducer [this ctx]
      ((dt-stats/descriptive-statistics [stat-name] options @ctx) stat-name))))
  ([colname reservoir-size stat-name]
   (reservoir-desc-stat colname reservoir-size stat-name nil)))


(export-symbols tech.v3.dataset.reductions.impl
                reservoir-dataset)

(defn reducer
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
    instances of `tech.v3.datatype.IndexReduction`.  Column values will be inferred from
    the finalized result of the first reduction with nil indicating an object column.

  Options:

  * `:map-initial-capacity` - initial hashmap capacity.  Resizing hash-maps is expensive so we
     would like to set this to something reasonable.  Defaults to 100000.
  * `:index-filter` - A function that given a dataset produces a function from long index
    to boolean.  Only indexes for which the index-filter returns true will be added to the
    aggregation.  For very large datasets, this is a bit faster than using filter before
    the aggregation.

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
   (let [results (ArrayList. 10000)
         ;;By default, we include the key-columns in the result.
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
         cnames (vec (keys agg-map))

         ;;allow a single dataset to be passed in without wrapping in seq notation
         ds-seq (if (ds-impl/dataset? ds-seq)
                  [ds-seq]
                  ds-seq)
         src-colname colname
         [colname ds-seq]
         (if (and (sequential? colname)
                  (nil? ((first ds-seq) colname)))
           (let [tmp-colname ::_temp_col]
             [tmp-colname
              (map #(assoc % tmp-colname
                           (ds-col/new-column
                            #:tech.v3.dataset{:name tmp-colname
                                              :data
                                              (-> (ds-base/select-columns % colname)
                                                  (ds-readers/value-reader {:copying? true}))
                                              :metadata {}
                                              :missing (bitmap/->bitmap)
                                              :force-datatype? true}))
                   ds-seq)])
           [colname ds-seq])
         ;;ensure the unique columns are in the result

         ;;group by using this reducer followed by this consumer fn.
         _ (ds-reduce-impl/group-by-column-aggregate-impl
            colname
            (ds-reduce-impl/aggregate-reducer (vals agg-map) options)
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
            (ds-impl/new-dataset {:dataset-name (str src-colname "-aggregation")})))))
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


(defn index-reducer
  "Define an indexed reducer which is used as the values in the group-by-agg and aggregate
  reduction maps.

  * `per-idx-fn` - This gets passed three arguments - the batch-ctx which defaults to
  the current dataset, the reduction context, and the row index.  It is expected to
  return a new reduction context.  The first time this is called for a given reduction
  bucket, the reduction context will be nil.

  Options:

  * `:per-batch-fn` - This is called once per dataset and the return value of this is passed
    as the first argument to `per-idx-fn`.
  * `:merge-fn` - For reductions are done with per-thread contexts, the final step is to merge
    the per-thread contexts back into one final context.  This is **not** currently used for
    `group-by-agg` but it is used for `aggregate`.
  * `:finalize` - Optional argument to perform a final pass on the result of the reduction
    before it is set as the row value for the column.

  Example:

```clojure
user> (def stocks (ds/->dataset \"test/data/stocks.csv\"))
#'user/stocks
user> (ds-reduce/group-by-column-agg
       \"symbol\"
       {:symbol (ds-reduce/first-value \"symbol\")
        :price-sum (ds-reduce/index-reducer
                    (fn [ds obj-ctx ^long row-idx]
                      (+ (or obj-ctx 0.0)
                         ((ds \"price\") row-idx))))}
       [stocks])
symbol-aggregation [5 2]:

| :symbol | :price-sum |
|---------|-----------:|
|    AAPL |    7961.85 |
|     IBM |   11225.13 |
|    AMZN |    5902.41 |
|    MSFT |    3042.62 |
|    GOOG |   28279.19 |
```"
  (^IndexReduction
   [per-idx-fn {:keys [per-batch-fn merge-fn finalize-fn]
                :or {per-batch-fn identity
                     merge-fn #(throw (Exception. "Merge function not defined for reducer"))
                     finalize-fn identity}}]
   (reify IndexReduction
     (prepareBatch [this dataset] (per-batch-fn dataset))
     (reduceIndex [this batch-ctx obj-ctx row-idx] (per-idx-fn batch-ctx obj-ctx row-idx))
     (reduceReductions [this lhs rhs] (merge-fn lhs rhs))
     (finalize [this ctx] (finalize-fn ctx))))
  (^IndexReduction
   [per-idx-fn]
   (index-reducer per-idx-fn nil)))


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
