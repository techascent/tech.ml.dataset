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
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.io.mapseq-colmap :as io-mapseq]
            [tech.v3.dataset.reductions.impl :as impl]
            [tech.v3.dataset.reductions.apache-data-sketch :as sketch]
            [tech.v3.parallel.for :as parallel-for]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [tech.v3.datatype Buffer]
           [java.util List HashSet ArrayList LinkedHashMap Map]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function LongConsumer Consumer LongPredicate]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype LongReader BooleanReader ObjectReader DoubleReader
            FastStruct]
           [ham_fisted Sum Reducible Transformables Consumers$IncConsumer]
           [clojure.lang IDeref IFn$OLO IFn$ODO])
  (:refer-clojure :exclude [distinct]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn reducer->column-reducer
  "Given a hamf parallel reducer and a column name, return a dataset reducer of one column."
  ([reducer cname]
   (reducer->column-reducer reducer nil cname))
  ([reducer op-space cname]
   (impl/reducer->column-reducer reducer op-space cname)))


(defn first-value
  [colname]
  (reify ds-proto/PDatasetReducer
    (ds->reducer [this ds]
      (let [col (ds-base/column ds colname)]
        (reify
          hamf-proto/Reducer
          (->init-val-fn [r] (constantly nil))
          (->rfn [r] (hamf-rf/long-accumulator
                      acc v (if (not acc) [(col v)] acc)))
          hamf-proto/ParallelReducer
          (->merge-fn [r] (fn [l r] l)))))
    (merge [this l r] l)
    hamf-proto/Finalize
    (finalize [this v] (first v))))


(defn sum
  "Create a double consumer which will sum the values."
  [colname]
  (reducer->column-reducer (hamf-rf/reducer-with-finalize
                            (Sum.)
                            #((deref %) :sum))
                           colname))


(defn mean
  "Create a double consumer which will produce a mean of the column."
  [colname]
  (let [ds-fn (sum colname)]
    (reducer->column-reducer (hamf-rf/reducer-with-finalize
                              (Sum.)
                              #(let [m (deref %)]
                                 (/ (double (m :sum))
                                    (double (m :n-elems)))))
                             colname)))

(defn row-count
  "Create a simple reducer that returns the number of times reduceIndex was called."
  []
  (reify ds-proto/PDatasetReducer
    (ds->reducer [this ds]
      (hamf-rf/consumer-reducer #(Consumers$IncConsumer.)))
    (merge [this l r] (hamf-rf/reducible-merge l r))
    hamf-proto/Finalize
    (finalize [this v] (deref v))))


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
   (let [r (hamf-rf/long-consumer-reducer #(BitmapConsumer. (bitmap/->bitmap)))
         r (if finalizer
             (hamf-rf/reducer-with-finalize r (fn [b] (finalizer @b)))
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
   (let [r (hamf-rf/consumer-reducer #(SetConsumer. (HashSet.)))
         r (if finalizer
             (hamf-rf/reducer-with-finalize r (fn [b] (finalizer @b)))
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

(defn prob-set-cardinality
  "See docs for [[tech.v3.dataset.reductions.apache-data-sketch/prob-set-cardinality]].

  Options:

  * `:hll-lgk` - defaults to 12, this is log-base2 of k, so k = 4096. lgK can be
           from 4 to 21.
  * `:hll-type` - One of #{4,6,8}, defaults to 8.  The HLL_4, HLL_6 and HLL_8
           represent different levels of compression of the final HLL array where the
           4, 6 and 8 refer to the number of bits each bucket of the HLL array is
           compressed down to. The HLL_4 is the most compressed but generally slightly
           slower than the other two, especially during union operations.
  * `:datatype` - One of :float64, :int64, :string"
  ([colname options] (sketch/prob-set-cardinality colname options))
  ([colname] (sketch/prob-set-cardinality colname)))

(defn prob-cdf
  "See docs for [[tech.v3.dataset.reductions.apache-data-sketch/prob-cdf]]

  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname cdf] (sketch/prob-cdf colname cdf))
  ([colname cdf k] (sketch/prob-cdf colname cdf k)))

(defn prob-quantile
  "See docs for [[tech.v3.dataset.reductions.apache-data-sketch/prob-quantile]]

  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname quantile] (sketch/prob-quantile colname quantile))
  ([colname quantile k] (sketch/prob-quantile colname quantile k)))

(defn prob-median
  "See docs for [[tech.v3.dataset.reductions.apache-data-sketch/prob-median]]

  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname] (sketch/prob-median colname))
  ([colname k] (sketch/prob-median colname k)))

(defn prob-interquartile-range
  "See docs for [[tech.v3.dataset.reductions.apache-data-sketch/prob-interquartile-range

  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname k] (sketch/prob-interquartile-range colname k))
  ([colname] (sketch/prob-interquartile-range colname)))

(defn reservoir-desc-stat
  "Calculate a descriptive statistic using reservoir sampling.  A list of statistic
  names are found in `tech.v3.datatype.statistics/all-descriptive-stats-names`.
  Options are options used in
  [reservoir-sampler](https://cnuernber.github.io/dtype-next/tech.v3.datatype.sampling.html#var-double-reservoir).

  Note that this method will *not* convert datetime objects to milliseconds for you as
  in descriptive-stats."
  ([colname reservoir-size stat-name options]
   (let [col-reducer (reducer->column-reducer
                      (dt-sample/reservoir-sampler reservoir-size
                                     (assoc options :datatype :float64))
                      colname)]
     (reify
       ds-proto/PDatasetReducer
       (ds->reducer [this ds] (ds-proto/ds->reducer col-reducer ds))
       (merge [this lhs rhs]
         (reduce hamf-rf/double-consumer-accumulator
                 lhs
                 @rhs))
       hamf-proto/Finalize
       (finalize [this ctx]
         ((dt-stats/descriptive-statistics @ctx [stat-name] options) stat-name))
       ds-proto/PReducerCombiner
       (reducer-combiner-key [reducer] [colname :reservoir-stats reservoir-size options]))))
  ([colname reservoir-size stat-name]
   (reservoir-desc-stat colname reservoir-size stat-name nil)))


(defn reservoir-dataset
  ([reservoir-size] (reservoir-dataset reservoir-size nil))
  ([reservoir-size options]
   (let [sampler-reducer (dt-sample/reservoir-sampler reservoir-size
                                                      (assoc options :datatype :object))
         sampler-rfn (hamf-proto/->rfn sampler-reducer)
         merge-fn (hamf-proto/->merge-fn sampler-reducer)]
     (reify
       ds-proto/PDatasetReducer
       (ds->reducer [this ds]
         (let [^Buffer rows (ds-proto/rows ds {:copying? true})]
           (reify
             hamf-proto/Reducer
             (->init-val-fn [r] (hamf-proto/->init-val-fn sampler-reducer))
             (->rfn [r] (hamf-rf/long-accumulator
                         acc idx (sampler-rfn acc (.readObject rows idx))))
             hamf-proto/ParallelReducer
             (->merge-fn [r] merge-fn))))
       (merge [this lhs rhs] (merge-fn lhs rhs))
       hamf-proto/Finalize
       (finalize [this ctx] (ds-io/->dataset @ctx options))))))


(defn reducer
  "Make a group-by-agg reducer.

  * `column-name` - Single column name or multiple columns.
  * `init-val-fn` - Function to produce initial accumulators
  * `rfn` - Function that takes the accumulator and each column's data as
     as further arguments.  For a single-column pathway this looks like a normal clojure
     reduction function but for two columns it gets extra arguments.
  * `merge-fn` - Function that takes two accumulators and merges them.  Merge is not required
    for [[group-by-column-agg]] but it *is* required for [[aggregate]].
  * `finalize-fn` - finalize the result after aggregation.  Optional, will be replaced
     with identity of not provided."
  ([column-name init-val-fn rfn merge-fn finalize-fn]
   (let [finalize-fn (or finalize-fn #(rfn %))
         init-val-fn (or init-val-fn #(rfn))
         column-names (if (= :scalar (argtypes/arg-type column-name))
                        [column-name]
                        (vec column-name))
         merge-fn (or merge-fn #(rfn %1 %2))]
     (reify ds-proto/PDatasetReducer
       (ds->reducer [this ds]
         (let [rvecs (-> (ds-proto/select-columns ds column-names)
                         (ds-proto/rowvecs {:copying? true}))]
           (reify
             hamf-proto/Reducer
             (->init-val-fn [r] init-val-fn)
             (->rfn [r] (hamf-rf/long-accumulator
                         acc row-idx
                         (apply rfn acc (.readObject rvecs row-idx))))
             hamf-proto/ParallelReducer
             (->merge-fn [r] merge-fn))))
       (merge [this lhs rhs] (merge-fn lhs rhs))
       hamf-proto/Finalize
       (finalize [this ctx] (finalize-fn ctx)))))
  ([column-name rfn]
   (reducer column-name rfn rfn
            rfn rfn)))


(defn- combine-reducers
  [reducers]
  (let [reducer->indexes (->> reducers
                              (map-indexed vector)
                              (hamf/group-by (comp ds-proto/reducer-combiner-key second))
                              (mapcat (fn [[ckey reducer-seq]]
                                        (if (nil? ckey)
                                          (lznc/map (fn [[ridx reducer]]
                                                      [reducer [ridx]])
                                                    reducer-seq)
                                          (let [reducer (second (first reducer-seq))]
                                            [[reducer (hamf/mapv first reducer-seq)]])))))]
    [(mapv first reducer->indexes)
     (->> (lznc/map second reducer->indexes)
          (lznc/map-indexed (fn [dst-idx src-indexes]
                              (map #(vector % dst-idx) src-indexes)))
          (apply lznc/concat)
          (hamf/sort-by first)
          (hamf/mapv second))]))


(defn- finalize-combined-reducers
  [reducers rev-indexes cnames ^objects reduced-values]
  (->> reducers
       (lznc/map-indexed
        (fn [^long src-idx reducer]
          (hamf-proto/finalize
           reducer (aget reduced-values (long (.get ^List rev-indexes src-idx))))))
       (hamf/vec)
       (FastStruct. cnames)))


(defn group-by-column-agg-rf
  "Produce a transduce-compatible rf that will perform the group-by-column-agg pathway.
  See documentation for [[group-by-column-agg]].

```clojure
tech.v3.dataset.reductions-test> (def stocks (ds/->dataset \"test/data/stocks.csv\" {:key-fn keyword}))
#'tech.v3.dataset.reductions-test/stocks
tech.v3.dataset.reductions-test> (transduce (map identity)
                                            (ds-reduce/group-by-column-agg-rf
                                             :symbol
                                             {:n-elems (ds-reduce/row-count)
                                              :price-avg (ds-reduce/mean :price)
                                              :price-sum (ds-reduce/sum :price)
                                              :symbol (ds-reduce/first-value :symbol)
                                              :n-dates (ds-reduce/count-distinct :date :int32)}
                                             {:index-filter (fn [dataset]
                                                              (let [rdr (dtype/->reader (dataset :price))]
                                                                (hamf/long-predicate
                                                                 idx (> (.readDouble rdr idx) 100.0))))})
                                            [stocks stocks stocks])
_unnamed [4 5]:

| :symbol | :n-elems |   :price-avg | :price-sum | :n-dates |
|---------|---------:|-------------:|-----------:|---------:|
|    AAPL |       93 | 160.19096774 |   14897.76 |       31 |
|     IBM |      120 | 111.03775000 |   13324.53 |       40 |
|    AMZN |       18 | 126.97833333 |    2285.61 |        6 |
|    GOOG |      204 | 415.87044118 |   84837.57 |       68 |
```"
  ([colname agg-map]
   (group-by-column-agg-rf colname agg-map nil))
  ([colname agg-map options]
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
                     (reduce (hamf-rf/indexed-accum
                              acc idx v (.put ^Map acc v idx) acc)
                             (LinkedHashMap.)))
         ;;convert reducers to something with lightning fast reduction.
         reducers (hamf/object-array-list (hamf/vals agg-map))
         [combined-reducers rev-indexes] (combine-reducers reducers)
         [colname ds-map-fn]
         (if (sequential? colname)
           (let [tmp-colname ::_temp_col]
             [tmp-colname
              #(assoc % tmp-colname
                      (ds-col/new-column
                       #:tech.v3.dataset{:name tmp-colname
                                         :data
                                         (-> (ds-base/select-columns % colname)
                                             (ds-readers/value-reader {:copying? true}))
                                         ;;no scanning for missing and no datatype
                                         ;;detection
                                         :missing (bitmap/->bitmap)
                                         :force-datatype? true}))])
           [colname identity])
         finalize-fn #(finalize-combined-reducers reducers rev-indexes cnames %)]
     (fn
       ([] (ConcurrentHashMap. (int (get options :map-initial-capacity 10000))))
       ([agg-map next-ds]
        (let [next-ds (ds-map-fn next-ds)
              group-col (dtype/->buffer (ds-base/column next-ds colname))
              n-rows (ds-base/row-count next-ds)
              ^LongPredicate idx-filter (when-let [filter-fn (get options :index-filter)]
                                          (let [idx-filter (filter-fn next-ds)]
                                            (if (instance? LongPredicate idx-filter)
                                              idx-filter
                                              (reify LongPredicate
                                                (test [this v] (boolean (idx-filter v)))))))
              agg-reducer (->> combined-reducers
                               (hamf/mapv #(ds-proto/ds->reducer % next-ds))
                               (hamf-rf/compose-reducers {:rfn-datatype :int64}))
              group-col (-> (ds-base/column next-ds colname)
                            (dtype/->reader))
              agg-init (hamf-proto/->init-val-fn agg-reducer)
              agg-rfn (hamf-proto/->rfn agg-reducer)]
          (dorun (hamf/pgroups
                  n-rows
                  (fn [^long sidx ^long eidx]
                    (if idx-filter
                      (loop [sidx sidx]
                        (when (< sidx eidx)
                          (when (.test idx-filter sidx)
                            (.compute ^Map agg-map (.readObject group-col sidx)
                                      (hamf-fn/bi-function
                                       k v
                                       (agg-rfn (or v (agg-init)) sidx))))
                          (recur (unchecked-inc sidx))))
                      (loop [sidx sidx]
                        (when (< sidx eidx)
                          (.compute ^Map agg-map (.readObject group-col sidx)
                                    (hamf-fn/bi-function
                                     k v
                                     (agg-rfn (or v (agg-init)) sidx)))
                          (recur (unchecked-inc sidx))))))))
          agg-map
          #_(->> (if idx-filter
                 (lznc/filter idx-filter (hamf/range n-rows))
                 (hamf/range n-rows))
               (hamf-rf/preduce (constantly nil)
                                (fn [acc ^long idx]

                                  )
                                )
               (hamf/group-by-reducer
                (hamf-fn/long->obj idx (.readObject group-col idx))
                agg-reducer
                {:map-fn (constantly agg-map)
                 :ordered? false
                 :skip-finalize? true
                 :min-n 1000}))))
       ([agg-map]
        (if (get options :skip-finalize?)
          agg-map
          (let [c ((hamf-proto/->init-val-fn (io-mapseq/mapseq-reducer nil)))]
            ;;Also possible to parse N datasets in parallel and do a concat-copying
            ;;operation but in my experience this steps takes up nearly no time.
            (.forEach ^ConcurrentHashMap agg-map 32
                      (hamf-fn/bi-consumer
                       k v
                       (do
                         (let [vv (finalize-fn v)]
                           (locking c (.accept ^Consumer c vv))))))
            @c)))))))


(defn group-by-column-agg
  "Group a sequence of datasets by a column and aggregate down into a new dataset.

  * colname - Either a single scalar column name or a vector of column names to group by.

  * agg-map - map of result column name to reducer.  All values in the agg map must be
    functions from dataset to hamf (non-parallel) reducers.  Note that transducer-compatible
    rf's - such as kixi.mean, are valid hamf reducers.
  * ds-seq - Either a single dataset or sequence of datasets.


  See also [[group-by-column-agg-rf]].

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
user> (def ds (ds/->dataset \"https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv\"
                            {:key-fn keyword}))

#'user/ds
user> (ds-reduce/group-by-column-agg
       :symbol
       {:price-avg (ds-reduce/mean :price)
        :price-sum (ds-reduce/sum :price)}
       ds)
_unnamed [5 3]:

| :symbol |   :price-avg | :price-sum |
|---------|-------------:|-----------:|
|    MSFT |  24.73674797 |    3042.62 |
|    AAPL |  64.73048780 |    7961.85 |
|     IBM |  91.26121951 |   11225.13 |
|    AMZN |  47.98707317 |    5902.41 |
|    GOOG | 415.87044118 |   28279.19 |

user> (def testds (ds/->dataset {:a [\"a\" \"a\" \"a\" \"b\" \"b\" \"b\" \"c\" \"d\" \"e\"]
                                 :b [22   21  22 44  42  44   77 88 99]}))
#'user/testds
user> (ds-reduce/group-by-column-agg
       [:a :b] {:c (ds-reduce/row-count)}
       testds)
_unnamed [7 3]:

| :a | :b | :c |
|----|---:|---:|
|  e | 99 |  1 |
|  a | 21 |  1 |
|  c | 77 |  1 |
|  d | 88 |  1 |
|  b | 44 |  2 |
|  b | 42 |  1 |
|  a | 22 |  2 |
```"
  ([colname agg-map options ds-seq]
   (hamf-rf/reduce-reducer (group-by-column-agg-rf colname agg-map options)
                        (if (ds-impl/dataset? ds-seq)
                          [ds-seq]
                          ds-seq)))
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
   [ds-seq])
```"
  ([agg-map options ds-seq]
   (let [cnames (vec (keys agg-map))
         reducers (vals agg-map)
         [combined-reducers rev-indexes] (combine-reducers reducers)
         context-map (ConcurrentHashMap.)
         _ (reduce (fn [acc ds]
                     (let [rs (->> (map #(ds-proto/ds->reducer % ds) combined-reducers)
                                   (hamf-rf/compose-reducers {:rfn-datatype :int64}))
                           rs-rfn (hamf-proto/->rfn rs)
                           rs-init (hamf-proto/->init-val-fn rs)
                           init-fn (hamf-fn/function _k (rs-init))]
                       (doall (hamf/pgroups
                               (ds-base/row-count ds)
                               (fn [^long sidx ^long eidx]
                                 (let [tid (.getId (Thread/currentThread))]
                                   (.put context-map tid
                                         (reduce rs-rfn
                                                 (.computeIfAbsent context-map tid init-fn)
                                                 (hamf/range sidx eidx)))))
                               {:min-n 100}))))
                   nil
                   (if (ds-impl/dataset? ds-seq) [ds-seq] ds-seq))
         ^objects final-ctx
         (reduce (fn [^objects lhs ^objects rhs]
                   (dotimes [idx (count combined-reducers)]
                     (aset lhs idx
                           (ds-proto/merge (combined-reducers idx)
                                           (aget lhs idx)
                                           (aget rhs idx))))
                   lhs)
                 (.values context-map))]
     (ds-io/->dataset [(zipmap cnames
                               (->> reducers
                                    (map-indexed
                                     (fn [ridx reducer]
                                       (hamf-proto/finalize
                                        reducer
                                        (aget final-ctx (long (rev-indexes ridx))))))))])))
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
