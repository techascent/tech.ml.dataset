(ns tech.v3.dataset.reductions.apache-data-sketch
  "Reduction reducers based on the apache data sketch family of algorithms.

  * [apache data sketches](https://datasketches.apache.org/)

  Algorithms included here are:

### Set Cardinality

   * [hyper-log-log](https://datasketches.apache.org/docs/HLL/HLL.html)
   * [theta](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
   * [cpc](https://datasketches.apache.org/docs/CPC/CPC.html)

### Quantiles
   * [doubles](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html)


  Example:
```clojure
user> (require '[tech.v3.dataset :as ds])
11:04:44.508 [nREPL-session-e40a19c2-8d41-40a8-8853-abe1293abe20] DEBUG tech.v3.tensor.dimensions.global-to-local - insn custom indexing enabled!
nil
user> (require '[tech.v3.dataset.reductions :as ds-reduce])
nil
user> (require '[tech.v3.dataset.reductions.apache-data-sketch :as ds-sketch])
#'user/stocks
user> (def stocks (ds/->dataset \"test/data/stocks.csv\" {:key-fn keyword}))
  #'user/stocks
user> (ds-reduce/group-by-column-agg
       :symbol
       {:symbol (ds-reduce/first-value :symbol)
        :price-quantiles (ds-sketch/prob-quantiles :price [0.25 0.5 0.75])
        :price-cdfs (ds-sketch/prob-cdfs :price [25 50 75])}
       [stocks stocks stocks])
:symbol-aggregation [5 3]:

| :symbol |      :price-quantiles |              :price-cdfs |
|---------|-----------------------|--------------------------|
|    AAPL | [11.03, 36.81, 105.1] | [0.4065, 0.5528, 0.6423] |
|     IBM | [77.26, 88.70, 102.4] |   [0.000, 0.000, 0.1382] |
|    AMZN | [30.12, 41.50, 67.00] | [0.2249, 0.6396, 0.8103] |
|    MSFT | [21.75, 24.11, 27.34] |   [0.5772, 1.000, 1.000] |
|    GOOG | [338.5, 421.6, 510.0] |    [0.000, 0.000, 0.000] |
```"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.reductions.impl :as impl]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.api :as hamf])
  (:import [org.apache.datasketches.hll HllSketch TgtHllType]
           [org.apache.datasketches.quantiles DoublesSketch UpdateDoublesSketch
            DoublesUnion]
           [java.util.function DoubleConsumer]
           [ham_fisted Reducible]
           [clojure.lang IDeref]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol ^:private PSketchResult
  (^{:private true
     :tag 'double} sketch-estimate [cls]))

(extend-protocol PSketchResult
  HllSketch
  (sketch-estimate [r] (.getEstimate r))
  org.apache.datasketches.hll.Union
  (sketch-estimate [r] (.getEstimate r)))


(defn- hll-union?
  [val]
  (instance? org.apache.datasketches.hll.Union val))


(defn- ->hll-union
  ^org.apache.datasketches.hll.Union
  [item]
  (if (instance? org.apache.datasketches.hll.Union item)
    item
    (org.apache.datasketches.hll.Union. ^HllSketch item)))


(defn hll-reducer
  "Return a hamf parallel reducer that produces a hyper-log-log-based set cardinality.

  At any point you can get an estimate from the reduced value - call sketch-estimate.

  Options:

  * `:hll-lgk` - defaults to 12, this is log-base2 of k, so k = 4096. lgK can be
           from 4 to 21.
  * `:hll-type` - One of #{4,6,8}, defaults to 8.  The HLL_4, HLL_6 and HLL_8
           represent different levels of compression of the final HLL array where the
           4, 6 and 8 refer to the number of bits each bucket of the HLL array is
           compressed down to. The HLL_4 is the most compressed but generally slightly
           slower than the other two, especially during union operations.
  * `:datatype` - One of :float64, :int64, :string"
  [{:keys [hll-lgk hll-type datatype]
    :or {hll-lgk 12 hll-type 8 datatype :float64}}]
  (let [cons-fn #(HllSketch. (int hll-lgk)
                             ^TgtHllType (case (long hll-type)
                                           4 TgtHllType/HLL_4
                                           6 TgtHllType/HLL_6
                                           8 TgtHllType/HLL_8))
        rfn (case datatype
              :float64 (hamf/double-accumulator
                        acc val (.update ^HllSketch acc val) acc)
              :int64 (hamf/long-accumulator
                      acc val (.update ^HllSketch acc val) acc)
              :string (fn [acc val] (.update ^HllSketch acc (str val)) acc))
        merge-fn (fn [lhs rhs]
                   (let [lhs (->hll-union lhs)]
                     (if (hll-union? rhs)
                       (.update lhs (.getResult (->hll-union rhs)))
                       (.update lhs ^HllSketch rhs))
                     lhs))]
    (reify
      hamf-proto/Reducer
      (->init-val-fn [r] cons-fn)
      (->rfn [r] rfn)
      hamf-proto/Finalize
      (finalize [r v] (sketch-estimate val))
      hamf-proto/ParallelReducer
      (->merge-fn [r] merge-fn))))


(defn prob-set-cardinality
  "Get the probabilistic set cardinality using hyper-log-log.  See [[hll-reducer]]."
  ([colname options]
   (impl/reducer->column-reducer (hll-reducer options) colname))
  ([colname]
   (prob-set-cardinality colname nil)))


(defn- ->doubles-union
  ^DoublesUnion [v]
  (if (instance? DoublesUnion v)
    v
    (doto (.. (DoublesUnion/builder) build)
      (.update ^UpdateDoublesSketch v))))


(defn- ->doubles-sketch
  ^DoublesSketch [item]
  (if (instance? DoublesSketch item)
    item
    (.getResult ^DoublesUnion item)))


(defn doubles-sketch-reducer
  "Return a doubles updater.  This is the reservoir and k is the reservoir size.  From
  a reservoir we can then get various different statistical quantities.

  A k of 128 results in about 1.7% error in returned quantities."
  [k finalize-fn]
  (let [cons-fn #(-> (DoublesSketch/builder)
                     (.setK (long k))
                     (.build))
        rfn (hamf/double-accumulator
             acc v (.update ^UpdateDoublesSketch acc v) acc)
        merge-fn (fn [lhs rhs]
                   (let [lhs (->doubles-union lhs)]
                     (.update lhs (->doubles-sketch rhs))
                     lhs))]
    (reify
      hamf-proto/Reducer
      (->init-val-fn [r] cons-fn)
      (->rfn [r] rfn)
      hamf-proto/ParallelReducer
      (->merge-fn [r] merge-fn)
      hamf-proto/Finalize
      (finalize [r v] (finalize-fn v)))))


(defn ^:private doubles-ds-reducer
  [colname k finalize-fn]
  (let [r (doubles-sketch-reducer k finalize-fn)
        col-reducer (impl/reducer->column-reducer r colname)
        merge-fn (hamf-proto/->merge-fn r)]
    (reify
      ds-proto/PDatasetReducer
      (ds->reducer [this ds] (ds-proto/ds->reducer col-reducer ds))
      (merge [this lhs rhs] (merge-fn lhs rhs))
      hamf-proto/Finalize
      (finalize [this v] (finalize-fn v))
      ds-proto/PReducerCombiner
      (reducer-combiner-key [this] [colname :doubles-sketch k]))))


(def ^:private default-doubles-k 128)


(defn prob-quantile
  "Probabilistic quantile estimation - see [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).

  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname quantile k]
   (doubles-ds-reducer colname k #(.getQuantile (->doubles-sketch %) (double quantile))))
  ([colname quantile]
   (prob-quantile colname quantile default-doubles-k)))


(defn prob-quantiles
  "Probabilistic quantile estimation - see [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).

  * quantiles - sequence of quantiles.
  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname quantiles k]
   (let [src-data (double-array quantiles)]
     (doubles-ds-reducer colname k
                         #(-> (.getQuantiles (->doubles-sketch %) src-data)
                              ;;fix printing
                              (dtype/as-array-buffer)
                              (vary-meta assoc :simple-print? true)))))
  ([colname quantiles]
   (prob-quantiles colname quantiles default-doubles-k)))


(defn prob-median
  "Probabilistic median -  [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html)."
  ([colname k]
   (prob-quantile colname 0.5 k))
  ([colname]
   (prob-quantile colname 0.5)))


(defn prob-interquartile-range
    "Probabilistic interquartile range -  [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html)."
  ([colname k]
   (doubles-ds-reducer colname k
                       #(let [quantiles (.getQuantiles (->doubles-sketch %)
                                                       (double-array [0.25 0.75]))]
                          (- (aget quantiles 1) (aget quantiles 0)))))
  ([colname]
   (prob-interquartile-range colname default-doubles-k)))


(defn prob-cdfs
  "Probabilistic cdfs, one for each value passed in.  See  [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
  See prob-quantiles for k."
  ([colname cdfs k]
   (doubles-ds-reducer
    colname k
    #(-> (.getCDF (->doubles-sketch %) (double-array cdfs))
         (dtype/as-array-buffer)
         (dtype/sub-buffer 0 (count cdfs))
         (vary-meta assoc :simple-print? true))))
  ([colname cdfs]
   (prob-cdfs colname cdfs default-doubles-k)))


(defn prob-cdf
  "Probabilistic cdfs, one for each value passed in.  See  [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
  See prob-quantiles for k."
  ([colname cdf k]
   (doubles-ds-reducer
    colname k
    #(-> (.getCDF (->doubles-sketch %) (double-array [cdf]))
         (first))))
  ([colname cdf]
   (prob-cdfs colname cdf default-doubles-k)))


(defn prob-pmfs
  "Returns an approximation to the Probability Mass Function (PMF) of the input stream
  given a set of splitPoints (values). See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).

  See prog-quantiles for k"
  ([colname pmfs k]
   (doubles-ds-reducer
    colname k
    #(-> (.getPMF (->doubles-sketch  %) (double-array pmfs))
         (dtype/as-array-buffer)
         (dtype/sub-buffer 0 (count pmfs))
         (vary-meta assoc :simple-print? true))))
  ([colname pmfs]
   (prob-pmfs colname pmfs default-doubles-k)))


(comment

  (extend-protocol PSketchResult
  org.apache.datasketches.theta.Sketch
  (sketch-estimate [sketch] (.getEstimate sketch)))

  (let [union (.. (org.apache.datasketches.theta.SetOperation/builder)
                  (buildUnion))]
    (.update union ^org.apache.datasketches.theta.Sketch
             (.value ^Consumers$StagedConsumer this))
    (dotimes [idx (.size other-sketches)]
      (.update union ^org.apache.datasketches.theta.Sketch
               (.value ^Consumers$StagedConsumer
                       (.get other-sketches idx))))
    (ThetaConsumer. (.getResult union)))

  (-> (org.apache.datasketches.theta.UpdateSketchBuilder.)
      (.build)
      (ThetaConsumer.))


  (extend-protocol PSketchResult
    org.apache.datasketches.cpc.CpcSketch
    (sketch-estimate [sketch] (.getEstimate sketch)))

  (combineList [this other-sketches]
               (let [union (org.apache.datasketches.cpc.CpcUnion.)]
                 (.update union ^org.apache.datasketches.cpc.CpcSketch
                          (.value ^Consumers$StagedConsumer this))
                 (dotimes [idx (.size other-sketches)]
                   (.update union ^org.apache.datasketches.cpc.CpcSketch
                            (.value ^Consumers$StagedConsumer
                                    (.get other-sketches idx))))
                 (CpcConsumer. (.getResult union))))

  (defmethod sketch-build-fn :cpc
    [{:keys [cpc-lgk]
      :or {cpc-lgk 10}}]
    #(-> (org.apache.datasketches.cpc.CpcSketch. cpc-lgk)
         (CpcConsumer.))))
