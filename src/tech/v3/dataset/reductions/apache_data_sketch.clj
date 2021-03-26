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
  "
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.reductions.impl :as ds-reduce-impl])
  (:import [org.apache.datasketches.hll HllSketch TgtHllType]
           [org.apache.datasketches.quantiles DoublesSketch UpdateDoublesSketch
            DoublesUnion]
           [java.util.function DoubleConsumer Consumer LongConsumer]
           [java.util List]
           [tech.v3.datatype Consumers$StagedConsumer Consumers$CombinedConsumer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol ^:private PSketchResult
  (^{:private true
     :tag 'double} sketch-estimate [cls]))



(defmulti ^:private sketch-build-fn
  "Given a map of options, make a new sketch algorithm build-fn parameterized by the
  rest of the arguments in the option map. Algorithms are denoted by the :algorithm
  member. Returns function that takes no arguments and constructs an object that
  implements Consumers$CombinedConsumer and Consumers$StagedConsumer.  Derefing the
  consumer returns sketch object itself.  The CombinedConsumer only implements
  combineList; single element combine is not supported."
  :algorithm)


(deftype ^:private SketchCombiner [combiner-options
                                   colname
                                   finalize-fn]
  ds-reduce-impl/PReducerCombiner
  (reducer-combiner-key [this]
    [colname :data-sketch (dissoc combiner-options :datatype)])
  (combine-reducers [this combiner-key]
    (let [build-fn (sketch-build-fn (last combiner-key))]
      (case (:datatype combiner-options)
        :float64 (ds-reduce-impl/staged-consumer-reducer
                  :float64 colname build-fn
                  identity)
        :int64 (ds-reduce-impl/staged-consumer-reducer
                :int64 colname build-fn
                identity)
        :string (ds-reduce-impl/staged-consumer-reducer
                 :string colname build-fn
                 identity))))
  (finalize-combined-reducer [this ctx]
    (finalize-fn (.value ^Consumers$StagedConsumer ctx))))


(extend-protocol PSketchResult
  HllSketch
  (sketch-estimate [sketch] (.getEstimate sketch)))


(deftype ^:private HllConsumer [^HllSketch sketch]
  Consumers$CombinedConsumer
  (acceptObject [this data]
    (.update sketch (str data)))
  (acceptLong [this data]
    (.update sketch data))
  (acceptDouble [this data]
    (.update sketch data))
  Consumers$StagedConsumer
  (combine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-list]
    (let [union (org.apache.datasketches.hll.Union.)]
      (.update union ^HllSketch (.value ^Consumers$StagedConsumer this))
      (dotimes [idx (.size other-list)]
        (.update union ^HllSketch (.value ^Consumers$StagedConsumer
                                          (.get other-list idx))))
      (HllConsumer. (.getResult union))))
  (value [this] sketch))


(defmethod sketch-build-fn :hyper-log-log
  [{:keys [hll-lgk hll-type]
    :or {hll-lgk 14 hll-type 8}}]
  #(-> (HllSketch. (int hll-lgk)
                   ^TgtHllType (case (long hll-type)
                                 4 TgtHllType/HLL_4
                                 6 TgtHllType/HLL_6
                                 8 TgtHllType/HLL_8))
       (HllConsumer.)))


(extend-protocol PSketchResult
  org.apache.datasketches.theta.Sketch
  (sketch-estimate [sketch] (.getEstimate sketch)))


(deftype ^:private ThetaConsumer [^org.apache.datasketches.theta.Sketch sketch]
  Consumers$CombinedConsumer
  (acceptDouble [this data]
    (.update ^org.apache.datasketches.theta.UpdateSketch sketch data))
  (acceptLong [this data]
    (.update ^org.apache.datasketches.theta.UpdateSketch sketch data))
  (acceptObject [this data]
    (.update ^org.apache.datasketches.theta.UpdateSketch sketch (str data)))
  Consumers$StagedConsumer
  (combine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-sketches]
    (let [union (.. (org.apache.datasketches.theta.SetOperation/builder)
                    (buildUnion))]
      (.update union ^org.apache.datasketches.theta.Sketch
               (.value ^Consumers$StagedConsumer this))
      (dotimes [idx (.size other-sketches)]
        (.update union ^org.apache.datasketches.theta.Sketch
                 (.value ^Consumers$StagedConsumer
                         (.get other-sketches idx))))
      (ThetaConsumer. (.getResult union))))
  (value [this] sketch))


(defmethod sketch-build-fn :theta
  [_options]
  #(-> (org.apache.datasketches.theta.UpdateSketchBuilder.)
       (.build)
       (ThetaConsumer.)))


(extend-protocol PSketchResult
  org.apache.datasketches.cpc.CpcSketch
  (sketch-estimate [sketch] (.getEstimate sketch)))


(deftype ^:private CpcConsumer [^org.apache.datasketches.cpc.CpcSketch sketch]
  Consumers$CombinedConsumer
  (acceptDouble [this data]
    (.update sketch data))
  (acceptLong [this data]
    (.update sketch data))
  (acceptObject [this data]
    (.update sketch (str data)))
  Consumers$StagedConsumer
  (combine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-sketches]
    (let [union (org.apache.datasketches.cpc.CpcUnion.)]
      (.update union ^org.apache.datasketches.cpc.CpcSketch
               (.value ^Consumers$StagedConsumer this))
      (dotimes [idx (.size other-sketches)]
        (.update union ^org.apache.datasketches.cpc.CpcSketch
                 (.value ^Consumers$StagedConsumer
                         (.get other-sketches idx))))
      (CpcConsumer. (.getResult union))))
  (value [this] sketch))


(defmethod sketch-build-fn :cpc
  [{:keys [cpc-lgk]
    :or {cpc-lgk 10}}]
  #(-> (org.apache.datasketches.cpc.CpcSketch. cpc-lgk)
       (CpcConsumer.)))


(defn prob-set-cardinality
  "Get the probabilistic set cardinality.

  Options:

  * `:datatype` - One of `#{:float64 :string}`.  Unspecified defaults to `:float64`.
  * `:algorithm` - defaults to :hyper-log-log.  Further algorithm-specific options
    may be included in the options map.

  Algorithm specific options:

  * [:hyper-log-log](https://datasketches.apache.org/docs/HLL/HLL.html)
        * `:hll-lgk` - defaults to 12, this is log-base2 of k, so k = 4096. lgK can be
           from 4 to 21.
        * `:hll-type` - One of #{4,6,8}, defaults to 8.  The HLL_4, HLL_6 and HLL_8
           represent different levels of compression of the final HLL array where the
           4, 6 and 8 refer to the number of bits each bucket of the HLL array is
           compressed down to. The HLL_4 is the most compressed but generally slightly
           slower than the other two, especially during union operations.
  * [:theta](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
  * [:cpc](https://datasketches.apache.org/docs/CPC/CPC.html)
        * `:cpc-lgk` - Defaults to 10. "
  ([colname {:keys [algorithm datatype]
             :or {algorithm :hyper-log-log
                  datatype :float64} :as
             options}]
   (SketchCombiner. (assoc options
                           :algorithm algorithm
                           :datatype datatype)
                    colname sketch-estimate))
  ([colname]
   (prob-set-cardinality colname nil)))


(deftype ^:private DoublesUpdateConsumer [^DoublesSketch sketch]
  DoubleConsumer
  (accept [this data]
    (.update ^UpdateDoublesSketch sketch data))
  Consumers$StagedConsumer
  (combine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-sketches]
    (let [union (.. (DoublesUnion/builder) build)]
      (.update union sketch)
      (dotimes [idx (.size other-sketches)]
        (.update union ^DoublesSketch
                 (.value ^Consumers$StagedConsumer
                         (.get other-sketches idx))))
      (DoublesUpdateConsumer. (.getResult union))))
  (value [this] sketch))


(defn doubles-updater-fn
  [k]
  #(-> (DoublesSketch/builder)
       (.setK (long k))
       (.build)
       (DoublesUpdateConsumer.)))


(deftype DoublesSketchCombiner [colname k finalize-fn]
  ds-reduce-impl/PReducerCombiner
  (reducer-combiner-key [this]
    [colname :doubles-sketch k])
  (combine-reducers [this combiner-key]
    (let [build-fn (doubles-updater-fn (last combiner-key))]
      (ds-reduce-impl/staged-consumer-reducer
       :float64 colname build-fn
       identity)))
  (finalize-combined-reducer [this ctx]
    (finalize-fn (.value ^Consumers$StagedConsumer ctx))))

(def ^:private default-doubles-k 128)


(defn prob-quantile
  "Probabilistic quantile estimation - see [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).

  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname quantile k]
   (DoublesSketchCombiner. colname k
                           #(.getQuantile ^DoublesSketch % (double quantile))))
  ([colname quantile]
   (prob-quantile colname quantile default-doubles-k)))


(defn prob-quantiles
  "Probabilistic quantile estimation - see [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).

  * quantiles - sequence of quantiles.
  * k - defaults to 128. This produces a normalized rank error of about 1.7%"
  ([colname quantiles k]
   (let [src-data (double-array quantiles)]
     (DoublesSketchCombiner. colname k
                             #(-> (.getQuantiles ^DoublesSketch % src-data)
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
   (DoublesSketchCombiner. colname k
                           #(let [quantiles (.getQuantiles ^DoublesSketch %
                                                           (double-array [0.25 0.75]))]
                              (- (aget quantiles 1) (aget quantiles 0)))))
  ([colname]
   (prob-interquartile-range colname default-doubles-k)))


(defn prob-cdfs
  "Probabilistic cdfs, one for each value passed in.  See  [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
  See prob-quantiles for k."
  ([colname cdfs k]
   (DoublesSketchCombiner.
    colname k
    #(-> (.getCDF ^DoublesSketch % (double-array cdfs))
         (dtype/as-array-buffer)
         (dtype/sub-buffer 0 (count cdfs))
         (vary-meta assoc :simple-print? true))))
  ([colname cdfs]
   (prob-cdfs colname cdfs default-doubles-k)))


(defn prob-pmfs
  "Returns an approximation to the Probability Mass Function (PMF) of the input stream
  given a set of splitPoints (values). See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).

  See prog-quantiles for k"
  ([colname pmfs k]
   (DoublesSketchCombiner.
    colname k
    #(-> (.getPMF ^DoublesSketch % (double-array pmfs))
         (dtype/as-array-buffer)
         (dtype/sub-buffer 0 (count pmfs))
         (vary-meta assoc :simple-print? true))))
  ([colname pmfs]
   (prob-pmfs colname pmfs default-doubles-k)))
