(ns tech.v3.dataset.reductions.apache-data-sketch
  "Reduction reducers based on the apache data sketch family of algorithms.

  * [apache data sketches](https://datasketches.apache.org/)

  Algorithms included here are:

### Set Cardinality

   * [hyper-log-log](https://datasketches.apache.org/docs/HLL/HLL.html)
   * [theta](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)

  "
  (:require [tech.v3.dataset.reductions.impl :as ds-reduce-impl])
  (:import [org.apache.datasketches.hll HllSketch TgtHllType]
           [java.util.function DoubleConsumer Consumer]
           [java.util List]
           [tech.v3.datatype Consumers$StagedConsumer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol ^:private PSketchResult
  (^{:private true
     :tag 'double} sketch-estimate [cls]))



(defmulti ^:private make-sketch
  "Make a new sketch algorithm  Algorithms are denoted by the :algorithm member."
  (fn [colname options]
    (:algorithm options)))


(deftype ^:private SketchCombiner [combiner-options
                                   colname
                                   finalize-fn]
  ds-reduce-impl/PReducerCombiner
  (reducer-combiner-key [this]
    [:data-sketch combiner-options])
  (combine-reducers [this _combiner-key]
    (make-sketch colname combiner-options))
  (finalize-combined-reducer [this ctx]
    (finalize-fn (.value ^Consumers$StagedConsumer ctx))))


(extend-protocol PSketchResult
  HllSketch
  (sketch-estimate [sketch] (.getEstimate sketch)))


(defn- union-hll-sketches
  [this ^List other-list]
  (let [union (org.apache.datasketches.hll.Union.)]
    (.update union ^HllSketch (.value ^Consumers$StagedConsumer this))
    (dotimes [idx (.size other-list)]
      (.update union ^HllSketch (.value ^Consumers$StagedConsumer
                                        (.get other-list idx))))
    (reify Consumers$StagedConsumer
      (value [this] (.getResult union)))))


(deftype ^:private HllConsumer [^HllSketch sketch]
  DoubleConsumer
  (accept [this data]
    (.update sketch data))
  Consumers$StagedConsumer
  (inplaceCombine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-list]
    (union-hll-sketches this other-list))
  (value [this] sketch))


(deftype ^:private HllStrConsumer [^HllSketch sketch]
  Consumer
  (accept [this data]
    (.update sketch (str data)))
  Consumers$StagedConsumer
  (inplaceCombine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-list]
    (union-hll-sketches this other-list))
  (value [this] sketch))


(defmethod make-sketch :hyper-log-log
  [colname {:keys [hll-k hll-type datatype]
            :or {hll-k 14 hll-type 8}}]
  (let [build-fn #(HllSketch. hll-k
                              ^TgtHllType (case (long hll-type)
                                            4 TgtHllType/HLL_4
                                            6 TgtHllType/HLL_6
                                            8 TgtHllType/HLL_8))]
    (case datatype
      :float64 (ds-reduce-impl/staged-consumer-reducer
                :float64 colname #(-> (build-fn)
                                      (HllConsumer.))
                identity)
      :string (ds-reduce-impl/staged-consumer-reducer
               :string colname #(-> (build-fn)
                                    (HllStrConsumer.))
               identity))))


(extend-protocol PSketchResult
  org.apache.datasketches.theta.Sketch
  (sketch-estimate [sketch] (.getEstimate sketch)))


(defn- union-theta-sketches
  [this ^List other-sketches]
  (let [union (.. (org.apache.datasketches.theta.SetOperation/builder)
                  (buildUnion))]
    (.update union ^org.apache.datasketches.theta.Sketch
             (.value ^Consumers$StagedConsumer this))
    (dotimes [idx (.size other-sketches)]
      (.update union ^org.apache.datasketches.theta.Sketch
               (.value ^Consumers$StagedConsumer
                       (.get other-sketches idx))))
    (reify Consumers$StagedConsumer
      (value [this] (.getResult union)))))


(deftype ^:private ThetaConsumer [^org.apache.datasketches.theta.Sketch sketch]
  DoubleConsumer
  (accept [this data]
    (.update ^org.apache.datasketches.theta.UpdateSketch sketch data))
  Consumers$StagedConsumer
  (inplaceCombine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-list]
    (union-theta-sketches this other-list))
  (value [this] sketch))


(deftype ^:private ThetaStrConsumer [^org.apache.datasketches.theta.Sketch sketch]
  Consumer
  (accept [this data]
    (.update ^org.apache.datasketches.theta.UpdateSketch sketch (str data)))
  Consumers$StagedConsumer
  (inplaceCombine [this other]
    (throw (Exception. "Unimplemented - use combineList")))
  (combineList [this other-list]
    (union-theta-sketches this other-list))
  (value [this] sketch))


(defmethod make-sketch :theta
  [colname {:keys [datatype]}]
  (let [build-fn (fn []
                   (-> (org.apache.datasketches.theta.UpdateSketchBuilder.)
                       (.build)))]
    (case datatype
      :float64 (ds-reduce-impl/staged-consumer-reducer
                :float64 colname #(-> (build-fn)
                                      (ThetaConsumer.))
                identity)
      :string (ds-reduce-impl/staged-consumer-reducer
               :string colname #(-> (build-fn)
                                    (ThetaStrConsumer.))
               identity))))


(defn set-cardinality
  "Get the probabilistic set cardinality.

  Options:

  * `:datatype` - One of `#{:float64 :string}`.  Unspecified defaults to `:float64`.
  * `:algorithm` - defaults to :hyper-log-log.  Further algorithm-specific options
    may be included in the options map.

  Algorithm options:
  * [:hyper-log-log](https://datasketches.apache.org/docs/HLL/HLL.html)
        * `:hll-k` - defaults to 14.
        * `:hll-type` - One of #{4,6,8}; defaults to 8.
  * [:theta](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)"
  ([colname {:keys [algorithm datatype]
             :or {algorithm :hyper-log-log
                  datatype :float64} :as
             options}]
   (SketchCombiner. (assoc options
                           :algorithm algorithm
                           :datatype datatype)
                    colname sketch-estimate))
  ([colname]
   (set-cardinality colname nil)))
