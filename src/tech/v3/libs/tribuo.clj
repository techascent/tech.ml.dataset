(ns tech.v3.libs.tribuo
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling :as modelling]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.jvm-map :as jvm-map]
            [clojure.set :as set])
  (:import [tech.v3.datatype Buffer ArrayHelpers]
           [java.util HashMap Map List]
           [org.tribuo.classification Label LabelFactory]
           [org.tribuo DataSource Output OutputFactory Trainer Model MutableDataset
            Prediction]
           [org.tribuo.impl ArrayExample]
           [org.tribuo.provenance SimpleDataSourceProvenance]
           [org.tribuo.regression RegressionFactory Regressor]))

(set! *warn-on-reflection* true)

(def ^:private str-ary-cls (Class/forName "[Ljava.lang.String;"))
(def ^:private dbl-ary-cls (Class/forName "[D"))

(defn- ->string
  ^String [obj]
  (if (nil? obj)
    ""
    (if (or (keyword? obj)
            (symbol? obj))
      (name obj)
      (str obj))))

(defn- ->string-array
  ^"[Ljava.lang.String;" [data]
  (if (instance? str-ary-cls data)
    data
    (into-array String (map ->string data))))

(defn- ->double-array
  "Slightly faster than (double-array) when the source data implements tech.v3.datatype.Buffer."
  ^doubles [data]
  (if (instance? dbl-ary-cls data)
    data
    (let [data (dtype/->buffer data)
          ndata (.size data)
          ddata (double-array ndata)]
      ;;Clojure's aset function returns a value and thus boxes every item in the array.
      (dotimes [idx ndata] (ArrayHelpers/aset ddata idx (.readDouble data idx)))
      ddata)))

(defn- ds->regression-outputs
  [ds]
  (let [fact (RegressionFactory.)]
    (with-meta
      (if (integer? ds)
        (dtype/make-reader :object ds (.getUnknownOutput fact))
        (let [rvecs (ds/rowvecs ds)
              names (->string-array (ds/column-names ds))]
          (dtype/make-reader :object (dtype/ecount rvecs)
                             (Regressor. names (->double-array (rvecs idx))))))
      {:output-factory fact})))


(defn- ds->single-label-outputs
  [ds]
  (let [label-fact (LabelFactory.)]
    (if (integer? ds)
      (with-meta
        (dtype/make-reader :object ds (.getUnknownOutput label-fact))
        {:output-factory label-fact})
      (do
        (when-not (== 1 (ds/column-count ds))
          (throw (Exception. "Single label output expected, got multi-label dataset")))
        (let [col (first (vals ds))
              colmeta (meta col)
              data (dtype/->buffer col)
              n-rows (.lsize data)
              data (if-let [cat-map (get-in colmeta [:categorical-map :lookup-table])]
                     (let [rev-map (set/map-invert cat-map)]
                       (dtype/make-reader :object n-rows (get rev-map (long (data idx)))))
                     data)]
          (with-meta
            (dtype/make-reader :object n-rows (Label. (->string (data idx))))
            {:output-factory label-fact}))))))


(defn- ds->examples
  ^Buffer [ds ds->outputs]
  (let [inf-columns (modelling/inference-target-column-names ds)
        inf-ds (when inf-columns (ds/select-columns ds inf-columns))
        feat-ds (ds/remove-columns ds inf-columns)
        feat-data (ds/rowvecs feat-ds)
        n-rows (ds/row-count ds)
        ;;column names have to be strings.
        cnames (->string-array (ds/column-names feat-ds))
        n-rows (ds/row-count ds)
        outputs (if inf-ds
                  (ds->outputs inf-ds)
                  (ds->outputs n-rows))]
    (with-meta
      (dtype/make-reader :object n-rows
                         ;;idx is a compile-time variable below
                         (ArrayExample. ^Output (outputs idx)
                                        cnames (->double-array (feat-data idx))))
      (meta outputs))))


(defn- ds->datasource
  ^DataSource [ds ds->outputs]
  (let [examples (ds->examples ds ds->outputs)
        {:keys [output-factory provenance]} (meta examples)
        provenance (or provenance
                       (SimpleDataSourceProvenance. (:name (meta ds)) output-factory))]
    (when-not output-factory
      (throw (RuntimeException. "Output factory not present in example metadata")))
    (reify DataSource
      (getProvenance [this] provenance)
      (iterator [this] (.iterator examples))
      (getOutputFactory [this] output-factory))))


(defn- label-score
  ^double [^Label label]
  (.getScore label))

(defn- label-label
  ^String [^Label label]
  (.getLabel label))

(defn- prediction-label
  ^String [^Prediction pred]
  (-> (.getOutput pred)
      (label-label)))

(defn- has-probabilities?
  [^Prediction pred]
  (.hasProbabilities pred))

(defn- prediction-scores
  ^Map [^Prediction pred]
  (.getOutputScores pred))


(defn- regressor-value
  ^double [^Regressor reg]
  (aget (.getValues reg) 0))


(defn- prediction-regval
  [^Prediction pred]
  (regressor-value (.getOutput pred)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Begin public API


;; Classification

(defn make-classification-datasource
  (^DataSource [ds]
   (make-classification-datasource ds nil))
  (^DataSource [ds inf-col-name]
   (if inf-col-name
     (-> (modelling/set-inference-target ds inf-col-name)
         (ds->datasource ds->single-label-outputs))
     (ds->datasource ds ds->single-label-outputs))))


(defn train-classification
  ^Model [^Trainer trainer ds & [inf-col-name]]
  (.train trainer (-> (make-classification-datasource ds inf-col-name)
                      (MutableDataset.))))


(defn classification-predictions->dataset
  [^List predictions]
  (let [labels (dtype/emap prediction-label :string predictions)
        pred (.get predictions 0)]
    (-> (ds/->dataset
         (merge {:prediction labels}
                (when (has-probabilities? pred)
                  (let [scores (prediction-scores pred)]
                    (->> (.entrySet scores)
                         (map (fn [entry]
                                (let [ekey (jvm-map/entry-key entry)]
                                  [ekey (-> (dtype/emap #(-> (prediction-scores %)
                                                             (.get ekey)
                                                             (label-score))
                                                        :float64
                                                        predictions)
                                            (dtype/clone))])))
                         (into {}))))))
        (modelling/set-inference-target :prediction))))


(defn predict-classification
  [^Model model ds]
  (when-not (== 0 (ds/row-count ds))
    (->> (.predict model (make-classification-datasource ds))
         (classification-predictions->dataset))))


;; Regression

(defn make-regression-datasource
  (^DataSource [ds]
   (make-regression-datasource ds nil))
  (^DataSource [ds inf-col-name]
   (if inf-col-name
     (-> (modelling/set-inference-target ds inf-col-name)
         (ds->datasource ds->regression-outputs))
     (ds->datasource ds ds->regression-outputs))))


(defn train-regression
  ^Model [^Trainer trainer ds & [inf-col-name]]
  (.train trainer (-> (make-regression-datasource ds inf-col-name)
                      (MutableDataset.))))


(defn predict-regression
  [^Model model ds]
  (when-not (== 0 (ds/row-count ds))
    (ds/->dataset {:prediction (->> (.predict model (make-regression-datasource ds))
                                    (dtype/emap prediction-regval :float64)
                                    (dtype/clone))})))
