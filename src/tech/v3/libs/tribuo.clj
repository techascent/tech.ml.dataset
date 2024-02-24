(ns tech.v3.libs.tribuo
  "Bindings to make working with tribuo more straight forward when using datasets.

```clojure
;; Classification

tech.v3.dataset.tribuo-test> (def ds (classification-example-ds 10000))
#'tech.v3.dataset.tribuo-test/ds
tech.v3.dataset.tribuo-test> (def model (tribuo/train-classification (org.tribuo.classification.xgboost.XGBoostClassificationTrainer. 6) ds :label))
#'tech.v3.dataset.tribuo-test/model
tech.v3.dataset.tribuo-test> (ds/head (tribuo/predict-classification model (ds/remove-columns ds [:label])))
_unnamed [5 3]:

| :prediction |        red |      green |
|-------------|-----------:|-----------:|
|         red | 0.92524981 | 0.07475022 |
|       green | 0.07464883 | 0.92535114 |
|       green | 0.07464883 | 0.92535114 |
|         red | 0.92525917 | 0.07474083 |
|       green | 0.07464883 | 0.92535114 |


  ;; Regression
tech.v3.dataset.tribuo-test> (def ds (ds/->dataset \"test/data/winequality-red.csv\" {:separator \\;}))
#'tech.v3.dataset.tribuo-test/ds
tech.v3.dataset.tribuo-test> (def model (tribuo/train-regression (org.tribuo.regression.xgboost.XGBoostRegressionTrainer. 50) ds \"quality\"))
#'tech.v3.dataset.tribuo-test/model
tech.v3.dataset.tribuo-test> (ds/head (tribuo/predict-regression model (ds/remove-columns ds [\"quality\"])))
_unnamed [5 1]:

| :prediction |
|------------:|
|  5.01974726 |
|  5.02164841 |
|  5.22696543 |
|  5.79519272 |
|  5.01974726 |
```"
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling :as modelling]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [clojure.set :as set]
            [cheshire.core :as json])
  (:import [tech.v3.datatype Buffer ArrayHelpers]
           [java.util HashMap Map List]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.tribuo.classification Label LabelFactory]
           [org.tribuo DataSource Output OutputFactory Trainer Model MutableDataset
            Prediction]
           [org.tribuo.impl ArrayExample]
           [org.tribuo.provenance SimpleDataSourceProvenance]
           [org.tribuo.regression RegressionFactory Regressor]
           [org.tribuo.regression.evaluation RegressionEvaluator RegressionEvaluation]
           [com.oracle.labs.mlrg.olcut.config ConfigurationManager]
           [com.oracle.labs.mlrg.olcut.config.json JsonConfigFactory]))
   

(set! *warn-on-reflection* true)

(ConfigurationManager/addFileFormatFactory (JsonConfigFactory.)) ; allows json config

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

(defn- ensure-numeric-columns
  [ds]
  (when-not (every? casting/numeric-type? (map (comp :datatype meta) (vals ds)))
    (throw (Exception.
            (str "All columns must be numeric at this point.  Non numeric columns:\n"
                 (->> (map meta (vals ds))
                      (filter #(not (casting/numeric-type? (get % :datatype))))
                      (mapv :name))))))
  ds)

(defn- ds->examples
  ^Buffer [ds ds->outputs]
  (let [inf-columns (modelling/inference-target-column-names ds)
        inf-ds (when inf-columns (ds/select-columns ds inf-columns))
        feat-ds (-> (ds/remove-columns ds inf-columns)
                    (ensure-numeric-columns))
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
  "Make a single label classification datasource."
  (^DataSource [ds]
   (make-classification-datasource ds nil))
  (^DataSource [ds inf-col-name]
   (if inf-col-name
     (-> (modelling/set-inference-target ds inf-col-name)
         (ds->datasource ds->single-label-outputs))
     (ds->datasource ds ds->single-label-outputs))))


(defn train-classification
  "Train a single label classification model.  Returns the model."
  ^Model [^Trainer trainer ds & [inf-col-name]]
  (.train trainer (-> (make-classification-datasource ds inf-col-name)
                      (MutableDataset.))))


(defn classification-predictions->dataset
  "Given the list of predictions from a classification model return a dataset
  that will include probability distributions when possible.  The actual prediction
  will be in the `:prediction` column."
  [^List predictions]
  (let [labels (dtype/emap prediction-label :string predictions)
        pred (.get predictions 0)]
    (-> (ds/->dataset
         (merge {:prediction labels}
                (when (has-probabilities? pred)
                  (let [scores (prediction-scores pred)]
                    (->> (.entrySet scores)
                         (map (fn [entry]
                                (let [ekey (key entry)]
                                  [ekey (-> (dtype/emap #(-> (prediction-scores %)
                                                             (.get ekey)
                                                             (label-score))
                                                        :float64
                                                        predictions)
                                            (dtype/clone))])))
                         (into {}))))))
        (modelling/set-inference-target :prediction))))


(defn predict-classification
  "Use this model to predict every row of this dataset returning a new dataset containing
  at least a `:prediction` column.  If this classifier is capable of predicting probability
  distributions those will be returned as per-label as separate columns."
  [^Model model ds]
  (when-not (== 0 (ds/row-count ds))
    (->> (.predict model (make-classification-datasource ds))
         (classification-predictions->dataset))))


;; Regression
(defn make-regression-datasource
  "Make a regression datasource from a dataset."
  (^DataSource [ds]
   (make-regression-datasource ds nil))
  (^DataSource [ds inf-col-name]
   (if inf-col-name
     (-> (modelling/set-inference-target ds inf-col-name)
         (ds->datasource ds->regression-outputs))
     (ds->datasource ds ds->regression-outputs))))


(defn train-regression
  "Train a regression model on a dataset returning the model."
  ^Model [^Trainer trainer ds & [inf-col-name]]
  (.train trainer (-> (make-regression-datasource ds inf-col-name)
                      (MutableDataset.))))


(defn predict-regression
  "Use a regression model to predict each column of the dataset returning a dataset with
  at least one column named `:prediction`."
  [^Model model ds]
  (when-not (== 0 (ds/row-count ds))
    (ds/->dataset {:prediction (->> (.predict model (make-regression-datasource ds))
                                    (dtype/emap prediction-regval :float64)
                                    (dtype/clone))})))


(defn evaluate-regression
  "Evaluate a regression model against this model.  Returns map of
  `{:rmse :mae :r2}`"
  [^Model model ds inf-col-name]
  (let [inf-col-name (->string inf-col-name)
        ^RegressionEvaluation eval (.evaluate (RegressionEvaluator.) model
                                              (MutableDataset. (make-regression-datasource ds inf-col-name)))
        dim (Regressor. inf-col-name Double/NaN)]
    {:rmse (.rmse eval dim)
     :mae (.mae eval dim)
     :r2 (.r2 eval dim)}))



(defn tribuo-trainer
  "Creates a tribuo trainer from a list of config components
  follwing OLCUT convention. One of the components should be a trainer,
  which is the looked-up by `trainer-name` and returned."

  [config-components trainer-name]
  (let [config {:config {:components config-components}}
        tmp (.toString (Files/createTempFile "config" ".json" (make-array FileAttribute 0)))
        _ (->> config json/generate-string (spit tmp))
        config-manager (ConfigurationManager. tmp)]
    (.lookup config-manager trainer-name)))
