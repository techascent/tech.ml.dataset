(ns tech.v3.dataset.tribuo-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.tribuo :as tribuo]
            [tech.v3.dataset.modelling :as ds-model]
            [tech.v3.dataset.categorical :as ds-cat]
            [clojure.test :refer [deftest is]])
  (:import [org.tribuo.classification.sgd.linear LogisticRegressionTrainer]
           [org.tribuo.classification.xgboost XGBoostClassificationTrainer]
           [org.tribuo.regression.xgboost XGBoostRegressionTrainer]
           [org.tribuo DataSource MutableDataset]))



(defn classification-example-ds
  [x]
  (let [x (if (integer? x)
            (vec (repeatedly x rand))
            x)
        y (repeatedly (count x) rand)
        label (dtype/emap #(if (< 0.25 % 0.75)
                             "green"
                             "red")
                          :string x)]
    (ds/->dataset {:x x
                   :y y
                   :label label})))


(deftest classification-pathway
  (let [ds (classification-example-ds 10000)
        ;;This is not necessary for tribuo's classification pathway.  Below is just setup
        ;;to test that if someone has a pipeline that is already using categorical mapping
        ;;they can still classify without too many changes.
        cat-data (ds-cat/fit-categorical-map ds :label)
        ds (ds-cat/transform-categorical-map ds cat-data)
        {:keys [test-ds train-ds]} (ds-model/train-test-split ds)
        ;;You can pull in many different classification trainers
        model (tribuo/train-classification (XGBoostClassificationTrainer. 6) train-ds :label)
        infer-ds (ds/remove-columns test-ds [:label])
        predict-ds (tribuo/predict-classification model infer-ds)
        n-rows (ds/row-count predict-ds)
        predictions (predict-ds :prediction)
        prob-ds (ds/drop-columns predict-ds [:prediction])
        num-correct (dfn/sum (dfn/eq predictions
                                     ;;reverse map the categorical mapping to get back string
                                     ;;labels
                                     (-> (ds-cat/invert-categorical-map test-ds cat-data)
                                         (ds/column :label))))
        accuracy (/ num-correct n-rows)]
    (is (not (nil? (ds/column predict-ds :prediction))))
    (is (== 2 (ds/column-count prob-ds)))
    (is (> accuracy 0.9))))


(deftest regression-pathway
  (let [ds (ds/->dataset "test/data/winequality-red.csv" {:separator \;})
        target-cname "quality"
        {:keys [test-ds train-ds]} (ds-model/train-test-split ds)
        model (tribuo/train-regression (XGBoostRegressionTrainer. 50) train-ds "quality")
        predictions (tribuo/predict-regression model (ds/remove-columns test-ds ["quality"]))
        mae (-> (dfn/- (predictions :prediction) (test-ds "quality"))
                (dfn/abs)
                (dfn/mean))]
    (is (< mae 0.5))))
