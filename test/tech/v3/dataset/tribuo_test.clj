(ns tech.v3.dataset.tribuo-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling :as ds-model]
            [tech.v3.dataset.categorical :as ds-cat]
            [tech.v3.libs.tribuo :as tribuo]
            [clojure.test :refer [deftest is]])
  (:import [org.tribuo.classification.xgboost XGBoostClassificationTrainer]
           [org.tribuo.regression.xgboost XGBoostRegressionTrainer]))



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
        predict-ds (tribuo/predict-classification model (ds/remove-columns test-ds [:label]))
        num-correct (dfn/sum (dfn/eq (predict-ds :prediction)
                                     ;;reverse map the categorical mapping to get back string
                                     ;;labels
                                     (-> (ds-cat/invert-categorical-map test-ds cat-data)
                                         (ds/column :label))))
        accuracy (/ num-correct (ds/row-count test-ds))]
    (is (not (nil? (ds/column predict-ds :prediction))))
    (is (== 2 (ds/column-count (ds/drop-columns predict-ds [:prediction]))))
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
