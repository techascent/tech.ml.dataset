(ns tech.ml.dataset.pipeline-test
  (:require [tech.ml.dataset.pipeline :as dsp]
            [tech.ml.dataset.pipeline.pipeline-operators
             :refer [pipeline-train-context
                     pipeline-inference-context]]
            [tech.ml.dataset.pipeline.column-filters :as cf]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [clojure.test :refer :all]))



(defn mini-pipeline
  [dataset]
  (-> dataset
      ds/->dataset
      (dsp/store-variables #(hash-map :dataset-shape (dtype/shape %)))
      (dsp/range-scale :x)
      (dsp/m= :norm-x #(dtype-fn// (dsp/col :x)
                                   (first (dsp/read-var
                                           :dataset-shape))))
      (dsp/pwhen (dsp/training?)
       #(ds/set-inference-target % :y))))


(deftest pipeline-context
  []
  (let [f (partial * 2)
        ;;Note the train and tests ranges have different ranges
        train-dataset (for [x (range -5.0 5.0 0.1)] {:x x :y (f x)})
        test-dataset (for [x (range -9.9 10 0.1)] {:x x :y (f x)})
        {train-dataset :dataset
         train-context :context} (pipeline-train-context
                                  (mini-pipeline train-dataset))
        {test-dataset :dataset} (pipeline-inference-context
                                 train-context
                                 (mini-pipeline test-dataset))]
    (is (= [-1 1]
           (map (comp int
                      (ds-col/stats (ds/column train-dataset :x)
                                    [:min :max]))
                [:min :max])))


    (is (= [-0.5 0.5]
           (map (comp double
                      (ds-col/stats (ds/column train-dataset :norm-x)
                                    [:min :max]))
                [:min :max])))

    (is (= [-2 2]
           (map (comp #(Math/round (double %))
                      (ds-col/stats (ds/column test-dataset :x)
                                    [:min :max]))
                [:min :max])))

    (is (= [-1 1]
           (map (comp #(Math/round (double %))
                      (ds-col/stats (ds/column test-dataset :norm-x)
                                    [:min :max]))
                [:min :max])))

    (is (= 1 (count (cf/target? train-dataset))))
    (is (= 0 (count (cf/target? test-dataset))))))
