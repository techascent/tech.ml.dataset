(ns tech.v3.dataset.modelling-test
  (:require [tech.v3.dataset.modelling :as modelling]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.categorical :as ds-cat]
            [tech.v3.dataset.test-utils :as test-utils]
            [tech.v3.datatype :as dtype]
            [clojure.test :refer [deftest is]]))

(deftest k-fold-sanity
  (let [dataset-seq (modelling/k-fold-datasets (test-utils/mapseq-fruit-dataset) 5 {})]
    (is (= 5 (count dataset-seq)))
    (is (= [[7 47] [7 47] [7 47] [7 47] [7 48]]
           (->> dataset-seq
                (mapv (comp dtype/shape :train-ds)))))
    (is (= [[7 12] [7 12] [7 12] [7 12] [7 11]]
           (->> dataset-seq
                (mapv (comp dtype/shape :test-ds)))))))


(deftest train-test-split-sanity
  (let [dataset (modelling/train-test-split
                 (test-utils/mapseq-fruit-dataset) {})]
    (is (= [7 41]
           (dtype/shape (:train-ds dataset))))
    (is (= [7 18]
           (dtype/shape (:test-ds dataset))))))


(deftest prob-dist->label-col
  (let [ds (ds/->dataset (tech.v3.dataset/->dataset
                          {:y-0 [0.0 0.5 0.3 0.1]
                           :y-1 [0.3 0.8 0.2 0.3]}))
        prob-dist-ds (modelling/probability-distributions->label-column ds :y)
        label-ds (ds-cat/reverse-map-categorical-xforms prob-dist-ds)]
    (is (= [:y-1 :y-1 :y-0 :y-1]
           (label-ds :y)))))


(deftest issue-267-prob-dist-fail-on-nan-missing
  (is (thrown? Throwable
               (-> (tech.v3.dataset/->dataset {:y-0 [Double/NaN] :y-1 [0.3]})
                   (modelling/probability-distributions->label-column :y))))
  (is (thrown? Throwable
               (-> (tech.v3.dataset/->dataset {:y-0 [nil] :y-1 [0.3]} )
                   (tech.v3.dataset.modelling/probability-distributions->label-column :y)))))
