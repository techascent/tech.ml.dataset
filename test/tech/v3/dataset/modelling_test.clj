(ns tech.v3.dataset.modelling-test
  (:require [tech.v3.dataset.modelling :as modelling]
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
