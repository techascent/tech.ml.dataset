(ns tech.ml.dataset-test
  (:require [tech.ml.dataset :as dataset]
            [tech.libs.tablesaw-test :as tbl-test]
            [clojure.test :refer :all]
            [clojure.core.matrix :as m]))



(deftest k-fold-sanity
  (let [dataset-seq (dataset/->k-fold-datasets (tbl-test/mapseq-fruit-dataset) 5 {})]
    (is (= 5 (count dataset-seq)))
    (is (= [[7 47] [7 47] [7 47] [7 47] [7 48]]
           (->> dataset-seq
                (mapv (comp m/shape :train-ds)))))
    (is (= [[7 12] [7 12] [7 12] [7 12] [7 11]]
           (->> dataset-seq
                (mapv (comp m/shape :test-ds)))))))


(deftest train-test-split-sanity
  (let [dataset (dataset/->train-test-split (tbl-test/mapseq-fruit-dataset) {})]
    (is (= [7 41]
           (m/shape (:train-ds dataset))))
    (is (= [7 18]
           (m/shape (:test-ds dataset))))))
