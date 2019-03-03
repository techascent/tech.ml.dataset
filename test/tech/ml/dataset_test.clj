(ns tech.ml.dataset-test
  (:require [tech.ml.dataset :as dataset]
            [tech.libs.tablesaw-test :as tbl-test]
            [tech.compute.tensor :as ct]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.ml.dataset.pca :as pca]
            [clojure.test :refer :all]
            [tech.datatype :as dtype]
            [clojure.core.matrix :as m])
  (:import [smile.projection PCA]))



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



(deftest tensor-and-back
  (let [test-tensor (ct/->tensor (->> (range 25)
                                      shuffle
                                      (partition 5)))
        ds (ds-tens/row-major-tensor->dataset test-tensor)

        ;; _ (println test-tensor)
        ;; _ (clojure.pprint/print-table (dataset/->flyweight ds))

        result-tens (ds-tens/dataset->row-major-tensor ds :float64)]
    (is (m/equals (ct/to-core-matrix test-tensor)
                  (ct/to-core-matrix result-tens)))))


(deftest pca
  (let [test-data (ct/->tensor (->> (range 25)
                                    shuffle
                                    (partition 5)))
        test-ds (ds-tens/row-major-tensor->dataset test-data)
        pca-info (pca/pca-dataset test-ds)
        transformed-ds (pca/pca-transform-dataset test-ds pca-info 3 :float64)
        trans-tens (ds-tens/dataset->row-major-tensor transformed-ds :float64)
        smile-svd-pca (doto (PCA. (->> test-data
                                       ct/rows
                                       (map dtype/->array-copy)
                                       (into-array (Class/forName "[D"))))
                        (.setProjection (int 3)))
        smile-transformed-ds (-> (.project smile-svd-pca
                                           (->> test-data
                                                (ct/rows)
                                                (map dtype/->array-copy)
                                                (into-array (Class/forName "[D"))))
                                 (ct/->tensor))]
    ;;Make sure we get the same answer as smile.
    (is (m/equals (ct/to-core-matrix trans-tens)
                  (ct/to-core-matrix smile-transformed-ds)
                  0.001))))
