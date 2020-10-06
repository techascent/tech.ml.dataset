(ns tech.v3.dataset.modelling-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling :as modelling]
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


;; (deftest train-test-split-sanity
;;   (let [dataset (ds/->train-test-split (mapseq-fruit-dataset) {})]
;;     (is (= [7 41]
;;            (dtype/shape (:train-ds dataset))))
;;     (is (= [7 18]
;;            (dtype/shape (:test-ds dataset))))))



;; (deftest tensor-and-back
;;   (let [test-tensor (tens/->tensor (->> (range 25)
;;                                         shuffle
;;                                         (partition 5)))
;;         ds (ds-tens/row-major-tensor->dataset test-tensor)

;;         ;; _ (println test-tensor)
;;         ;; _ (clojure.pprint/print-table (ds/->flyweight ds))

;;         result-tens (ds-tens/dataset->row-major-tensor ds :float64)]
;;     (is (= (tens/->jvm test-tensor :datatype :int32)
;;            (tens/->jvm result-tens :datatype :int32)))))


;; (deftest n-permutations
;;   (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
;;                (ds/set-inference-target :fruit-name))]
;;     (is (= 35
;;            (count (ds/n-permutations 3 ds))))
;;     (is (= 20
;;            (count (ds/n-feature-permutations 3 ds))))
;;     ;;The label column shows up in every permutation.
;;     (is (every? :fruit-name
;;                 (->> (ds/n-feature-permutations 3 ds)
;;                      (map (comp set ds/column-names)))))))
