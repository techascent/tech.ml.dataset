(ns tech.ml.dataset-test
  (:require [tech.ml.dataset :as dataset]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline :as ds-pipe]
            [tech.v2.tensor :as tens]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.ml.dataset.pca :as pca]
            [clojure.test :refer :all]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.unary-op :as unary-op]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [camel-snake-kebab.core :refer [->kebab-case]])
  (:import [smile.projection PCA]))


(def mapseq-fruit-dataset
  (memoize
   (fn []
     (let [fruit-ds (slurp (io/resource "fruit_data_with_colors.txt"))
           dataset (->> (s/split fruit-ds #"\n")
                        (mapv #(s/split % #"\s+")))
           ds-keys (->> (first dataset)
                        (mapv (comp keyword ->kebab-case)))]
       (->> (rest dataset)
            (map (fn [ds-line]
                   (->> ds-line
                        (map (fn [ds-val]
                               (try
                                 (Double/parseDouble ^String ds-val)
                                 (catch Throwable e
                                   (-> (->kebab-case ds-val)
                                       keyword)))))
                        (zipmap ds-keys)))))))))



(deftest k-fold-sanity
  (let [dataset-seq (dataset/->k-fold-datasets (mapseq-fruit-dataset) 5 {})]
    (is (= 5 (count dataset-seq)))
    (is (= [[7 47] [7 47] [7 47] [7 47] [7 48]]
           (->> dataset-seq
                (mapv (comp dtype/shape :train-ds)))))
    (is (= [[7 12] [7 12] [7 12] [7 12] [7 11]]
           (->> dataset-seq
                (mapv (comp dtype/shape :test-ds)))))))


(deftest train-test-split-sanity
  (let [dataset (dataset/->train-test-split (mapseq-fruit-dataset) {})]
    (is (= [7 41]
           (dtype/shape (:train-ds dataset))))
    (is (= [7 18]
           (dtype/shape (:test-ds dataset))))))



(deftest tensor-and-back
  (let [test-tensor (tens/->tensor (->> (range 25)
                                        shuffle
                                        (partition 5)))
        ds (ds-tens/row-major-tensor->dataset test-tensor)

        ;; _ (println test-tensor)
        ;; _ (clojure.pprint/print-table (dataset/->flyweight ds))

        result-tens (ds-tens/dataset->row-major-tensor ds :float64)]
    (is (= (tens/->jvm test-tensor :datatype :int32)
           (tens/->jvm result-tens :datatype :int32)))))


(deftest pca
  (let [test-data (tens/->tensor (->> (range 25)
                                    shuffle
                                    (partition 5)))
        test-ds (ds-tens/row-major-tensor->dataset test-data)
        pca-info (pca/pca-dataset test-ds)
        transformed-ds (pca/pca-transform-dataset test-ds pca-info 3 :float64)
        trans-tens (ds-tens/dataset->row-major-tensor transformed-ds :float64)
        smile-svd-pca (doto (PCA. (->> test-data
                                       tens/rows
                                       (map dtype/->array-copy)
                                       (into-array (Class/forName "[D"))))
                        (.setProjection (int 3)))
        smile-transformed-ds (-> (.project smile-svd-pca
                                           (->> test-data
                                                (tens/rows)
                                                (map dtype/->array-copy)
                                                (into-array (Class/forName "[D"))))
                                 (tens/->tensor))]
    ;;Make sure we get the same answer as smile.
    (is (dtype-fn/equals trans-tens
                         smile-transformed-ds
                         0.01))))


(deftest n-permutations
  (let [ds (-> (dataset/->dataset (mapseq-fruit-dataset))
               (dataset/set-inference-target :fruit-name))]
    (is (= 35
           (count (dataset/n-permutations ds 3))))
    (is (= 20
           (count (dataset/n-feature-permutations ds 3))))
    ;;The label column shows up in every permutation.
    (is (every? :fruit-name
                (->> (dataset/n-feature-permutations ds 3)
                     (map (comp set dataset/column-names)))))))


(deftest iterable
  (let [ds (dataset/->dataset (mapseq-fruit-dataset))]
    (is (= (dataset/column-names ds)
           (map ds-col/column-name ds)))))


(deftest string-column-add-or-update
  (let [ds (-> (dataset/->dataset (mapseq-fruit-dataset))
               (dataset/update-column :fruit-name
                                      #(->> (dtype/->reader %)
                                            (unary-op/unary-reader
                                             :string
                                             (.concat ^String x "-fn")))))]
    (is (= ["apple-fn" "apple-fn" "apple-fn" "mandarin-fn" "mandarin-fn"
            "mandarin-fn" "mandarin-fn" "mandarin-fn" "apple-fn" "apple-fn"]
           (->> (ds :fruit-name)
                (dtype/->reader)
                (take 10)
                vec)))))


(deftest name-values-seq->dataset-test
  (is (= [{:a 0.0, :b "a"} {:a 1.0, :b "b"} {:a 2.0, :b "c"}
          {:a 3.0, :b "d"} {:a 4.0, :b "e"}]
         (-> (dataset/name-values-seq->dataset
              {:a (double-array (range 5))
               :b ["a" "b" "c" "d" "e"]})
             (dataset/mapseq-reader))))
  (is (thrown? Throwable
               (dataset/name-values-seq->dataset
                {:a (double-array (range 5))
                 :b ["a" "b" "c" "d"]})))
  (is (= [{:a 0, :b "a"} {:a 1, :b "b"} {:a 2, :b "c"}
          {:a 3, :b "d"} {:a 4, :b "e"}]
         (-> (dataset/name-values-seq->dataset
              {:a (long-array (range 5))
               :b ["a" "b" "c" "d" "e"]})
             (dataset/mapseq-reader)))))


(deftest unique-by-test
  (let [ds (dataset/->dataset (mapseq-fruit-dataset))]
    (is (= [7 4]
           (dtype/shape (dataset/unique-by :fruit-name ds))))
    (is (= [7 4]
           (dtype/shape (dataset/unique-by-column :fruit-name ds))))
    (is (= #{"apple" "orange" "lemon" "mandarin"}
           (->> (dataset/column (dataset/unique-by-column :fruit-name ds)
                                :fruit-name)
                set)))

    (is (= [7 24]
           (dtype/shape (dataset/unique-by :width ds))))
    (is (= [7 24]
           (dtype/shape (dataset/unique-by-column :width ds))))
    (is (dtype-fn/equals [5.8 5.9 6.0 6.1 6.2 6.3 6.5
                          6.7 6.8 6.9 7.0 7.1 7.2 7.3
                          7.4 7.5 7.6 7.7 7.8 8.0 8.4
                          9.0 9.2 9.6]
                         (->> (dataset/column (dataset/unique-by-column :width ds)
                                              :width)
                              sort
                              vec)))))


(deftest aggregate-by-test
  (let [ds (dataset/->dataset (mapseq-fruit-dataset))]

    ))
