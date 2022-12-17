(ns tech.v3.dataset.neanderthal-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.neanderthal :as ds-neanderthal]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is testing]]))


(deftest base-ds-test
  (let [test-ds (ds/->dataset {:a [1 2 3 4 5]
                               :b [6 7 8 9 10]
                               :c [11 12 13 14 15]})]
    (testing "Column major conversion"
      (let [n-mat (ds-neanderthal/dataset->dense test-ds :column)
            res-ds (ds-neanderthal/dense->dataset n-mat)]
        (is (= 3 (ds/column-count res-ds)))
        (is (every? #(dfn/equals (first %) (second %))
                    (map vector
                         (ds/columns test-ds)
                         (ds/columns res-ds))))))
    (testing "Row major conversion"
      (let [n-mat (ds-neanderthal/dataset->dense test-ds :row)
            res-ds (ds-neanderthal/dense->dataset n-mat)]
        (is (= 3 (ds/column-count res-ds)))
        (is (every? #(dfn/equals (first %) (second %))
                    (map vector
                         (ds/columns test-ds)
                         (ds/columns res-ds))))))
    (testing "Column major conversion - float32"
      (let [n-mat (ds-neanderthal/dataset->dense test-ds :column :float32)
            res-ds (ds-neanderthal/dense->dataset n-mat)]
        (is (= 3 (ds/column-count res-ds)))
        (is (every? #(dfn/equals (first %) (second %))
                    (map vector
                         (ds/columns test-ds)
                         (ds/columns res-ds))))))))


(deftest single-row-test
  (let [test-ds (ds/->dataset {:a [1]
                               :b [6]
                               :c [11]})]
    (testing "Column major conversion"
      (let [n-mat (ds-neanderthal/dataset->dense test-ds :column)
            res-ds (ds-neanderthal/dense->dataset n-mat)]
        (is (= 3 (ds/column-count res-ds)))
        (is (every? #(dfn/equals (first %) (second %))
                    (map vector
                         (ds/columns test-ds)
                         (ds/columns res-ds))))))
    (testing "Row major conversion"
      (let [n-mat (ds-neanderthal/dataset->dense test-ds :row)
            res-ds (ds-neanderthal/dense->dataset n-mat)]
        (is (= 3 (ds/column-count res-ds)))
        (is (every? #(dfn/equals (first %) (second %))
                    (map vector
                         (ds/columns test-ds)
                         (ds/columns res-ds))))))
    (testing "Column major conversion - float32"
      (let [n-mat (ds-neanderthal/dataset->dense test-ds :column :float32)
            res-ds (ds-neanderthal/dense->dataset n-mat)]
        (is (= 3 (ds/column-count res-ds)))
        (is (every? #(dfn/equals (first %) (second %))
                    (map vector
                         (ds/columns test-ds)
                         (ds/columns res-ds)))))))
  )


#_(deftest ^:travis-broken pca-smile
  (let [test-data (dtt/transpose
                   (dtt/->tensor [[7 4 6 8 8 7 5 9 7 8]
                                  [4 1 3 6 5 2 3 5 4 2]
                                  [3 8 5 1 7 9 3 8 5 2]] :datatype :float64)
                   [1 0])
        test-ds (ds-tens/tensor->dataset test-data)
        svd-fit (ds-math/fit-pca test-ds {:method :svd :n-components 2})
        svd-transformed-ds (ds-math/transform-pca test-ds svd-fit)
        corr-fit (ds-math/fit-pca test-ds {:method :cov :n-components 2})
        corr-transformed-ds (ds-math/transform-pca test-ds corr-fit)
        ;;Slow, partially correct smile method (only correct for n-rows > n-cols)
        smile-svd-pca (doto (PCA/fit (->> test-data
                                          dtt/rows
                                          (map dtype/->double-array)
                                          (into-array (Class/forName "[D"))))
                        (.setProjection (int 2)))
        smile-transformed-ds (-> (.project smile-svd-pca
                                           (->> test-data
                                                (dtt/rows)
                                                (map dtype/->double-array)
                                                (into-array (Class/forName "[D"))))
                                 (dtt/->tensor))]
    ;;Make sure we get the same answer as smile.
    (is (= :svd (:method svd-fit)))
    (is (dfn/equals smile-transformed-ds
                    (ds-tens/dataset->tensor svd-transformed-ds)
                    0.01))
    (is (= :cov (:method corr-fit)))
    (is (dfn/equals smile-transformed-ds
                    (ds-tens/dataset->tensor corr-transformed-ds)
                    0.01))))
