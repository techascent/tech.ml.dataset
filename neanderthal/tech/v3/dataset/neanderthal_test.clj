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
