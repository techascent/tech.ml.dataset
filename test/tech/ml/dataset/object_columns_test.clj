(ns tech.ml.dataset.object-columns-test
  (:require [clojure.test :refer :all]
            [tech.ml.dataset :as ds]
            [tech.v2.datatype :as dtype]))



(deftest basic-object-columns
  (let [src-ds (ds/name-values-seq->dataset
                {:a (range 10)
                 :b (repeat 10 {:a 1 :b 2})})]
    (is (= :object
           (dtype/get-datatype (src-ds :b))))
    (is (= (vec (repeat 10 {:a 1 :b 2}))
           (vec (dtype/->reader (src-ds :b)))))))
