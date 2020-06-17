(ns tech.ml.dataset.object-columns-test
  (:require [clojure.test :refer :all]
            [tech.ml.dataset :as ds]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.tensor :as dtt]))



(deftest basic-object-columns
  (let [src-ds (ds/name-values-seq->dataset
                {:a (range 10)
                 :b (repeat 10 {:a 1 :b 2})})]
    (is (= :object
           (dtype/get-datatype (src-ds :b))))
    (is (= (vec (repeat 10 {:a 1 :b 2}))
           (vec (dtype/->reader (src-ds :b)))))))



(deftest involved-object-columns
  (let [src-ds (ds/name-values-seq->dataset
                {:dates (list "2000-01-01" "2000-02-01" "2000-03-01"
                              "2000-04-01" "2000-05-01")
                 :integers (range 5)
                 :durations (repeat 5 (dtype-dt/duration))
                 :doubles (map double (range 5))
                 :tensors (repeat 5 (dtt/->tensor (partition 2 (range 4))))})]
    (is (= #{:float64 :string :int64 :object
             :packed-duration}
           (->> (map dtype/get-datatype src-ds)
                set)))))
