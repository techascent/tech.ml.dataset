(ns tech.v3.dataset.column-tests
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset.column-filters :as cf]
            [clojure.test :refer [deftest is]]))


(deftest column-copy-test
  []
  (let [short-col (:a (ds/->dataset (interleave
                                     (repeat 10 {:a (short 25)})
                                     (repeat 10 {:a nil}))))]
    (is (= (vec (apply concat  (repeat 10 [25 -32768])))
           (vec  (dtype/->array short-col))))
    (is (= (vec (apply concat (repeat 10 [true false])))
           (vec (dfn/finite? (dtype/->array :float64 short-col)))))
    (is (= (vec (apply concat (repeat 10 [true false])))
           (vec (dfn/finite? short-col))))
    (is (= 25
           (Math/round (dfn/mean short-col))))))

(deftest select-columns-test
  (let [DS (ds/->dataset {:A [1 2 3]
                          :B [4 5 6]
                          :C ["A" "B" "C"]})]
    (is (= (ds/select-columns DS [:C])
           (ds/select-columns DS cf/categorical)))
    (is (= (ds/select-columns DS cf/numeric)
           (ds/select-columns DS [:A :B])))))

(deftest drop-columns-test
  (let [DS (ds/->dataset {:A [1 2 3]
                          :B [4 5 6]
                          :C ["A" "B" "C"]})]
    (is (= (ds/drop-columns DS cf/categorical)
           (ds/remove-columns DS cf/categorical)
           (ds/select-columns DS [:A :B])))
    (is (= (ds/drop-columns DS cf/numeric)
           (ds/remove-columns DS cf/numeric)
           (ds/select-columns DS [:C])))))

(deftest select-columns-test
  (let [c (col/new-column :test [0 1 2 3 4 5])]
    (is (= [0 1 2]
           (col/select c [0 1 2])))
    (is (= [0 1 2]
           (col/select c (dfn/< c 3))))))
