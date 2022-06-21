(ns tech.v3.dataset.column-test
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

(deftest column-select-test
  (let [c (col/new-column :test [0 1 2 3 4 5])]
    (is (= [0 1 2]
           (col/select c [0 1 2])))
    (is (= [0 1 2]
           (col/select c (dfn/< c 3))))))

(deftest dataset-column-select-test
  (let [ds (ds/->dataset {:A [1 2 3 4 5]
                          :B [2 3 4 5 6]})]
    (is (= (ds/->dataset {:A [1 5]
                          :B [2 6]})
           (ds/select ds :all [0 4])))
    (is (= (ds/->dataset {:A [1 2]
                          :B [2 3]})
           (ds/select ds :all (dfn/< (:A ds) 3))))))

(deftest test-tostring
  (is (= "#tech.v3.dataset.column<float64>[3]\n:A\n[2.000, 1.000, 0.000]\n{:categorical? true,\n :name :A,\n :datatype :float64,\n :n-elems 3,\n :categorical-map\n {:lookup-table {:c 0, :b 1, :a 2},\n  :src-column :A,\n  :result-datatype :float64}}\n"
         (-> (ds/->dataset {:A [:a :b :c]})
             (ds/categorical->number [:A])
             :A
           str))))
