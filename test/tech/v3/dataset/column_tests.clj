(ns tech.v3.dataset.column-tests
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
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
