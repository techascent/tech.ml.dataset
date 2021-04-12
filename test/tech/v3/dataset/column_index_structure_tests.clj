(ns tech.v3.dataset.column-index-structure-tests
  (:import [java.util TreeMap])
  (:require [tech.v3.datatype :refer [make-container]]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :refer [index-structure]]
            [tech.v3.dataset.impl.column-index-structure :refer [select-from-index] :as col-index]
            [clojure.test :refer [deftest is]]))

(deftest slice-from-index-test
  (let [DS (ds/->dataset {:A [-1 9 4 10]})]
    (is (= {9 [1], 10 [3]}
           (-> (:A DS)
               index-structure
               (select-from-index ::col-index/slice {:from 5 :to 10})))))
  ;; categorical
  (let [DS (ds/->dataset {:keywords [:a :b :c]
                          :strings  ["a" "b" "c"]
                          :symbols  ['a 'b 'c]})]
    (is (= {:a [0], :c [2]}
           (-> (:keywords DS)
               index-structure
               (select-from-index ::col-index/pick [:a :c]))))
    (is (= {"a" [0], "c" [2]}
           (-> (:strings DS)
               index-structure
               (select-from-index ::col-index/pick ["a" "c"]))))
    (is (= {'a [0], 'c [2]}
           (-> (:symbols DS)
               index-structure
               (select-from-index ::col-index/pick ['a 'c]))))))


(comment
  (let [DS (ds/->dataset {:keywords [:a :b :c]})]
    (-> (:keywords DS)
        index-structure
        (select-from-index ::col-index/pick [:a :c])))

  )
