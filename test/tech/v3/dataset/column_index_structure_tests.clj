(ns tech.v3.dataset.column-index-structure-tests
  (:import  [java.util TreeMap LinkedHashMap])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :refer [index-structure]]
            [tech.v3.dataset.impl.column-index-structure :refer [select-from-index] :as col-index]
            [clojure.test :refer [testing deftest is]]))


(deftest test-index-structure
  (let [DS (ds/->dataset {:continuous  [1 2 3]
                          :categorical [:a :b :c]})]
    (is (= TreeMap
           (-> (:continuous DS)
               index-structure
               type)))
    (is (= LinkedHashMap
           (-> (:categorical DS)
               index-structure
               type)))))


(testing "select-from-index"
  (deftest test-with-continuous-data
    (let [DS (ds/->dataset {:continuous [-1 4 9 10]})]
      (is (= {9 [2]
              10 [3]}
             (-> (:continuous DS)
                 index-structure
                 (select-from-index ::col-index/slice {:from 5 :to 10}))))
      (is (= {9 [2]}
             (-> (:continuous DS)
                 index-structure
                 (select-from-index ::col-index/slice {:from 5 :from-inclusive? false
                                                       :to 10  :to-inclusive? false}))))))


  (deftest test-categorical-data
    (let [DS (ds/->dataset {:keywords [:a :b :c]
                            :strings  ["a" "b" "c"]
                            :symbols  ['a 'b 'c]})]
      (is (= {"a" [0]
              "c" [2]}
            (-> (:strings DS)
                index-structure
                (select-from-index ::col-index/pick ["a" "c"]))))
      (is (= {'a [0]
              'c [2]}
            (-> (:keywords DS)
                index-structure
                (select-from-index ::col-index/pick [:a :c]))))
      ;; (is (= {'a [0]
      ;;         'c [2]}
      ;;       (-> (:symbols DS)
      ;;           index-structure
      ;;           (select-from-index ::col-index/pick ['a 'c]))))
      )))
