(ns tech.v3.dataset.column-index-structure-tests
  (:import  [java.util TreeMap LinkedHashMap])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :refer [index-structure with-index-structure select-from-index]]
            [tech.v3.dataset.impl.column-index-structure :as col-index]
            [clojure.test :refer [testing deftest is]]))


(deftest test-default-index-structure-type-dispatch
  (let [DS (ds/->dataset {:continuous  [1 2 3]
                          :categorical [:a :b :c]})]
    (is (= TreeMap
           (-> (:continuous DS)
               index-structure
               type)))
    (is (= LinkedHashMap
           (-> (:categorical DS)
               index-structure
               type)))
    ;; sensitive to :cateogrical? meta overrides
    (is (= LinkedHashMap
           (-> (:continuous DS)
               (with-meta {:categorical? true})
               index-structure
               type)))
    (is (= TreeMap
           (-> (:categorical DS)
               (with-meta {:categorical? false})
               index-structure
               type)))))


(deftest test-with-index-structure
  (let [DS (ds/->dataset {:categorical [:a :b :c]})]
    (is (= []
        (-> (:categorical DS)
            (with-index-structure :keyword clojure.lang.Keyword (fn [data metadata] []))
            index-structure)))))


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
      (is (= {:a [0]
              :c [2]}
             (-> (:keywords DS)
                 index-structure
                 (select-from-index ::col-index/pick [:a :c]))))
      (is (= {'a [0]
              'c [2]}
             (-> (:symbols DS)
                 index-structure
                 (select-from-index ::col-index/pick ['a 'c])))))))