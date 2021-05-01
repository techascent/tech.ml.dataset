(ns tech.v3.dataset.column-index-structure-tests
  (:import  [java.util TreeMap LinkedHashMap])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :refer [index-structure with-index-structure
                                            index-structure-realized?]]
            [tech.v3.dataset.column-index-structure :refer [select-from-index]]
            [tech.v3.datatype.datetime :as datetime]
            [clojure.test :refer [testing deftest is]]))

(deftest test-default-index-structure-type-dispatch
  (let [DS (ds/->dataset {:continuous  [1 2 3]
                          :categorical [:a :b :c]
                          :local-dates (datetime/plus-temporal-amount (datetime/local-date) (range 3) :days)
                          :years       (datetime/plus-temporal-amount (java.time.Year/parse "1970") (range 3) :years)})]
    (is (= TreeMap
           (-> (:continuous DS)
               index-structure
               type)))
    (is (= LinkedHashMap
           (-> (:categorical DS)
               index-structure
               type)))
    (is (= TreeMap
           (-> (:local-dates DS)
               index-structure
               type)))
    (is (= TreeMap
           (-> (:years DS)
               index-structure
               type)))
    (is (= LinkedHashMap
           (-> (:continuous DS)
               (with-meta {:categorical? true})
               index-structure
               type)))
    (is (= TreeMap
           (-> (:categorical DS)
               (with-meta {:categorical? false})
               index-structure
               type)))
    (is (= LinkedHashMap
           (-> (:years DS)
               (with-meta {:categorical? true})
               index-structure
               type)))))



(deftest test-index-structure-realized?
  (is (= false
         (-> (:A (ds/->dataset {:A [1 2 3]}))
             (index-structure-realized?))))
  (is (= true
         (let [DS (ds/->dataset {:A [1 2 3]})]
           (index-structure (:A DS))
           (index-structure-realized? (:A DS))))))



(deftest test-with-index-structure
  (let [DS (ds/->dataset {:categorical [:a :b :c]})]
    (is (= []
        (-> (:categorical DS)
            (with-index-structure (fn [data metadata] []))
            index-structure)))))


(deftest test-select-with-index
  (testing "continuous data"
    (let [DS (ds/->dataset {:continuous [-1 4 9 10]})]
      (is (= [2 3] 
             (-> (:continuous DS)
                 index-structure
                 (select-from-index :slice {:from 5 :to 10}))))
      (is (= [2]
             (-> (:continuous DS)
                 index-structure
                 (select-from-index :slice {:from 5 :from-inclusive? false
                                            :to 10  :to-inclusive? false}))))))

  (testing "cateogrical data"
    (let [DS (ds/->dataset {:keywords [:a :b :c]
                            :strings  ["a" "b" "c"]
                            :symbols  ['a 'b 'c]})]
      (is (= [0 2]
             (-> (:strings DS)
                 index-structure
                 (select-from-index :pick ["a" "c"]))))
      (is (= [0 2]
             (-> (:keywords DS)
                 index-structure
                 (select-from-index :pick [:a :c]))))
      (is (= [0 2]
             (-> (:symbols DS)
                 index-structure
                 (select-from-index :pick ['a 'c]))))))

  (testing ":as-index-structure option"
    (let [DS (ds/->dataset {:continuous  [1 2 3]
                            :categorical [:a :b :c]})]
      (is (= {2 [1], 3 [2]}
             (-> (:continuous DS)
                 index-structure
                 (select-from-index :slice
                                    {:from 2 :to 3}
                                    {:as-index-structure true}))))
      (is (= {2 [1], 3 [2]}
             (-> (:continuous DS)
                 index-structure
                 (select-from-index :pick [2 3]
                                    {:as-index-structure true}))))
      (is (= {:b [1], :c [2]}
             (-> (:categorical DS)
                 index-structure
                 (select-from-index :pick
                                    [:b :c]
                                    {:as-index-structure true})))))))





