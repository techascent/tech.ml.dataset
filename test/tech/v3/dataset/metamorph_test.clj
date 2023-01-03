(ns tech.v3.dataset.metamorph-test
  (:require [tech.v3.dataset.metamorph :as ds-mm]
            [tech.v3.dataset :as ds]
            [clojure.test :as t :refer [deftest is]]))

(def df
  (ds/->dataset "test/data/ames-train.csv.gz" {:key-fn keyword}))


(deftest call-with-df-1
  (is (= [1 2 3 4 5]
         (->>
          ((ds-mm/set-inference-target :SalePrice) df)
          :metamorph/data
          :Id
          (take 5)
          ))))

(deftest call-with-df-2
  (is (= [1 2 3 4 5]
         (->>
          ((ds-mm/rename-columns {:SalePrice :sale-price :Id :id})
           {:metamorph/data df})
          ((ds-mm/set-inference-target :sale-price) )
          :metamorph/data
          :id
          (take 5)))))

(deftest brief
  (let [df (ds/select-columns df (sort (ds/column-names df)))]
    (is (= 334.0
           (->>
            ((ds-mm/brief)
             {:metamorph/data df})
            :metamorph/data
            first
            :min)))))
