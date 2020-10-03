(ns tech.v3.dataset.math-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.math :as ds-math]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))

(deftest basic-interp
  (let [interp-ds (-> (ds/->dataset "test/data/stocks.csv")
                      (ds/filter-column "symbol" "MSFT")
                      ;;The interpolate requires a sorted dataset
                      (ds/sort-by-column "date")
                      (ds-math/interpolate-loess "date" "price"
                                                 {:result-name "price-loess"}))]
    (is (not (nil? (:interpolator (meta (interp-ds "price-loess"))))))))


(deftest fill-range-replace
  (let [ds (-> (ds/->dataset {:a [1 5  10 15 20]
                              :b [2 2 nil  4  8]})
               (ds-math/fill-range-replace :a 2))]
    (is (dfn/equals
         [1.0 3.0 5.0 6.66 8.33 10.0
          11.66 13.33 15.0 16.66 18.33 20.0]
         (vec (ds :a))
         0.1))
    (is (= [2 2 2 2 2 2 2 2 4 4 4 8]
           (vec (ds :b)))))
  (let [ds (-> (ds/->dataset {:a [1 5  10 15 20]
                              :b [2 2 nil  4  8]})
               (ds-math/fill-range-replace :a 2 nil))]
    (is (= [2 nil 2 nil nil nil nil nil 4 nil nil 8]
           (vec (ds :b)))))
  (let [ds (-> (ds/->dataset {:a [1 5  10 15 20]
                              :b [2 2 nil  4  8]})
               (ds-math/fill-range-replace :a 2 :value 20))]
    (is (= [2 20 2 20 20 20 20 20 4 20 20 8]
           (vec (ds :b)))))
  (let [ds (-> (ds/->dataset {:a (dtype-dt/plus-temporal-amount
                                  (dtype-dt/local-date)
                                  [1 5  10 15 20]
                                  :days)
                              :b [2 2 nil 4 8]})
               (ds-math/fill-range-replace :a (* 2 dtype-dt/milliseconds-in-day)
                                           :value 20))]
    (is (= [2 20 2 20 20 20 20 20 4 20 20 8]
           (vec (ds :b))))))
