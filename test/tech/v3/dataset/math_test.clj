(ns tech.v3.dataset.math-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.math :as ds-math]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.tensor :as dtt]
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


(comment
  (def test-ds (ds/->dataset {:a [7 4 6 8 8 7 5 9 7 8]
                              :b [4 1 3 6 5 2 3 5 4 2]
                              :c [3 8 5 1 7 9 3 8 5 2]}))

  (def test-data (dtt/->tensor [[10  8  6 20  9]
                                [11 21 23 18  4]
                                [12  7  5 13 19]
                                [ 3 14 15 22 17]
                                [24  1  2  0 16]] :datatype :float64))
  (def test-data (dtt/transpose (dtt/->tensor [[7 4 6 8 8 7 5 9 7 8]
                                               [4 1 3 6 5 2 3 5 4 2]
                                               [3 8 5 1 7 9 3 8 5 2]] :datatype :float64)
                                [1 0]))
  )
