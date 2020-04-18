(ns tech.ml.dataset.interpolate-test
  (:require [tech.ml.dataset :as ds]
            [clojure.test :refer [deftest is]]))


(deftest basic-interp
  (let [interp-ds (as-> (ds/->dataset "test/data/stocks.csv") ds
                    (ds/filter-column #(= "MSFT" %) "symbol" ds)
                    ;;The interpolate requires a sorted dataset
                    (ds/->sort-by-column ds "date")
                    (ds/interpolate-loess ds "date" "price"
                                          {:result-name "price-loess"}))]
    (is (not (nil? (:interpolator (meta (interp-ds "price-loess"))))))))
