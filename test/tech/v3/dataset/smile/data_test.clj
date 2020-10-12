(ns tech.v3.dataset.smile.data-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]])
  (:import [smile.data DataFrame]))


(deftest stocks-test
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        df-stocks (ds/dataset->smile-dataframe stocks)
        new-val (ds/->dataset df-stocks)]
    (is (instance? DataFrame df-stocks))
    ;;Datetime types included
    (is (= (vec ((ds/ensure-array-backed stocks) "date"))
           (vec (new-val "date"))))
    (is (= (vec ((ds/ensure-array-backed stocks) "symbol"))
           (vec (new-val "symbol"))))
    (is (dfn/equals (stocks "price")
                    (new-val "price")))))


(deftest ames-test
  (let [ames (-> (ds/->dataset "data/ames-house-prices/train.csv")
                 (ds/select-rows (range 10))
                 (ds/ensure-array-backed))
        df-ames (ds/dataset->smile-dataframe ames)
        new-val (ds/->dataset df-ames)]
    (is (instance? DataFrame df-ames))
    ;;Datetime types included
    (is (= (vec (ames "SalePrice"))
           (vec (new-val "SalePrice"))))
    (is (= (vec (ames "PoolQC"))
           (vec (new-val "PoolQC"))))))
