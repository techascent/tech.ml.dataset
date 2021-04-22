(ns tech.v3.dataset.smile.data-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.smile.data :as smile-data]
            [clojure.test :refer [deftest is]])
  (:import [smile.data DataFrame]))


(deftest stocks-test
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        df-stocks (smile-data/dataset->smile-dataframe stocks)
        new-val (smile-data/smile-dataframe->dataset df-stocks)]
    (is (instance? DataFrame df-stocks))
    ;;Datetime types included
    (is (= (vec ((ds/ensure-array-backed stocks) "date"))
           (vec (new-val "date"))))
    (is (= (vec ((ds/ensure-array-backed stocks) "symbol"))
           (vec (new-val "symbol"))))
    (is (dfn/equals (stocks "price")
                    (new-val "price")))))


(deftest ames-test
  (let [ames-src (-> (ds/->dataset "test/data/ames-house-prices/train.csv")
                     (ds/select-rows (range 10)))
        ames-ary (ds/ensure-array-backed ames-src)
        df-ames (smile-data/dataset->smile-dataframe ames-ary)
        new-val (smile-data/smile-dataframe->dataset df-ames)]
    (is (every? = (map vector
                       (map (comp :datatype meta) (vals ames-src))
                       (map (comp :datatype meta) (vals ames-ary))
                       (map (comp :datatype meta) (vals new-val))
                       )))
    (is (java.util.Objects/equals (ds/missing ames-src)
                                  (ds/missing ames-ary)))
    ;;Missing for booleans gets lost in the translation with inference turned on.
    #_(is (java.util.Objects/equals (ds/missing ames-src)
                                    (ds/missing new-val)))
    (is (instance? DataFrame df-ames))
    ;;Datetime types included
    (is (= (vec (ames-src "SalePrice"))
           (vec (new-val "SalePrice"))))
    ;;Missing for booleans gets lost in the translation with inference turned on.
    #_(is (= (vec (ames-src "PoolQC"))
           (vec (new-val "PoolQC"))))))
