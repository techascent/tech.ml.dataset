(ns tech.v3.libs.parquet-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.parquet]
            [tech.v3.dataset.utils :as ds-utils]
            [clojure.test :refer [deftest is]]))

(ds-utils/set-slf4j-log-level :info)


(deftest stocks-test
  (try
    (let [stocks (ds/->dataset "test/data/stocks.csv")
          _ (ds/write! stocks "stocks.parquet")
          stocks-p (ds/->dataset "stocks.parquet")]
      (is (= (vec (stocks "symbol"))
             (mapv str (stocks-p "symbol"))))
      (is (dfn/equals (stocks "price")
                      (stocks-p "price")))
      (is (= (vec (stocks "date"))
             (vec (stocks-p "date")))))
    (finally
      (.delete (java.io.File. "stocks.parquet")))))
