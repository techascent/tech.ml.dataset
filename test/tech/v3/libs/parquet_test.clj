(ns tech.v3.libs.parquet-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.parquet :as parquet]
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



(deftest userdata1-test
  (try
    (let [testd (ds/->dataset "test/data/parquet/userdata1.parquet")
          _ (ds/write! testd "userdata1.parquet")
          newd (ds/->dataset "userdata1.parquet")
          _ (ds/write! newd "userdata1.nippy")
          nippy-d (ds/->dataset "userdata1.nippy")]
      (is (= (vec (testd "registration_dttm"))
             (vec (newd "registration_dttm"))))
      (is (= (vec (testd "comments"))
             (vec (newd "comments"))))
      (is (= (vec (testd "comments"))
             (vec (nippy-d "comments")))))

    (finally
      (.delete (java.io.File. "userdata1.parquet"))
      (.delete (java.io.File. "userdata1.nippy")))))



(deftest ames-ds
  (try
    (let [ames (ds/->dataset "test/data/ames-house-prices/train.csv")
          _ (ds/write! ames "ames.parquet")
          newd (ds/->dataset "ames.parquet")]
      (is (= (ds/missing (ames "LotFrontage"))
             (ds/missing (newd "LotFrontage"))))
      (is (= (vec (ames "CentralAir"))
             (vec (newd "CentralAir"))))
      (is (dfn/equals (ames "SalePrice") (newd "SalePrice"))))
    (finally
      (.delete (java.io.File. "ames.parquet")))))


(deftest uuid-test
  (try
    (let [uuid-ds (ds/->dataset "test/data/uuid.parquet"
                                {:parser-fn {"uuids" :uuid}})
          _ (ds/write! uuid-ds "test-uuid.parquet")
          new-ds (ds/->dataset "test-uuid.parquet"
                               {:parser-fn {"uuids" :uuid}})]
      (is (= :uuid ((comp :datatype meta) (uuid-ds "uuids"))))
      (is (= :uuid ((comp :datatype meta) (new-ds "uuids")))))
    (finally
      (.delete (java.io.File. "test-uuid.parquet")))))
