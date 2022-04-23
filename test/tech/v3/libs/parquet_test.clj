(ns tech.v3.libs.parquet-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.parquet :as parquet]
            [tech.v3.dataset.utils :as ds-utils]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.datetime :as dtype-dt]
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


(deftest whitelist-test
  (let [testd (ds/->dataset "test/data/parquet/userdata1.parquet"
                            {:column-whitelist ["first_name" "last_name" "gender"]})]
    (is (= 3 (ds/column-count testd)))))


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


(deftest missing-uint8-data
  ;;Use a large enough value the the system is forced to use uint8 columns else
  ;;it will default to int8 columns based on the column data min/max
  (let [ds (ds/->dataset {:a (dtype/make-container :uint8 [10 20 245])})
        ds (ds/update-column ds :a #(ds-col/set-missing % [1 5]))]
    (try
      (parquet/ds->parquet ds "test.parquet")
      (let [nds (ds/->dataset "test.parquet" {:key-fn keyword})]
        (is (= 3 (ds/row-count nds)))
        (is (= [1] (vec (dtype/->reader (ds/missing ds)))))
        (is (= :uint8 (dtype/elemwise-datatype (ds :a))))
        (is (= :uint8 (dtype/elemwise-datatype (nds :a))))
        (is (= [1] (vec (dtype/->reader (ds/missing nds))))))
      (finally
        (.delete (java.io.File. "test.parquet"))))))


(deftest nested-parquet
  (let [ds (ds/->dataset "test/data/nested.parquet")]
    (is (= [1 nil 2 nil 3 nil nil] (vec (ds "id"))))
    (is (= ["a" "b" "a" "b" "a" "b" "c"] (vec (ds "val.key_value.key"))))
    (is (= ["va" "vb" nil nil "vb" nil nil] (vec (ds "val2.key_value.key"))))))


(deftest local-time
  (try
    (let [ds (ds/->dataset {:a (range 10)
                            :b (repeat 10 (java.time.LocalTime/now))})
          _ (parquet/ds->parquet ds "test.parquet")
          pds (ds/->dataset "test.parquet"  {:key-fn keyword})]
      (is (= (vec (ds :b))
             (vec (pds :b)))))
    (finally
      (.delete (java.io.File. "test.parquet")))))


(deftest decimaltable
  (let [table (ds/->dataset "test/data/decimaltable.parquet")
        decimals (table "decimals")]
    (is (dfn/equals [3.420 1.246] decimals))))
