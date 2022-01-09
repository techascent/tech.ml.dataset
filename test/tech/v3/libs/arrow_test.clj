(ns tech.v3.libs.arrow-test
  (:require [tech.v3.libs.arrow :as arrow]
            [tech.v3.libs.arrow.in-place :as arrow-in-place]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.v3.libs.parquet]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.test :refer [deftest is]])
  (:import [java.time LocalTime]
           [tech.v3.dataset Text]))


(tech.v3.dataset.utils/set-slf4j-log-level :info)

(defn supported-datatype-ds
  []
  (-> (ds/->dataset {:boolean [true false true true false false true false false true]
                     :bytes (byte-array (range 10))
                     :ubytes (dtype/make-container :uint8 (range 10))
                     :shorts (short-array (range 10))
                     :ushorts (dtype/make-container :uint16 (range 10))
                     :ints (int-array (range 10))
                     :uints (dtype/make-container :uint32 (range 10))
                     :longs (long-array (range 10))
                     :floats (float-array (range 10))
                     :doubles (double-array (range 10))
                     :strings (map str (range 10))
                     :text (map (comp #(Text. %) str) (range 10))
                     :instants (repeatedly 10 dtype-dt/instant)
                     ;;sql doesn't support dash-case
                     :local_dates (repeatedly 10 dtype-dt/local-date)
                     ;;some sql engines (or the jdbc api) don't support more than second
                     ;;resolution for sql time objects
                     :local_times (repeatedly 10 dtype-dt/local-time)})
      (vary-meta assoc
                 :primary-key :longs
                 :name :testtable)))


(deftest base-datatype-test
  (try
    (let [ds (supported-datatype-ds)
          _ (arrow-in-place/write-dataset! ds "alldtypes.arrow")])
    (finally
      (.delete (java.io.File. "alldtypes.arrow")))))



(deftest simple-stocks
  (try
    (let [stocks (ds/->dataset "test/data/stocks.csv")
          _ (arrow/write-dataset-to-stream! stocks "temp.stocks.arrow")
          stocks-copying (arrow/read-stream-dataset-copying "temp.stocks.arrow")
          stocks-inplace (arrow/read-stream-dataset-inplace "temp.stocks.arrow")
          pystocks-copying (arrow/read-stream-dataset-copying "test/data/stocks.pyarrow.stream")
          pystocks-inplace (arrow/read-stream-dataset-inplace "test/data/stocks.pyarrow.stream")]
      ;;This is here just to make sure that the data isn't cleaned up until it actually can safely
      ;;be cleaned up.  This was a bug that caused datatype to bump from 5.11 to 5.12
      (System/gc)
      (is (dfn/equals (stocks "price") (stocks-copying "price")))
      (is (dfn/equals (stocks "price") (stocks-inplace "price")))
      (is (dfn/equals (stocks "price") (pystocks-copying "price")))
      (is (dfn/equals (stocks "price") (pystocks-inplace "price")))

      (is (= (vec (stocks "symbol")) (vec (stocks-copying "symbol"))))
      (is (= (vec (stocks "symbol")) (vec (stocks-inplace "symbol"))))
      (is (= (vec (stocks "symbol")) (mapv str (pystocks-copying "symbol"))))
      (is (= (vec (stocks "symbol")) (mapv str (pystocks-inplace "symbol")))))
    (finally
      (.delete (java.io.File. "temp.stocks.arrow")))))


(deftest ames-house-prices
  (try
    (let [ames (ds/->dataset "test/data/ames-house-prices/train.csv")
          _ (arrow/write-dataset-to-stream! ames "temp.ames.arrow")
          ames-copying (arrow/read-stream-dataset-copying "temp.ames.arrow")
          ames-inplace (arrow/read-stream-dataset-inplace "temp.ames.arrow")
          pyames-copying (arrow/read-stream-dataset-copying
                          "test/data/ames.pyarrow.stream")
          pyames-inplace (arrow/read-stream-dataset-inplace
                          "test/data/ames.pyarrow.stream")]
      (System/gc)
      (is (dfn/equals (ames "SalePrice") (ames-copying "SalePrice")))
      (is (dfn/equals (ames "SalePrice") (ames-inplace "SalePrice")))
      (is (= (ds-col/missing (ames "LotFrontage"))
             (ds-col/missing (ames-copying "LotFrontage"))))
      (is (= (ds-col/missing (ames "LotFrontage"))
             (ds-col/missing (ames-inplace "LotFrontage"))))
      (is (not= 0 (dtype/ecount (ds-col/missing (ames-inplace "LotFrontage")))))
      (is (dfn/equals (ames "SalePrice") (pyames-copying "SalePrice")))
      (is (dfn/equals (ames "SalePrice") (pyames-inplace "SalePrice")))
      (is (= (ds-col/missing (ames "LotFrontage"))
             (ds-col/missing (pyames-copying "LotFrontage"))))
      (is (= (ds-col/missing (ames "LotFrontage"))
             (ds-col/missing (pyames-inplace "LotFrontage")))))
    (finally
      (.delete (java.io.File. "temp.ames.arrow")))))


;; > read_ipc_stream ("with_date.arrow")
;; member-id day                         trx-id brand-id month year quantity
;; 1     86422  23 564132249-257605208-1718971337      202     6 2019        1
;; 2     12597  25   897161990-1972492812-1691041      134     6 2019        2
;; 3    126980  16  31433047-823825990-2105753041       11     6 2019        2
;; price style-id       date
;; 1  65536      171 2019-06-23
;; 2 131072       38 2019-06-25
;; 3 131072       33 2019-06-16

(deftest date-arrow-test
  (let [date-data (arrow/read-stream-dataset-copying "test/data/with_date.arrow")]
    (is (= [18070 18072 18063]
           (date-data "date")))
    (is (= :epoch-days (dtype/elemwise-datatype (date-data "date")))))
  (let [date-data (arrow/read-stream-dataset-copying "test/data/with_date.arrow"
                                                     {:epoch->datetime? true})]
    (is (= (mapv #(java.time.LocalDate/parse %)
                 ["2019-06-23" "2019-06-25" "2019-06-16"])
           (date-data "date")))
    (is (= :local-date (dtype/elemwise-datatype (date-data "date")))))
  (let [date-data (arrow/read-stream-dataset-inplace "test/data/with_date.arrow"
                                                     {:epoch->datetime? true})]
    (is (= (mapv #(java.time.LocalDate/parse %)
                 ["2019-06-23" "2019-06-25" "2019-06-16"])
           (date-data "date")))
    (is (= :local-date (dtype/elemwise-datatype (date-data "date"))))))


(deftest odd-parquet-crash
  (let [test-data (ds/->dataset "test/data/part-00000-74d3eb51-bc9c-4ba5-9d13-9e0d71eea31f.c000.snappy.parquet")]
    (try
      (arrow/write-dataset-to-stream! test-data "test.arrow")
      (let [arrow-ds (arrow/read-stream-dataset-copying "test.arrow")]
        (is (= (ds/missing test-data)
               (ds/missing arrow-ds))))
      (finally
        (.delete (java.io.File. "test.arrow"))))))


(deftest failed-R-file
  (let [cp-data (arrow/read-stream-dataset-copying "test/data/part-8981.ipc_stream")
        inp-data (arrow/read-stream-dataset-inplace "test/data/part-8981.ipc_stream")]
    (is (= (vec (ds/column-names cp-data))
           (vec (ds/column-names inp-data))))))


(deftest large-var-char-file
  (let [cp-data (arrow/read-stream-dataset-copying "test/data/largeVarChar.ipc")
        inp-data (arrow/read-stream-dataset-inplace "test/data/largeVarChar.ipc")]
    (is (= (vec (ds/column-names cp-data))
           (vec (ds/column-names inp-data))))
    (is (= (vec (first (ds/columns cp-data)))
           (vec (first (ds/columns inp-data)))))))


(deftest uuid-test
  (try
    (let [uuid-ds (ds/->dataset "test/data/uuid.parquet"
                                {:parser-fn {"uuids" :uuid}})
          _ (arrow/write-dataset-to-stream! uuid-ds "test-uuid.arrow")
          copying-ds (arrow/read-stream-dataset-copying "test-uuid.arrow")
          inplace-ds (arrow/read-stream-dataset-inplace "test-uuid.arrow")
          ]
      (is (= :text ((comp :datatype meta) (copying-ds "uuids"))))
      (is (= :text ((comp :datatype meta) (inplace-ds "uuids"))))
      (is (= (vec (copying-ds "uuids"))
             (vec (inplace-ds "uuids"))))
      (is (= (mapv str (uuid-ds "uuids"))
             (mapv str (copying-ds "uuids")))))
    (finally
      (.delete (java.io.File. "test-uuid.arrow")))))


(deftest local-time
  (try
    (let [ds (ds/->dataset {"a" (range 10)
                            "b" (repeat 10 (java.time.LocalTime/now))})
          _ (arrow/write-dataset-to-stream! ds "test-local-time.arrow")
          copying-ds (arrow/read-stream-dataset-copying "test-local-time.arrow")
          inplace-ds (arrow/read-stream-dataset-inplace "test-local-time.arrow")]
      (is (= :time-microseconds (dtype/elemwise-datatype (copying-ds "b"))))
      (is (= :time-microseconds (dtype/elemwise-datatype (inplace-ds "b"))))
      (is (= (vec (copying-ds "b"))
             (vec (inplace-ds "b"))))
      ;;Making a primitive container will use the packed data.
      (is (= (vec (dtype/make-container :int64 (ds "b")))
             (vec (copying-ds "b")))))
    (finally
      (.delete (java.io.File. "test-local-time.arrow")))))
