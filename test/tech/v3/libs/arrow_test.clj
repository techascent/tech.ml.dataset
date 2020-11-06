(ns tech.v3.libs.arrow-test
  (:require [tech.v3.libs.arrow :as arrow]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.test :refer [deftest is]]))


(tech.v3.dataset.utils/set-slf4j-log-level :info)

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



(comment

  (deftest big-arrow-text
    (let [big-data (arrow/read-stream-dataset-inplace "10m.arrow")]
      (is (= "A character vector containing abbreviations for the character strings in its first argument. Duplicates in the original names.arg will be given identical abbreviations. If any non-duplicated elements have the same minlength abbreviations then, if method = both.sides the basic internal abbreviate() algorithm is applied to the characterwise reversed strings; if there are still duplicated abbreviations and if strict = FALSE as by default, minlength is incremented by one and new abbreviations are found for those elements only. This process is repeated until all unique elements of names.arg have unique abbreviations."
             (first (big-data "texts"))))))

  (def lots-of-infos
    (vec (repeatedly 200 #(mapv meta (vals (arrow/read-stream-dataset-inplace "10m.arrow"))))))

  (dotimes [iter 10]
    (arrow/read-stream-dataset-copying "/home/chrisn/Downloads/screenings.arrow")
    )

  (defn count-rows-arrow
    []
    (apply +
     (->>
      (repeat 2000 "/home/chrisn/Downloads/screenings.arrow")
      (mapv #(ds/row-count (arrow/read-stream-dataset-inplace %))))))
  )
