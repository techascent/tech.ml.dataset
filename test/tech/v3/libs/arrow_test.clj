(ns tech.v3.libs.arrow-test
  (:require [tech.v3.libs.arrow :as arrow]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.resource :as resource]
            [clojure.test :refer [deftest is]]))


(tech.v3.dataset.utils/set-slf4j-log-level :info)

(deftest simple-stocks
  (try
    ;;!!CRUCIAL!!!  inplace *requires* a stack resource context and the dataset
    ;;is invalid outside the resource context.  If you want a valid dataset outside
    ;;this context, use dtype/clone
    (resource/stack-resource-context
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

       (is (= (vec (stocks "symbol")) (vec (stocks-copying "symbol")) ))
       (is (= (vec (stocks "symbol")) (vec (stocks-inplace "symbol"))))
       (is (= (vec (stocks "symbol")) (vec (pystocks-copying "symbol"))))
       (is (= (vec (stocks "symbol")) (vec (pystocks-inplace "symbol"))))))
    (finally
      (.delete (java.io.File. "temp.stocks.arrow")))))


(deftest ames-house-prices
  (try
    (resource/stack-resource-context
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
              (ds-col/missing (pyames-inplace "LotFrontage"))))))
    (finally
      (.delete (java.io.File. "temp.ames.arrow")))))
