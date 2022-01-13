(ns tech.v3.libs.arrow-test
  (:require [tech.v3.libs.arrow :as arrow]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.v3.libs.parquet]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.resource :as resource]
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
                     ;;external formats often don't support dash-case
                     :local_dates (repeatedly 10 dtype-dt/local-date)
                     :local_times (repeatedly 10 dtype-dt/local-time)})
      (vary-meta assoc
                 :primary-key :longs
                 :name :testtable)))


(deftest base-datatype-test
  (try
    (resource/stack-resource-context
     (let [ds (supported-datatype-ds)
           _ (arrow/dataset->stream! ds "alldtypes.arrow")
           mmap-ds (arrow/stream->dataset "alldtypes.arrow" {:open-type :mmap
                                                             :key-fn keyword})
           copy-ds (arrow/stream->dataset "alldtypes.arrow" {:key-fn keyword})]
       (doseq [col (vals ds)]
         (let [cname ((meta col) :name)
               dt (dtype/elemwise-datatype col)
               inp-col (mmap-ds cname)
               cp-col (copy-ds cname)]
           (is (= dt (dtype/elemwise-datatype inp-col)) (str "inplace failure " cname))
           (is (= dt (dtype/elemwise-datatype cp-col)) (str "copy failure " cname))
           (is (= (vec col) (vec inp-col)) (str "inplace failure " cname))
           (is (= (vec col) (vec cp-col)) (str "copy failure " cname))))))
    (finally
      (.delete (java.io.File. "alldtypes.arrow")))))


(deftest base-ds-seq-test
  (try
    (let [ds (supported-datatype-ds)
          _ (arrow/dataset-seq->stream! "alldtypes-seq.arrow" [ds ds ds])
          mmap-ds-seq (arrow/stream->dataset-seq "alldtypes-seq.arrow" {:key-fn keyword
                                                                        :open-type :mmap})
          copy-ds-seq (arrow/stream->dataset-seq "alldtypes-seq.arrow" {:key-fn keyword})]
      (is (= 3 (count mmap-ds-seq)))
      (is (= 3 (count copy-ds-seq)))
      (let [mmap-ds (last mmap-ds-seq)
            copy-ds (last copy-ds-seq)]
        (doseq [col (vals ds)]
         (let [cname ((meta col) :name)
               dt (dtype/elemwise-datatype col)
               inp-col (mmap-ds cname)
               cp-col (copy-ds cname)]
           (is (= dt (dtype/elemwise-datatype inp-col)) (str "inplace failure " cname))
           (is (= dt (dtype/elemwise-datatype cp-col)) (str "copy failure " cname))
           (is (= (vec col) (vec inp-col)) (str "inplace failure " cname))
           (is (= (vec col) (vec cp-col)) (str "copy failure " cname))))))
    (finally
      (.delete (java.io.File. "alldtypes-seq.arrow")))))


(deftest simple-stocks
    (try
      (let [stocks (ds/->dataset "test/data/stocks.csv")
            _ (arrow/dataset->stream! stocks "temp.stocks.arrow")
            stocks-copying (arrow/stream->dataset "temp.stocks.arrow")
            stocks-inplace (arrow/stream->dataset "temp.stocks.arrow" {:open-type :mmap})
            pystocks-copying (arrow/stream->dataset "test/data/stocks.pyarrow.stream")
            pystocks-inplace (arrow/stream->dataset "test/data/stocks.pyarrow.stream")]
        ;;This is here just to make sure that the data isn't cleaned up until it
        ;;actually can safely be cleaned up.  This was a bug that caused datatype to
        ;;bump from 5.11 to 5.12
        (System/gc)
        (is (dfn/equals (stocks "price") (stocks-copying "price")))
        (is (dfn/equals (stocks "price") (stocks-inplace "price")))
        (is (dfn/equals (stocks "price") (pystocks-copying "price")))
        (is (dfn/equals (stocks "price") (pystocks-inplace "price")))

        (is (= (vec (stocks "symbol")) (vec (stocks-copying "symbol"))))
        (is (= (vec (stocks "symbol")) (vec (stocks-inplace "symbol"))))
        ;;python saves strings inline in the file - equivalent to :strings-as-text?
        ;;save option
        (is (= (vec (stocks "symbol")) (mapv str (pystocks-copying "symbol"))))
        (is (= (vec (stocks "symbol")) (mapv str (pystocks-inplace "symbol")))))
      (finally
        (.delete (java.io.File. "temp.stocks.arrow")))))


(deftest ames-house-prices
    (try
      (let [ames (ds/->dataset "test/data/ames-house-prices/train.csv")
            _ (arrow/dataset->stream! ames "temp.ames.arrow")
            ames-copying (arrow/stream->dataset "temp.ames.arrow")
            ames-inplace (arrow/stream->dataset "temp.ames.arrow" {:open-type :mmap})
            pyames-copying (arrow/stream->dataset "test/data/ames.pyarrow.stream")
            pyames-inplace (arrow/stream->dataset "test/data/ames.pyarrow.stream")]
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


(deftest date-arrow-test
  (let [date-data (arrow/read-stream-dataset-copying "test/data/with_date.arrow"
                                                     {:integer-datetime-types? true})]
    (is (= [18070 18072 18063]
           (date-data "date")))
    (is (= :epoch-days (dtype/elemwise-datatype (date-data "date")))))
  (let [date-data (arrow/read-stream-dataset-copying "test/data/with_date.arrow")]
    (is (= (mapv #(java.time.LocalDate/parse %)
                 ["2019-06-23" "2019-06-25" "2019-06-16"])
           (date-data "date")))
    (is (= :packed-local-date (dtype/elemwise-datatype (date-data "date"))))))

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
          inplace-ds (arrow/read-stream-dataset-inplace "test-uuid.arrow")]
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
        (is (= :packed-local-time (dtype/elemwise-datatype (copying-ds "b"))))
        (is (= :packed-local-time (dtype/elemwise-datatype (inplace-ds "b"))))
        (is (= (vec (copying-ds "b"))
               (vec (inplace-ds "b"))))
        ;;Making a primitive container will use the packed data.
        (is (= (vec (ds "b"))
               (vec (copying-ds "b")))))
      (finally
        (.delete (java.io.File. "test-local-time.arrow")))))
