(ns tech.ml.dataset.datetime-test
  (:require [tech.ml.dataset :as ds]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.datetime :as dtype-dt]
            [clojure.test :refer [deftest is]]))


(deftest epoch-millis-second-maps
  (let [ds (-> (ds/->dataset "test/data/stocks.csv")
               (ds/update-column "date" dtype-dt-ops/get-epoch-milliseconds)
               (ds/mapseq-reader))]
    (is (number? (get (first ds) "date"))))
  (let [ds (-> (ds/->dataset "test/data/stocks.csv")
               (ds/update-column "date" dtype-dt-ops/get-epoch-seconds)
               (ds/mapseq-reader))]
    (is (number? (get (first ds) "date")))))


(deftest datetime-column-datatype-test
  (let [ds (ds/->dataset "test/data/stocks.csv")]
    (is (= :packed-local-date
           (-> (ds "date")
               (dtype/->reader)
               (dtype/sub-buffer 0 20)
               (dtype/get-datatype))))))


(deftest stocks-descriptive-stats
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        desc-stats (ds/descriptive-stats stocks)
        date-only (-> (ds/filter-column #(= "date" %) :col-name desc-stats)
                      (ds/mapseq-reader)
                      (first))]
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     (vals (select-keys date-only [:min :mean :max])))))))


(deftest stocks-descriptive-stats-2
  (let [stocks (-> (ds/->dataset "test/data/stocks.csv")
                   (ds/update-column "date" #(dtype/object-reader
                                              (dtype/ecount %)
                                              (fn [idx]
                                                (-> (% idx)
                                                    (dtype-dt/unpack-local-date)
                                                    (dtype-dt/local-date->instant)))
                                              :instant)))
        desc-stats (ds/descriptive-stats stocks)
        date-only (-> (ds/filter-column #(= "date" %) :col-name desc-stats)
                      (ds/mapseq-reader)
                      (first))]
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     (vals (select-keys date-only [:min :mean :max])))))))
