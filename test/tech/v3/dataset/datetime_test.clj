(ns tech.v3.dataset.datetime-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.test :refer [deftest is]]))


(deftest epoch-millis-second-maps
  (let [ds (-> (ds/->dataset "test/data/stocks.csv")
               (ds/update-column "date" dtype-dt/datetime->milliseconds)
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
        date-only (-> (ds/filter-column desc-stats :col-name #(= "date" %))
                      (ds/mapseq-reader)
                      (first))]
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     (vals (select-keys date-only [:min :mean :max])))))))


(deftest stocks-descriptive-stats-2
  (let [stocks (-> (ds/->dataset "test/data/stocks.csv")
                   (ds/update-column "date" (partial dtype/emap
                                                     dtype-dt/local-date->instant
                                                     :instant)))
        desc-stats (ds/descriptive-stats stocks {:stat-names (ds/all-descriptive-stats-names)})
        date-only (-> (ds/filter-column desc-stats :col-name #(= "date" %))
                      (ds/mapseq-reader)
                      (first))]
    (is (every? dtype-dt/datetime-datatype?
                (map dtype/get-datatype
                     (vals (select-keys date-only [:min :mean :max
                                                   :quartile-1 :quartile-3])))))))


(deftest datetime-shenanigans-1
  (is (= (java.time.LocalDateTime/of 2020 01 01 11 22 33)
         (nth (ds/column
               (ds/->dataset {:dt [(java.time.LocalDateTime/of 2020 01 01 11 22 33)
                                   (java.time.LocalDateTime/of 2020 10 01 01 01 01)]})
               :dt) 0)))

  (is (= (java.time.LocalDateTime/of 2020 01 01 11 22 33)
         (dtype/get-value
          (ds/column
           (ds/->dataset {:dt [(java.time.LocalDateTime/of 2020 01 01 11 22 33)
                               (java.time.LocalDateTime/of 2020 10 01 01 01 01)]})
           :dt) 0))))
