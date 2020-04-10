(ns tech.ml.dataset.datetime-test
  (:require [tech.ml.dataset :as ds]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
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
