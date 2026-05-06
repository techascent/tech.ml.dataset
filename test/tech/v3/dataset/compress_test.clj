(ns tech.v3.dataset.compress-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.datetime.operations :as dt-dt-ops]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.compress :as compress]
            [tech.v3.dataset.protocols :as ds-proto]))


(deftest basic-compression
  (let [ds (ds/->dataset {:a (-> (vec (range 100))
                                 (assoc 1 nil 30 nil))
                          :b (-> (vec (repeat 100 :a))
                                 (assoc 3 nil 98 nil))
                          :c (take 100 (cycle [:a :b :c]))
                          :d (vec (dt-dt-ops/plus-temporal-amount
                                   (dt/make-container :packed-local-date (repeat 100 (java.time.LocalDate/now)))
                                   (range 100)
                                   :days))})
        cds (compress/compress-ds ds)]
    (is (every? compress/compressed-buffer? (map ds-proto/column-buffer (vals cds))))
    (is (= ds cds))))
