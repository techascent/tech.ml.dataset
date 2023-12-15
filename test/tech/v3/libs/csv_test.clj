(ns tech.v3.libs.csv-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.io.csv :as csv-parse]))

(def duplicate-headers-file "test/data/duplicate-headers.csv")

(deftest ensure-unique-headers-test
  (testing "that all headers are are forced to be unique"
    (let [ds (ds/->dataset duplicate-headers-file
                           {:ensure-unique-column-names? true})]
      (is (ds/column-count ds) 7)
      (is (count (set (ds/column-names ds))) 7))
    (let [ds (csv-parse/csv->dataset duplicate-headers-file
                                     {:ensure-unique-column-names? true})]
      (is (ds/column-count ds) 7)
      (is (count (set (ds/column-names ds))) 7)))

  (testing "that exception is thrown on duplicate headers"
    (is (thrown? RuntimeException (ds/->dataset duplicate-headers-file))))

  (testing "that custom postfix-fn works correctly"
    (let [ds (ds/->dataset duplicate-headers-file
                           {:ensure-unique-column-names? true
                            :unique-column-name-fn (fn [col-idx colname] (str colname "::" col-idx))})]
      (is (some? (ds/column ds "column::2")))
      (is (some? (ds/column ds "column::4")))
      (is (some? (ds/column ds "column-1::6"))))))
