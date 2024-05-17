(ns tech.v3.libs.fastexcel-test
  (:require [tech.v3.libs.fastexcel :as xlsx-parse]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [clojure.test :refer [deftest is testing]]))

(def xls-file "test/data/file_example_XLS_1000.xls")
(def xlsx-file "test/data/file_example_XLSX_1000.xlsx")
(def sparse-file "test/data/sparsefile.xlsx")
(def stocks-file "test/data/stocks.xlsx")
(def stocks-bad-date-file "test/data/stocks-bad-date.xlsx")
(def duplicate-headers-file "test/data/duplicate-headers.xlsx")



(deftest happy-path-parse-test
  (let [ds (first (xlsx-parse/workbook->datasets xlsx-file))]
    (is (= #{"column-0" "Age" "Country" "First Name" "Gender" "Date" "Last Name" "Id"}
           (set (ds/column-names ds))))
    (is (= #{:float64 :string}
           (set (map dtype/get-datatype (ds/columns ds)))))
    (is (= 1000 (ds/row-count ds)))
    (is (= 8 (ds/column-count ds)))))



(deftest sparse-file-parse-test
  (let [ds (first (xlsx-parse/workbook->datasets sparse-file))]
    (is (= 8 (ds/row-count ds)))
    (is (= 8 (ds/column-count ds)))
    (is (every? #(= (set (range 8)) %)
                (map (comp set ds/missing ds) ["column-0" "a" "column-6"])))
    (is (= [1.0 1.0 1.0 "a" 2.0 23.0]
           (->> (ds/columns ds)
                (mapcat (comp dtype/->reader ds/drop-missing))
                vec)))))

(deftest datetime-test
  (let [ds (first (xlsx-parse/workbook->datasets
                   stocks-file
                   {:parser-fn {"date" :packed-local-date}}))]
    (is (= :packed-local-date (dtype/get-datatype (ds "date"))))))


(deftest bad-datetime-test
  (let [ds (first (xlsx-parse/workbook->datasets stocks-bad-date-file))]
    (is (= :string (dtype/get-datatype (ds "date"))))
    (is (= {java.lang.String 29}
           (->> (ds "date")
                (map type)
                frequencies)))))


(deftest skip-rows-test
  (let [ds (ds/->dataset "test/data/holdings-daily-us-en-mdy.xlsx"
                         {:n-initial-skip-rows 4
                          :parser-fn {"Identifier" :string
                                      "Weight" :float64}})]
    ;;column-8 had no data
    (is (= #{:float64 :string :boolean}
           (set (map dtype/get-datatype (vals ds)))))
    (is (= ["Name"
            "Ticker"
            "Identifier"
            "SEDOL"
            "Weight"
            "Sector"
            "Shares Held"
            "Local Currency"
            "column-8"]
           (vec (ds/column-names ds))))))

(deftest ensure-unique-headers-test
  (testing "that all headers are are forced to be unique"
    (let [ds (ds/->dataset duplicate-headers-file
                           {:ensure-unique-column-names? true})]
      (is (ds/column-count ds) 7)
      (is (count (set (ds/column-names ds))) 7))
    (let [ds (first (xlsx-parse/workbook->datasets duplicate-headers-file
                                                   {:ensure-unique-column-names? true}))]
      (is (ds/column-count ds) 7)
      (is (count (set (ds/column-names ds))) 7)))

  (testing "that exception is thrown on duplicate headers"
    (is (thrown? RuntimeException (ds/->dataset duplicate-headers-file)))
    (is (thrown? RuntimeException (xlsx-parse/workbook->datasets duplicate-headers-file))))

  (testing "that custom postfix-fn works correctly"
    (let [ds (ds/->dataset duplicate-headers-file
                           {:ensure-unique-column-names? true
                            :unique-column-name-fn (fn [col-idx colname] (str colname "::" col-idx))})]
      (is (some? (ds/column ds "column::2")))
      (is (some? (ds/column ds "column::4")))
      (is (some? (ds/column ds "column-1::6"))))))


(deftest number-colname
  (let [ds (ds/->dataset "test/data/number_column.xlsx")]
    (is (= (first (ds/column-names ds)) 0.0))))
