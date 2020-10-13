(ns tech.v3.libs.poi-test
  (:require [tech.v3.libs.poi :as xlsx-parse]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [clojure.test :refer [deftest is]]))


(def xls-file "test/data/file_example_XLS_1000.xls")
(def xlsx-file "test/data/file_example_XLSX_1000.xlsx")
(def sparse-file "test/data/sparsefile.xlsx")
(def stocks-file "test/data/stocks.xlsx")


(deftest happy-path-parse-test
  (let [ds (first (xlsx-parse/workbook->datasets xlsx-file))
        ds2 (first (xlsx-parse/workbook->datasets xlsx-file))]
    (is (= #{"column-0" "Age" "Country" "First Name" "Gender" "Date" "Last Name" "Id"}
           (set (ds/column-names ds))))
    (is (= #{"column-0" "Age" "Country" "First Name" "Gender" "Date" "Last Name" "Id"}
           (set (ds/column-names ds2))))
    (is (= #{:float64 :string}
           (set (map dtype/get-datatype (ds/columns ds)))))
    (is (= 1000 (ds/row-count ds)))
    (is (= 1000 (ds/row-count ds2)))
    (is (= 8 (ds/column-count ds)))
    (is (= 8 (ds/column-count ds2)))
    (is (dfn/equals (ds "Age") (ds2 "Age")))
    (is (dfn/equals (ds "Id") (ds2 "Id")))))


(deftest sparse-file-parse-test
  (let [ds (first (xlsx-parse/workbook->datasets sparse-file))]
    (is (= 8 (ds/row-count ds)))
    (is (= 8 (ds/column-count ds)))
    (is (every? #(= (set (range 8)) %)
                (map (comp set ds-col/missing ds) ["column-0" "a" "column-6"])))
    (is (= [1.0 1.0 1.0 "a" 2.0 23.0]
           (->> (ds/columns ds)
                (mapcat (comp dtype/->reader ds/drop-missing))
                vec)))))


(deftest datetime-test
  (let [ds (first (xlsx-parse/workbook->datasets
                   stocks-file
                   {:parser-fn {"date" :packed-local-date}}))]
    (is (= :packed-local-date (dtype/get-datatype (ds "date"))))))


(deftest custom-parser-test
  (let [ds (first (xlsx-parse/workbook->datasets
                   xls-file
                   {:parser-fn {"Date" [:local-date
                                        "dd/MM/yyyy"]}}))]
    (is (= :local-date (dtype/get-datatype (ds "Date"))))))


(deftest integer-field-test
  (let [ds (first (xlsx-parse/workbook->datasets
                   xls-file
                   {:parser-fn {"Id" :int64}}))]
    (is (= :int64 (dtype/get-datatype (ds "Id"))))))


(deftest xls-keyword-colnames
  (let [ds (first (xlsx-parse/workbook->datasets
                   xls-file
                   {:key-fn keyword}))]
    ;;The first column is an integer so keyword returns nil for that.
    ;;This is also a good example in that the system produces keywords with spaces
    ;;in them...that definitely isn't ideal.
    (is (every? keyword? (rest (ds/column-names ds))))))


(deftest key-fn-number-columns
  (let [ds (ds/->dataset xlsx-file {:key-fn keyword})]
    (is (= 0 (count (filter nil? (ds/column-names ds)))))
    (is (= #{:column-0 :Age :Country (keyword "First Name") :Gender :Date
             (keyword "Last Name") (keyword "Id")}
           (set (ds/column-names ds))))))
