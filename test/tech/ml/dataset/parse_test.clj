(ns tech.ml.dataset.parse-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.base :as ds-base]
            [tech.ml.dataset.column :as ds-col]
            [clojure.set :as set])
  (:import  [com.univocity.parsers.csv CsvFormat CsvParserSettings CsvParser]))


(def test-file "data/ames-house-prices/train.csv")


(def missing-data
  (->> [{:column-name "LotFrontage", :missing-count 259}
        {:column-name "Alley", :missing-count 1369}
        {:column-name "MasVnrType", :missing-count 8}
        {:column-name "MasVnrArea", :missing-count 8}
        {:column-name "BsmtQual", :missing-count 37}
        {:column-name "BsmtCond", :missing-count 37}
        {:column-name "BsmtExposure", :missing-count 38}
        {:column-name "BsmtFinType1", :missing-count 37}
        {:column-name "BsmtFinType2", :missing-count 38}
        {:column-name "Electrical", :missing-count 1}
        {:column-name "FireplaceQu", :missing-count 690}
        {:column-name "GarageType", :missing-count 81}
        {:column-name "GarageYrBlt", :missing-count 81}
        {:column-name "GarageFinish", :missing-count 81}
        {:column-name "GarageQual", :missing-count 81}
        {:column-name "GarageCond", :missing-count 81}
        {:column-name "PoolQC", :missing-count 1453}
        {:column-name "Fence", :missing-count 1179}
        {:column-name "MiscFeature", :missing-count 1406}
        ]
       (map (juxt :column-name :missing-count))
       (sort-by first)))

(def datatype-answers
  [["1stFlrSF" :int16]
   ["2ndFlrSF" :int16]
   ["3SsnPorch" :int16]
   ["Alley" :string]
   ["BedroomAbvGr" :int16]
   ["BldgType" :string]
   ["BsmtCond" :string]
   ["BsmtExposure" :string]
   ["BsmtFinSF1" :int16]
   ["BsmtFinSF2" :int16]
   ["BsmtFinType1" :string]
   ["BsmtFinType2" :string]
   ["BsmtFullBath" :int16]
   ["BsmtHalfBath" :int16]
   ["BsmtQual" :string]
   ["BsmtUnfSF" :int16]
   ["CentralAir" :boolean]
   ["Condition1" :string]
   ["Condition2" :string]
   ["Electrical" :string]
   ["EnclosedPorch" :int16]
   ["ExterCond" :string]
   ["ExterQual" :string]
   ["Exterior1st" :string]
   ["Exterior2nd" :string]
   ["Fence" :string]
   ["FireplaceQu" :string]
   ["Fireplaces" :int16]
   ["Foundation" :string]
   ["FullBath" :int16]
   ["Functional" :string]
   ["GarageArea" :int16]
   ["GarageCars" :int16]
   ["GarageCond" :string]
   ["GarageFinish" :string]
   ["GarageQual" :string]
   ["GarageType" :string]
   ["GarageYrBlt" :int16]
   ["GrLivArea" :int16]
   ["HalfBath" :int16]
   ["Heating" :string]
   ["HeatingQC" :string]
   ["HouseStyle" :string]
   ["Id" :int16]
   ["KitchenAbvGr" :int16]
   ["KitchenQual" :string]
   ["LandContour" :string]
   ["LandSlope" :string]
   ["LotArea" :int32]
   ["LotConfig" :string]
   ["LotFrontage" :int16]
   ["LotShape" :string]
   ["LowQualFinSF" :int16]
   ["MSSubClass" :int16]
   ["MSZoning" :string]
   ["MasVnrArea" :int16]
   ["MasVnrType" :string]
   ["MiscFeature" :string]
   ["MiscVal" :int16]
   ["MoSold" :int16]
   ["Neighborhood" :string]
   ["OpenPorchSF" :int16]
   ["OverallCond" :int16]
   ["OverallQual" :int16]
   ["PavedDrive" :string]
   ["PoolArea" :int16]
   ["PoolQC" :string]
   ["RoofMatl" :string]
   ["RoofStyle" :string]
   ["SaleCondition" :string]
   ["SalePrice" :int32]
   ["SaleType" :string]
   ["ScreenPorch" :int16]
   ["Street" :string]
   ["TotRmsAbvGrd" :int16]
   ["TotalBsmtSF" :int16]
   ["Utilities" :string]
   ["WoodDeckSF" :int16]
   ["YearBuilt" :int16]
   ["YearRemodAdd" :int16]
   ["YrSold" :int16]])


(deftest base-ames-parser-test
  (let [result (ds-parse/csv->dataset test-file)
        dtypes (->> result
                    (map meta)
                    (sort-by :name)
                    (mapv (juxt :name :datatype)))]
    (is (= (set (map first datatype-answers))
           (set (map first dtypes))))

    (let [dtype-map (into {} dtypes)
          differences (->> datatype-answers
                           (map (fn [[colname col-dtype]]
                                  (let [detected-dtype (dtype-map colname)]
                                    (when-not (= detected-dtype col-dtype)
                                      {:name colname
                                       :expected-datatype col-dtype
                                       :result-datatype detected-dtype}))))
                           (remove nil?)
                           seq)]

      (is (nil? differences)
          (str differences)))
    (let [result-missing-data (->> result
                                   (map (juxt ds-col/column-name
                                              (comp dtype/ecount ds-col/missing)))
                                   (remove #(= 0 (second %)))
                                   (sort-by first))]
      (is (= (set (map first missing-data))
             (set (map first result-missing-data))))))

  (let [result (ds-parse/csv->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["Id" "SalePrice" "YearBuilt"]})]
    (is (= 3 (count result)))
    ;;Header row accounts for one.
    (is (= 100 (ds-base/row-count result))))
  (let [result (ds-parse/csv->dataset test-file {:n-records 100
                                                 :column-blacklist (range 70)})]
    (is (= 11 (count result)))
    (is (= 100 (ds-base/row-count result)))))


(deftest base-ames-load-test
  ;;Here we just test that the options correctly pass through ->dataset
  (let [result (ds-base/->dataset test-file
                                  {:n-records 100
                                   :column-whitelist ["Id" "SalePrice" "YearBuilt"]})]
      (is (= 3 (ds-base/column-count result)))
      ;;Header row accounts for one.
      (is (= 100 (ds-base/row-count result))))


  (let [result (ds-base/->dataset test-file {:n-records 100
                                             :column-blacklist (range 70)})]
      (is (= 11 (ds-base/column-count result)))
      ;;Header row accounts for one.
      (is (= 100 (ds-base/row-count result)))))


(deftest specify-column-types
  ;;parse everything as float32
  (let [result (ds-base/->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]
                 :parser-fn :float32})]
    (is (= #{:float32}
           (set (map dtype/get-datatype result))))
    (is (= 3 (ds-base/column-count result))))

  ;;Next up is a map of colname->datatype
  (let [result (ds-base/->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]
                 :parser-fn {"1stFlrSF" :float32
                             "2ndFlrSF" :int32}})]
    (is (= #{:float32 :int32 :int16}
           (set (map dtype/get-datatype result)))))

  ;;Or you can implement a function from colname,first-n-strings->parser
  (let [parser-fn (fn [_colname _coldata-n-strings]
                    ;;Shortcut to create a full parser from a stateless simple parser
                    (ds-parse/simple-parser->parser :float32))
        result (ds-base/->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]
                 :parser-fn parser-fn})]
    (is (= #{:float32}
           (set (map dtype/get-datatype result))))))


(deftest semi-colon-delimited-file
  (let [result (ds-base/->dataset "test/data/sample01.csv"
                                  {:separator \;})]
    (is (= 3 (ds-base/column-count result)))))


(deftest tough-file
  (let [result (ds-base/->dataset "test/data/essential.csv"
                                  {:n-initial-skip-rows 1
                                   :skip-bad-rows? true})]
    (is (= 5 (ds-base/column-count result)))))


(defn- make-essential-csv-parser
  []
  (-> (doto (CsvParserSettings.)
        (.. getFormat (setLineSeparator "\n"))
        (.setHeaderExtractionEnabled true)
        (.setIgnoreLeadingWhitespaces true)
        (.setIgnoreTrailingWhitespaces true))
      (CsvParser.)))


(deftest custom-csv-parser
  (let [result (ds-base/->dataset "test/data/essential.csv"
                                  {:csv-parser (make-essential-csv-parser)
                                   :skip-bad-rows? true})]
    (is (= 5 (ds-base/column-count result)))))


(deftest simple-write-test
  (let [initial-ds (ds-base/->dataset
                    test-file
                    {:n-records 20
                     :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]})
        _ (ds-base/write-csv! initial-ds "test.tsv")
        new-ds (ds-base/->dataset "test.tsv")]
    (is (dfn/equals (initial-ds "1stFlrSF")
                    (new-ds "1stFlrSF")))
    (is (dfn/equals (initial-ds "2ndFlrSF")
                    (new-ds "2ndFlrSF"))))
  (let [missing-ds (-> (ds-base/->dataset
                        test-file
                        {:n-records 20
                         :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]})
                       (ds-base/update-column
                        "1stFlrSF"
                        #(ds-col/set-missing % [2 4 7 9])))
        _ (ds-base/write-csv! missing-ds "test.tsv")
        new-ds (ds-base/->dataset "test.tsv")]
    (is (dfn/equals (missing-ds "1stFlrSF")
                    (new-ds "1stFlrSF")))
    (is (= #{2 4 7 9}
           (set (ds-col/missing (new-ds "1stFlrSF")))))))


(deftest date-time-format-test-1
  (let [stock-ds (ds-base/->dataset "test/data/stocks.csv")]
    (is (= :packed-local-date (dtype/get-datatype (stock-ds "date")))))
  (let [temp-ds (ds-base/->dataset "test/data/seattle-temps.csv")]
    (is (= :zoned-date-time (dtype/get-datatype (temp-ds "date")))))
  (let [stock-ds (ds-base/->dataset "test/data/stocks.csv"
                                    {:parser-fn
                                     {"date" :local-date}})]
    (is (= :local-date (dtype/get-datatype (stock-ds "date"))))))


(defn verify-relaxed-parse
  [ds]
  (let [date-col (ds "date")
        col-meta (meta date-col)
        ^List unparsed-data (:unparsed-data col-meta)
        ^RoaringBitmap unparsed-indexes (:unparsed-indexes col-meta)]
    (is (= :packed-local-date (dtype/get-datatype date-col)))
    ;;Make sure unparsed data came through intact
    (is (= #{"hello" "1212"}
           (set unparsed-data)))))


(deftest bad-csv-relaxed-1
  (let [ds (ds-base/->dataset "test/data/stocks-bad-date.csv")]
    (is (= :string (dtype/get-datatype (ds "date"))))
    ;;Make sure unparsed data came through intact
    (is (= #{"hello" "1212"}
           (set/intersection #{"hello" "1212"}
                             (set (ds-col/unique (ds "date"))))))
    (let [updated-ds (ds-base/update-column
                      ds "date" (partial ds-col/parse-column
                                         [:packed-local-date :relaxed?]))]
      (verify-relaxed-parse updated-ds))))


(deftest bad-csv-relaxed-2
  (let [ds (ds-base/->dataset "test/data/stocks-bad-date.csv"
                              {:parser-fn
                               {"date" [:packed-local-date :relaxed?]}})]
    (verify-relaxed-parse ds)))


(deftest csv-keyword-colnames
  (let [stocks (ds-base/->dataset "test/data/stocks.csv" {:key-fn keyword})]
    (is (every? keyword? (ds-base/column-names stocks)))))


(deftest parse-empty-column-name
  (let [data (ds-base/->dataset "test/data/rcsv.csv")]
    (is (= #{0 "Urban Female" "Urban Male" "Rural Female" "Rural Male"}
           (set (ds-base/column-names data))))))


(deftest parse-ip-addrs-as-string
  (let [data (ds-base/->dataset "test/data/ip-addrs.csv")]
    (is (= :string (dtype/get-datatype (data "ip"))))))


(def arrow-file "test/data/iris.feather")
(def parquet-file "test/data/parquet/userdata1.parquet")


(deftest parse-parquet
  (let [ds (ds-base/->dataset parquet-file)]
    (is (= 13 (ds-base/column-count ds)))
    (is (= 1000 (ds-base/row-count ds)))
    (is (= #{:local-date-time :float64 :int32 :string}
           (->> (map dtype/get-datatype ds)
                set)))))


(deftest parse-ragged
  (let [ds (ds-base/->dataset "test/data/ragged.csv"
                              {:header-row? false})]
    (is (= 12 (ds-base/column-count ds)))
    (is (= [4 24 31 33 65 67 68 71 75 76 93 97]
           (vec ((ds-base/value-reader ds) 4))))
    (is (= [10 33 51 66 67 84 nil nil nil nil nil nil]
           (vec ((ds-base/value-reader ds) 10))))))


(deftest parse-small-doubles
  (let [ds (ds-base/->dataset "test/data/double_parse_test.csv")]
    (is (= 197 (count (filter #(not= 0.0 % ) (ds "pvalue")))))))


;; Failing due to apparently invalid iris.feather file
;; (deftest parse-arrow
;;   (let [ds (ds-base/->dataset arrow-file)]
;;     (is (= 13 (ds-base/column-count ds)))
;;     (is (= 1000 (ds-base/row-count ds)))
;;     (is (= #{:local-date-time :float64 :int32 :string}
;;            (->> (map dtype/get-datatype ds)
;;                 set)))))
