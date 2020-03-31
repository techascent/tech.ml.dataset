(ns tech.ml.dataset.parse-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.base :as ds-base]
            [tech.ml.dataset.column :as ds-col])
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
  (let [result (ds-parse/csv->columns test-file)
        dtypes (->> result
                    (sort-by :name)
                    (mapv (juxt :name (comp dtype/get-datatype :data))))]
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
                                   (map (juxt :name (comp dtype/ecount :missing)))
                                   (remove #(= 0 (second %)))
                                   (sort-by first))]
      (is (= (set (map first missing-data))
             (set (map first result-missing-data))))))

  (let [result (ds-parse/csv->columns
                test-file
                {:n-records 100
                 :column-whitelist ["Id" "SalePrice" "YearBuilt"]})]
    (is (= 3 (count result)))
    ;;Header row accounts for one.
    (is (= 100 (dtype/ecount (:data (first result))))))
  (let [result (ds-parse/csv->columns test-file {:n-records 100
                                                 :column-blacklist (range 70)})]
    (is (= 11 (count result)))
    (is (= 100 (dtype/ecount (:data (first result)))))))


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
