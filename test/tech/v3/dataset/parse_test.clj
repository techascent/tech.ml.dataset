(ns tech.v3.dataset.parse-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.zip :as zip]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.io.nippy]
            [tech.v3.libs.arrow :as arrow]
            [tech.v3.libs.clj-transit :as ds-transit]
            [taoensso.nippy :as nippy]
            [clojure.set :as set]
            [clojure.java.io :as io])
  (:import  [com.univocity.parsers.csv CsvFormat CsvParserSettings CsvParser]
            [java.nio.charset StandardCharsets]))


(def test-file "test/data/ames-house-prices/train.csv")


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
   ["CentralAir" :string]
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
  (let [result (ds/->dataset test-file)
        dtypes (->> (vals result)
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
    (let [result-missing-data (->> (vals result)
                                   (map (juxt ds-col/column-name
                                              (comp dtype/ecount ds-col/missing)))
                                   (remove #(= 0 (second %)))
                                   (sort-by first))]
      (is (= (set (map first missing-data))
             (set (map first result-missing-data))))))

  (let [result (ds/->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["Id" "SalePrice" "YearBuilt"]})]
    (is (= 3 (count result)))
    ;;Header row accounts for one.
    (is (= 100 (ds/row-count result)))))


(deftest base-ames-load-test
  ;;Here we just test that the options correctly pass through ->dataset
  (let [result (ds/->dataset test-file
                             {:n-records 100
                              :column-whitelist ["Id" "SalePrice" "YearBuilt"]})]
    (is (= 3 (ds/column-count result)))
    ;;Header row accounts for one.
    (is (= 100 (ds/row-count result)))))


(deftest specify-column-types
  ;;parse everything as float32
  (let [result (ds/->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]
                 :parser-fn :float32})]
    (is (= #{:float32}
           (set (map dtype/get-datatype (vals result)))))
    (is (= 3 (ds/column-count result))))

  ;;Next up is a map of colname->datatype
  (let [result (ds/->dataset
                test-file
                {:n-records 100
                 :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]
                 :parser-fn {"1stFlrSF" :float32
                             "2ndFlrSF" :int32}})]
    (is (= #{:float32 :int32 :int16}
           (set (map dtype/get-datatype (vals result)))))))


(deftest semi-colon-delimited-file
  (let [result (ds/->dataset "test/data/sample01.csv"
                             {:separator \;})]
    (is (= 3 (ds/column-count result)))))


(deftest tough-file
  (let [result (ds/->dataset "test/data/essential.csv"
                             {:n-initial-skip-rows 1
                              :skip-bad-rows? true})]
    (is (= 5 (ds/column-count result)))))


(defn- make-essential-csv-parser
  []
  (-> (doto (CsvParserSettings.)
        (.. getFormat (setLineSeparator "\n"))
        (.setHeaderExtractionEnabled true)
        (.setIgnoreLeadingWhitespaces true)
        (.setIgnoreTrailingWhitespaces true))
      (CsvParser.)))


(deftest custom-csv-parser
  (let [result (ds/->dataset "test/data/essential.csv"
                             {:csv-parser (make-essential-csv-parser)
                              :skip-bad-rows? true})]
    (is (= 5 (ds/column-count result)))))


(deftest simple-write-test
  (let [initial-ds (ds/->dataset
                    test-file
                    {:num-rows 20
                     :column-whitelist ["1stFlrSF" "2ndFlrSF" "3SsnPorch"]})
        _ (ds/write! initial-ds "test.tsv")
        new-ds (ds/->dataset "test.tsv")]
    (is (dfn/equals (initial-ds "1stFlrSF")
                    (new-ds "1stFlrSF")))
    (is (dfn/equals (initial-ds "2ndFlrSF")
                    (new-ds "2ndFlrSF"))))
  (let [missing-ds (-> (ds/->dataset
                        test-file
                        {:n-records 20
                         :column-whitelist [43 44 69]})
                       (ds/update-column
                        "1stFlrSF"
                        #(ds-col/set-missing % [2 4 7 9])))
        _ (ds/write! missing-ds "test.tsv")
        new-ds (ds/->dataset "test.tsv")]
    (is (dfn/equals (missing-ds "1stFlrSF")
                    (new-ds "1stFlrSF")))
    (is (= #{2 4 7 9}
           (set (ds-col/missing (new-ds "1stFlrSF")))))))


(deftest date-time-format-test-1
  (let [stock-ds (ds/->dataset "test/data/stocks.csv")]
    (is (= :packed-local-date (dtype/get-datatype (stock-ds "date")))))
  (let [temp-ds (ds/->dataset "test/data/seattle-temps.csv")]
    (is (= :zoned-date-time (dtype/get-datatype (temp-ds "date")))))
  (let [stock-ds (ds/->dataset "test/data/stocks.csv"
                               {:parser-fn
                                {"date" :local-date}})]
    (is (= :local-date (dtype/get-datatype (stock-ds "date"))))))


(deftest custom-reader
  (is (= 560 (ds/row-count (ds/->dataset (io/reader "test/data/stocks.csv")
                                         {:file-type :csv})))))


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
  (let [ds (ds/->dataset "test/data/stocks-bad-date.csv")]
    (is (= :string (dtype/get-datatype (ds "date"))))
    ;;Make sure unparsed data came through intact
    (is (= #{"hello" "1212"}
           (set/intersection #{"hello" "1212"}
                             (set (ds-col/unique (ds "date"))))))
    (let [updated-ds (ds/update-column
                      ds "date" (partial ds-col/parse-column
                                         [:packed-local-date :relaxed?]))]
      (verify-relaxed-parse updated-ds))))


(deftest bad-csv-relaxed-2
  (let [ds (ds/->dataset "test/data/stocks-bad-date.csv"
                         {:parser-fn
                          {"date" [:packed-local-date :relaxed?]}})]
    (verify-relaxed-parse ds)))


(deftest csv-keyword-colnames
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})]
    (is (every? keyword? (ds/column-names stocks)))))


(deftest parse-empty-column-name
  (let [data (ds/->dataset "test/data/rcsv.csv")]
    (is (= #{"column-0" "Urban Female" "Urban Male" "Rural Female" "Rural Male"}
           (set (ds/column-names data))))))


(deftest parse-ip-addrs-as-string
  (let [data (ds/->dataset "test/data/ip-addrs.csv")]
    (is (= :string (dtype/get-datatype (data "ip"))))))


(def arrow-file "test/data/iris.feather")
(def parquet-file "test/data/parquet/userdata1.parquet")


;;We will get back to this one.  Potentially there are good ways into this
;;via arrow.
#_(deftest parse-parquet
    (let [ds (ds/->dataset parquet-file)]
      (is (= 13 (ds/column-count ds)))
      (is (= 1000 (ds/row-count ds)))
      (is (= #{:local-date-time :float64 :int32 :string}
             (->> (map dtype/get-datatype (vals ds))
                  set)))))


(deftest parse-ragged
  (let [ds (ds/->dataset "test/data/ragged.csv"
                         {:header-row? false
                          :key-fn keyword})]
    (is (= [:column-0 :column-1 :column-2 :column-3 :column-4 :column-5
            :column-6 :column-7 :column-8 :column-9 :column-10 :column-11]
           (vec (ds/column-names ds))))
    (is (= 12 (ds/column-count ds)))
    (is (= [4 24 31 33 65 67 68 71 75 76 93 97]
           (vec ((ds/value-reader ds) 4))))
    (is (= [10 33 51 66 67 84 nil nil nil nil nil nil]
           (vec ((ds/value-reader ds) 10))))))


(deftest parse-small-doubles
  (let [ds (ds/->dataset "test/data/double_parse_test.csv")]
    (is (= 197 (count (filter #(not= 0.0 % ) (ds "pvalue")))))))


(deftest string-separators
  (let [ds (ds/->dataset "test/data/double_parse_test.csv" {:separator ","})]
    (is (= 197 (count (filter #(not= 0.0 % ) (ds "pvalue")))))
    (is (thrown? Throwable (ds/->dataset "test/data/double_parse_test.csv"
                                         {:separator ",n"})))))


(deftest quoted-column-data
  (try
    (let [ds (ds/->dataset [{:a "onelongstring"}])]
      (ds/write! ds "quoted.csv" {:quote? true})
      (is (= "\"a\"\n\"onelongstring\"\n"
             (slurp "quoted.csv"))))
    (finally
      (.delete (java.io.File. "quoted.csv")))))


(deftest text-data
  (try
    (let [ds (ds/->dataset [{:a "onestring"}
                            {:a "anotherstring"}
                            {}]
                           {:parser-fn :text})
          _ (is (= :text (-> (ds :a) meta :datatype)))
          _ (ds/write! ds "text.csv")
          _ (ds/write! ds "text.nippy")
          csv-ds (ds/->dataset "text.csv" {:parser-fn {"a" :text}
                                           :key-fn keyword})
          _ (is (= :text (-> (csv-ds :a) meta :datatype)))
          ;;_ (is (= 3 (ds/row-count csv-ds)))
          nippy-ds (ds/->dataset "text.nippy")
          _ (is (= :text (-> (nippy-ds :a) meta :datatype)))
          _ (is (= 3 (ds/row-count nippy-ds)))
          _ (arrow/write-dataset-to-stream! ds "text.arrow")
          ds-copy (arrow/read-stream-dataset-copying "text.arrow" {:key-fn keyword})
          _ (is (= :text (-> (ds-copy :a) meta :datatype)))
          _ (is (= 3 (ds/row-count nippy-ds)))
          ds-inplace (arrow/read-stream-dataset-inplace "text.arrow")]
      (is (= :text (-> (ds-inplace "a") meta :datatype)))
      (is (= 3 (ds/row-count nippy-ds))))
    (finally
      (.delete (java.io.File. "text.csv"))
      (.delete (java.io.File. "text.nippy"))
      (.delete (java.io.File. "text.arrow")))))


(deftest custom-parse-method
  (try
    (let [src-ds (ds/->dataset {:a ["1" "missing" "parse-failure" "2" "3"]})
          _ (ds/write! src-ds "custom-parse.csv")
          ds (ds/->dataset
              "custom-parse.csv"
              {:parser-fn {"a" [:int64
                                (fn [str-val]
                                  (cond
                                    (= str-val "missing")
                                    :tech.v3.dataset/missing
                                    (= str-val "parse-failure")
                                    :tech.v3.dataset/parse-failure
                                    :else
                                    (Long/parseLong str-val)))]}})]
      (is (= [1 nil nil 2 3]
             (vec (ds "a"))))
      (is (= #{1 2} (set (ds/missing ds))))
      (is (= #{2}
             (set (:unparsed-indexes (meta (ds "a"))))))
      (is (= ["parse-failure"]
             (vec (:unparsed-data (meta (ds "a")))))))
    (finally
      (.delete (java.io.File. "custom-parse.csv")))))


(deftest stocks-v5
  (let [v5 (ds/->dataset "test/data/stocks-v5.nippy")
        cur (ds/->dataset "test/data/stocks.csv")]
    (is (= (vec (v5 "date"))
           (vec (cur "date"))))))



(deftest gzipped-input-stream-issue-247
  (let [ds (ds/->dataset (io/input-stream "test/data/ames-train.csv.gz")
                         {:file-type :csv
                          :gzipped? true})
        correct-ds (ds/->dataset "test/data/ames-train.csv.gz")]
    (is (= (ds/row-count correct-ds) (ds/row-count ds)))))


(deftest pokemon-csv
  (let [ds (ds/->dataset "test/data/pokemon.csv")]
    (is (= "['Overgrow', 'Chlorophyll']" (first (ds "abilities"))))))

(deftest issue-292
  (let [ds (ds/->dataset "test/data/issue-292.csv" )]
    (is (== 3 (ds/column-count ds)))))


(deftest json-test
  (try
    (let [ds (-> (ds/->dataset "test/data/stocks.csv")
                 (ds/column-map "date" str ["date"]))
          _ (ds/write! ds "stocks.json")
          jds (ds/->dataset "stocks.json")]
      (is (= (vec (ds "date")) (vec (jds "date"))))
      (is (dfn/equals (ds "price") (jds "price"))))
    (finally
      (.delete (java.io.File. "stocks.json")))))


(deftest nippy-column
  (let [ds (ds/->dataset {:a [1 2 3] :b [4 5 6]})
        frozen (nippy/freeze (ds :a))
        thawed (nippy/thaw frozen)]
    (is (dfn/equals (ds :a) thawed))
    (is (ds-proto/is-column? thawed))))


(deftest empty-csv
  (let [ds (ds/->dataset "test/data/empty-csv-header.csv")]
    (is (= 7 (ds/column-count ds))))
  (let [ds (ds/->dataset "test/data/empty-csv.csv")]
    (is (= 0 (ds/column-count ds)))
    (is (ds/dataset? ds))))


(deftest comment-char
  (let [ds (ds/->dataset "test/data/csv-comment.csv")
        rows (ds/rows ds)]
    (is (= 5 (ds/row-count ds)))
    (is (= (rows -1) (rows -2)))))

(deftest issue-304
  (let [ds (ds/->dataset "test/data/issue-292.csv" {:n-initial-skip-rows 10})]
    (is (= 11 (-> (ds "10") (first))))))


(deftest issue-362
  (let [ds-seq (zip/zipfile->dataset-seq "test/data/unknown.zip")]
    (is (= 2 (count ds-seq)))))


(deftest issue-388-transit-support
  (let [ds (ds/->dataset {:a [1 2 3]
                          :b [:one :two :three]})
        str-data (ds-transit/dataset->transit-str ds)
        nds (ds-transit/transit-str->dataset str-data)]
    (is (= (ds :a) (nds :a)))
    (is (= (ds :b) (nds :b)))))


(deftest issue-434-transit-support
  (let [ds (ds/->dataset {:a [1 2 3]
                          :b [:one :two :three]
                          ;;transit encoding is milli instants
                          :c (dtype/make-container :packed-milli-instant [(java.time.Instant/now) (java.time.Instant/now)])})
        str-data (ds-transit/dataset->transit-str ds)
        nds (ds-transit/transit-str->dataset str-data)]
    (is (= (ds :a) (nds :a)))
    (is (= (ds :b) (nds :b)))
    (is (= (ds :c) (nds :c)))))


(deftest issue-414-json-parser-fn
  (is (= [1 2 3] (get (ds/->dataset "test/data/local_date.json"
                                    {:parser-fn {:time-period :local-date}})
                      "test"))))

(deftest dataset-parser-clear-packed-column
  (let [p (ds/dataset-parser)]
    (ds-proto/add-row p {:date (java.time.Instant/now)})
    (ds-proto/ds-clear p)
    (ds-proto/add-row p {:date (java.time.Instant/now)})
    (is (= 1 (count (@p :date))))))
