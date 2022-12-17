(ns tech.v3.dataset.ames-test
  (:require [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column-filters :as cf]
            [tech.v3.dataset.modelling :as ds-mod]
            [tech.v3.dataset.math :as ds-math]
            [tech.v3.dataset.neanderthal :as ds-nean]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [taoensso.nippy :as nippy]
            [clojure.set :as c-set]
            [clojure.pprint :as pp]
            [clojure.data :as data]
            [clojure.test :refer [deftest is]]
            [clojure.tools.logging :as log]))


(deftest tablesaw-col-subset-test
  (let [test-col (ds-col/new-column "unnamed" (range 10))
        select-vec [3 5 7 3 2 1]
        new-col (ds-col/select test-col select-vec)]
    (is (= select-vec
           (dtype/->vector new-col)))))


(def src-ds (ds/->dataset "test/data/ames-house-prices/train.csv"
                          {:parser-fn {"CentralAir" :boolean}}))


(defn missing-pipeline
  [dataset]
  (ds/bind-> (ds/->dataset dataset) ds
    (ds/remove-column "Id")
    (ds/update cf/string ds/replace-missing-value "NA")
    (ds/update-elemwise cf/string #(get {"" "NA"} % %))
    (ds/update cf/numeric ds/replace-missing-value 0)
    (ds/update cf/boolean ds/replace-missing-value false)
    (ds/update-columnwise (cf/union (cf/numeric ds) (cf/boolean ds))
                          #(dtype/elemwise-cast % :float64))))

(def original-missing
  #{"LotFrontage" "Alley" "MasVnrType" "MasVnrArea"
    "BsmtQual" "BsmtCond" "BsmtExposure" "BsmtFinType1"
    "BsmtFinType2" "Electrical" "FireplaceQu" "GarageType"
    "GarageYrBlt" "GarageFinish" "GarageQual" "GarageCond"
    "PoolQC" "Fence" "MiscFeature"})


(deftest basic-pipeline-test
  (let [dataset (missing-pipeline src-ds)]
    (is (= original-missing
           (set (map :column-name (ds/columns-with-missing-seq src-ds))))
        (with-out-str
          (pp/pprint
           (data/diff
            original-missing
            (set (map :column-name (ds/columns-with-missing-seq src-ds)))))))
    (is (= 0 (count (ds/columns-with-missing-seq dataset))))
    (is (= 42 (ds/column-count (cf/categorical dataset))))
    (is (= #{:string :float64}
           (->> (ds/columns dataset)
                (map dtype/get-datatype)
                set)))))


(deftest log1p-changes-datatype
  ;;This causes actual data corruption--if the column datatype gets clipped
  ;;back to an integer type you get values like 12 instead of 12.5.  For this
  ;;dataset that destroys the accuracy so we make sure the log1p operation does
  ;;in fact change the datatype correctly.
  (is (dfn/equals [12.24769911637256
                   12.109016442313738
                   12.317171167298682
                   11.849404844423074
                   12.429220196836383]
                  (-> (ds/update-columns src-ds ["SalePrice"] dfn/log1p)
                      (ds/select-rows (range 5))
                      (ds/column "SalePrice")
                      (vec)))))


(defn skew-column-filter
  [dataset]
  (ds/bind-> (dissoc dataset "SalePrice") ds
    (cf/numeric)
    (cf/difference (cf/categorical ds))
    (cf/column-filter #(> (Math/abs (dfn/skew %))
                          0.5))))

(def old-cols
  #{"TotalBsmtSF" "YearRemodAdd" "LotFrontage" "PoolArea" "BsmtFinSF2" "YearBuilt"
  "LowQualFinSF" "GrLivArea" "MSSubClass" "WoodDeckSF" "KitchenAbvGr" "Fireplaces"
  "3SsnPorch" "OverallCond" "1stFlrSF" "EnclosedPorch" "MiscVal" "2ndFlrSF"
  "TotRmsAbvGrd" "GarageYrBlt" "BsmtHalfBath" "OpenPorchSF" "BsmtFinSF1" "LotArea"
  "MasVnrArea" "ScreenPorch" "BsmtFullBath" "BsmtUnfSF" "HalfBath"})


(deftest custom-colfilter-test
  (is (= old-cols
         (-> (skew-column-filter src-ds)
             (ds/column-names)
             (set)))))


(defn string-and-math
  [dataset]
  (let [initial-ds
        (-> dataset
            (ds/categorical->number ["Utilities"] [["NA" -1] "ELO" "NoSeWa"
                                              "NoSewr" "AllPub"])
            (ds/categorical->number ["LandSlope"] ["Gtl" "Mod" "Sev" "NA"])
            (ds/categorical->number ["ExterQual"
                                "ExterCond"
                                "BsmtQual"
                                "BsmtCond"
                                "HeatingQC"
                                "KitchenQual"
                                "FireplaceQu"
                                "GarageQual"
                                "GarageCond"
                                "PoolQC"]   ["Ex" "Gd" "TA" "Fa" "Po" "NA"])
            (ds/assoc-metadata ["MSSubClass" "OverallQual" "OverallCond"]
                               :categorical? true)
            (ds/update-column "MasVnrType"
                              (fn [col] (map
                                         #(case % ("BrkCmn" "BrkFace" "CBlock" "Stone" ) "Brk"
                                                %)
                                         col)))


            (ds/update-column "SaleCondition"
                              (fn [col] (map
                                         #(case % ("Abnorml" "Alloca" "AdjLand" "Family" "Normal" ) "sale-1"
                                               %)
                                         col)))



            (ds/categorical->number ["MasVnrType"] {
                                                    "Brk"  1
                                                    "None" 0
                                                    "NA" -1})



            (ds/categorical->number ["SaleCondition"] {"sale-1" 0
                                                       "Partial" 1
                                                       "NA" -1})

            ;; ;;Auto convert the rest that are still string columns
            (ds/categorical->number cf/string))]
    (if (ds/has-column? initial-ds "SalePrice")
      (-> initial-ds
          (assoc "SalePriceDup" (initial-ds "SalePrice"))
          (ds/update-column "SalePrice" dfn/log1p)
          (ds-mod/set-inference-target "SalePrice"))
      initial-ds)))


(deftest base-etl-test
  (let [src-dataset src-ds
        ;;For inference, we won't have the target but we will have everything else.
        inference-columns (c-set/difference
                           (set (map ds-col/column-name
                                     (ds/columns src-dataset)))
                           #{"SalePrice"})
        inference-dataset (-> (ds/select src-dataset
                                         inference-columns
                                         (range 10))
                              (ds/mapseq-reader))

        dataset (-> src-ds
                    missing-pipeline
                    string-and-math)

        post-pipeline-columns (c-set/difference inference-columns #{"Id"})
        sane-dataset-for-flyweight (ds/select dataset post-pipeline-columns
                                                    (range 10))
        final-flyweight (-> sane-dataset-for-flyweight
                            (ds/mapseq-reader))]
    (is (= [81 1460] (dtype/shape src-dataset)))
    (is (= [81 1460] (dtype/shape dataset)))

    (is (= 45 (ds/column-count (cf/categorical dataset))))
    (is (= #{"MSSubClass" "OverallQual" "OverallCond"}
           (c-set/intersection #{"MSSubClass" "OverallQual" "OverallCond"}
                               (set (ds/column-names (cf/categorical dataset))))))
    (is (= 0 (ds/column-count (cf/string dataset))))
    (is (= ["SalePrice"]
           (vec (ds/column-names (cf/target dataset)))))
    (is (= []
           (vec (ds/column-names (cf/difference dataset (cf/numeric dataset))))))
    (let [sale-price (ds/column dataset "SalePriceDup")
          sale-price-l1p (ds/column dataset "SalePrice")
          sp-stats (ds-col/stats sale-price [:mean :min :max])
          sp1p-stats (ds-col/stats sale-price-l1p [:mean :min :max])]
      (is (dfn/equals (mapv sp-stats [:mean :min :max])
                      [180921.195890 34900 755000]
                      0.01))
      (is (dfn/equals (mapv sp1p-stats [:mean :min :max])
                      [12.024 10.460 13.534]
                      0.01)))

    (is (= 10 (count inference-dataset)))
    (is (= 10 (count final-flyweight)))


    (let [pre-pipeline (map meta (ds/columns src-ds))
          col-dtype-map (->> pre-pipeline
                             (map (fn [{:keys [name datatype]}]
                                    [name datatype]))
                             (into {}))
          exact-columns (ds/->dataset
                         inference-dataset
                         {:parser-fn col-dtype-map})
          ;;Just checking that this works at all..
          autoscan-columns (ds/->dataset inference-dataset {})]

      ;;And the definition of exact is...
      (is (every? #(= (dtype/get-datatype %)
                      (get col-dtype-map
                           (ds-col/column-name %)))
                  (ds/columns exact-columns)))
      (let [inference-ds (-> exact-columns
                             missing-pipeline
                             string-and-math)]
        ;;spot check a few of the items
        (is (dfn/equals (dtype/->vector (ds/column sane-dataset-for-flyweight
                                                 "MSSubClass"))
                      (dtype/->vector (ds/column inference-ds "MSSubClass"))))
        ;;did categorical values get encoded identically?
        (is (dfn/equals (dtype/->vector (ds/column sane-dataset-for-flyweight
                                                 "OverallQual"))
                      (dtype/->vector (ds/column inference-ds "OverallQual"))))))))


(defn full-ames-pt-1
  [dataset]
  (ds/bind-> (missing-pipeline dataset) ds
             (ds/categorical->number ["Utilities"] [["NA" -1] "ELO" "NoSeWa" "NoSewr" "AllPub"])
             (ds/categorical->number ["LandSlope"] ["Gtl" "Mod" "Sev" "NA"])
             (ds/categorical->number ["ExterQual"
                                      "ExterCond"
                                      "BsmtQual"
                                      "BsmtCond"
                                      "HeatingQC"
                                      "KitchenQual"
                                      "FireplaceQu"
                                      "GarageQual"
                                      "GarageCond"
                                      "PoolQC"]   ["Ex" "Gd" "TA" "Fa" "Po" "NA"])
             (ds/assoc-metadata ["MSSubClass" "OverallQual" "OverallCond"]
                                :categorical? true)

             (ds/update-column "MasVnrType"
                               (fn [col] (map
                                          #(case % ("BrkCmn" "BrkFace" "CBlock" "Stone" ) "Brk"
                                                 %)
                                          col)))


             (ds/update-column "SaleCondition"
                               (fn [col] (map
                                          #(case % ("Abnorml" "Alloca" "AdjLand" "Family" "Normal" ) "sale-1"
                                                %)
                                          col)))



             (ds/categorical->number ["MasVnrType"] {
                                                     "Brk"  1
                                                     "None" 0
                                                     "NA" -1})



             (ds/categorical->number ["SaleCondition"] {"sale-1" 0
                                                        "Partial" 1
                                                        "NA" -1})
             ;; ;;Auto convert the rest that are still string columns
             (ds/categorical->number cf/string)
             (ds/update-column "SalePrice" dfn/log1p)
             (ds-mod/set-inference-target "SalePrice")
             (assoc "OverallGrade" (dfn/* (ds "OverallQual") (ds "OverallCond")))
             ;; Overall quality of the garage
             (assoc "GarageGrade"  (dfn/* (ds "GarageQual") (ds "GarageCond")))
             ;; Overall quality of the exterior
             (assoc "ExterGrade" (dfn/* (ds "ExterQual") (ds "ExterCond")))
             ;; Overall kitchen score
             (assoc  "KitchenScore" (dfn/* (ds "KitchenAbvGr") (ds "KitchenQual")))
             ;; Overall fireplace score
             (assoc "FireplaceScore" (dfn/* (ds "Fireplaces") (ds "FireplaceQu")))
             ;; Overall garage score
             (assoc "GarageScore" (dfn/* (ds "GarageArea") (ds "GarageQual")))
             ;; Overall pool score
             (assoc "PoolScore" (dfn/* (ds "PoolArea") (ds "PoolQC")))
             ;; Simplified overall quality of the house
             (assoc "SimplOverallGrade" (dfn/* (ds "OverallQual") (ds "OverallCond")))
             ;; Simplified overall quality of the exterior
             (assoc "SimplExterGrade" (dfn/* (ds "ExterQual") (ds "ExterCond")))
             ;; Simplified overall pool score
             (assoc "SimplPoolScore" (dfn/* (ds "PoolArea") (ds "PoolQC")))
             ;; Simplified overall garage score
             (assoc "SimplGarageScore" (dfn/* (ds "GarageArea") (ds "GarageQual")))
             ;; Simplified overall fireplace score
             (assoc "SimplFireplaceScore" (dfn/* (ds "Fireplaces") (ds "FireplaceQu")))
             ;; Simplified overall kitchen score
             (assoc "SimplKitchenScore" (dfn/* (ds "KitchenAbvGr") (ds "KitchenQual")))
             ;; Total number of bathrooms
             (assoc "TotalBath" (dfn/+ (ds "BsmtFullBath")
                                       (dfn/* 0.5 (ds "BsmtHalfBath"))
                                       (ds "FullBath")
                                       (dfn/* 0.5 (ds "HalfBath"))))
             ;; Total SF for house (incl. basement)
             (assoc "AllSF" (dfn/+ (ds "GrLivArea") (ds "TotalBsmtSF")))
             ;; Total SF for 1st + 2nd floors
             (assoc "AllFlrsSF" (dfn/+ (ds "1stFlrSF") (ds "2ndFlrSF")))
             ;; Total SF for porch
             (assoc "AllPorchSF" (dfn/+ (ds "OpenPorchSF") (ds "EnclosedPorch")
                                        (ds "3SsnPorch") (ds "ScreenPorch")))))


(def ames-top-columns
  ["SalePrice"
   "OverallQual"
   "AllSF"
   "AllFlrsSF"
   "GrLivArea"
   "GarageCars"
   "ExterQual"
   "TotalBath"
   "KitchenQual"
   "GarageArea"
   "SimplExterGrade"])


(defn full-ames-pt-2
  [dataset]
  ;;Drop SalePrice column of course.
  (->> (rest ames-top-columns)
       (reduce (fn [dataset colname]
                 (ds/bind-> dataset ds
                   (assoc (str colname "-s2") (dfn/pow (ds colname) 2))
                   (assoc (str colname "-s3") (dfn/pow (ds colname) 3))
                   (assoc (str colname "-sqrt") (dfn/sqrt (ds colname)))))
               dataset)))


(defn full-ames-pt-3
  [dataset]
  (let [feature-ds (cf/difference dataset (cf/target dataset))
        numeric-feature-ds (cf/difference feature-ds (cf/categorical feature-ds))
        skew-fixed (ds/update-columnwise numeric-feature-ds skew-column-filter
                                         dfn/log1p)
        std-scale-fit (ds-math/fit-std-scale skew-fixed)]
    (merge dataset (ds-math/transform-std-scale skew-fixed std-scale-fit))))


(deftest full-ames-pipeline-test
  (let [dataset (full-ames-pt-1 src-ds)]
    (is (= ames-top-columns
           (->> (get (ds-math/correlation-table dataset :colname-seq ["SalePrice"])
                     "SalePrice")
                (take 11)
                (mapv first))))
    (let [[n-cols n-rows] (dtype/shape src-ds)
          [n-new-cols n-new-rows] (-> (ds/filter-column src-ds
                                                        "GrLivArea"
                                                        #(< % 4000))
                                      dtype/shape)
          num-over-the-line (->> (ds/column src-ds "GrLivArea")
                                 (dtype/->reader)
                                 (filter #(>= (int %) 4000))
                                 count)]
      ;;Ensure our test isn't pointless.
      (is (not= 0 num-over-the-line))
      (is (= n-new-rows
             (- n-rows num-over-the-line))))
    (let [new-ds (assoc src-ds "SimplOverallQual"
                        (dtype/emap {1 1 2 1 3 1
                                     4 2 5 2 6 2
                                     7 3 8 3 9 3 10 3}
                                    :int64
                                    (src-ds "OverallQual")))]
      (is (= #{1 2 3}
             (->> (ds/column new-ds "SimplOverallQual")
                  (ds-col/unique)
                  (map int)
                  set))))
    (let [dataset (-> src-ds
                      full-ames-pt-1
                      full-ames-pt-2)
          skewed-set (set (ds/column-names (skew-column-filter dataset)))]
      ;;This count seems rather high...a diff against the python stuff would be wise.
      (is (= 64 (count skewed-set)))
      (is (= 45 (ds/column-count (cf/categorical dataset))))
      ;;Sale price cannot be in the set as it was explicitly removed.
      (is (not (contains? skewed-set "SalePrice"))))))


(deftest ^:travis-broken full-ames-pipeline-pca
  (let [dataset (-> src-ds
                    full-ames-pt-1
                    full-ames-pt-2
                    full-ames-pt-3)
        numeric-ds (cf/difference
                    (cf/numeric dataset)
                    (cf/union (cf/categorical dataset)
                              (cf/target dataset)))
        std-set (set (ds/column-names numeric-ds))
        mean-var-seq (->> std-set
                          (map (comp #(ds-col/stats % [:mean :variance])
                                     (partial ds/column dataset))))]
    ;;Are means 0?
    (is (dfn/equals (mapv :mean mean-var-seq)
                    (vec (repeat (count mean-var-seq) 0))
                    0.001))
    (let [cat-ds (cf/categorical dataset)
          pca-fit (ds-nean/fit-pca numeric-ds {:n-components 10})
          pca-ds (ds-nean/transform-pca numeric-ds pca-fit)]
      (is (= 127 (ds/column-count dataset)))
      (is (= 45 (ds/column-count cat-ds)))
      (is (= 10 (count (ds/columns pca-ds)))))))


(deftest tostring-regression
  (is (string?
       (.toString ^Object src-ds))))


(deftest desc-stats-and-correlation
  []
  (let [stats-data (ds/descriptive-stats src-ds)
        corr-data (ds-math/correlation-table src-ds :colname-seq ["SalePrice"])]
    (is (= #{:min :n-missing :col-name :mean :datatype :skew :mode
             :standard-deviation :n-valid :max :first :last}
           (set (ds/column-names stats-data))))
    (is (= 35
           (->> corr-data
                first
                second
                count)))))


(deftest nippyfreezethaw
  (let [ds src-ds
        data (ds/dataset->data ds)
        thawed (ds/data->dataset data)]
    (is (= (ds/row-count ds)
           (count (mapv #(into [] %) (ds/rowvecs thawed)))))))
