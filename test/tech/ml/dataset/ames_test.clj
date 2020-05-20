(ns tech.ml.dataset.ames-test
  (:require [tech.ml.dataset.pipeline
             :refer [m= col int-map]
             :as dsp]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline :as ds-pipe]
            [tech.ml.dataset.pipeline.base
             :refer [with-ds]]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.pipeline.column-filters
             :as cf]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [clojure.set :as c-set]
            [clojure.test :refer :all]))


(deftest tablesaw-col-subset-test
  (let [test-col (ds-col/new-column "unnamed" (range 10))
        select-vec [3 5 7 3 2 1]
        new-col (ds-col/select test-col select-vec)]
    (is (= select-vec
           (dtype/->vector new-col)))))


(def src-ds (ds/->dataset "data/ames-house-prices/train.csv"))


(defn missing-pipeline
  [dataset]
  (-> (ds/->dataset dataset)
      (ds/remove-column "Id")
      (dsp/replace-missing cf/string? "NA")
      (dsp/replace cf/string? {"" "NA"})
      (dsp/replace-missing cf/numeric? 0)
      (dsp/replace-missing cf/boolean? false)
      (dsp/->datatype #(cf/or cf/numeric?
                              cf/boolean?))))


(deftest basic-pipeline-test
  (let [dataset (missing-pipeline src-ds)]
    (is (= 19 (count (ds/columns-with-missing-seq src-ds))))
    (is (= 0 (count (ds/columns-with-missing-seq dataset))))
    (is (= 42 (count (cf/categorical? dataset))))
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
                  (->> (dsp/m= src-ds "SalePrice" #(dfn/log1p (col)))
                       (#(ds/column % "SalePrice"))
                       (take 5)
                       (vec)))))


(defn skew-column-filter
  [dataset]
  (with-ds dataset
    (cf/and cf/numeric-and-non-categorical-and-not-target
            #(cf/not "SalePrice")
            (fn []
              (cf/> #(let [skewness (dfn/skewness
                                     (dtype/->reader (col)
                                                     :float64
                                                     {:missing-policy :elide}))]
                       (dfn/abs skewness))
                    0.5)))))


;;This test fails if col-filters/and (intersection) isn't
;;implemented correctly
(deftest and-is-lazy
  (is (= 29 (count (skew-column-filter src-ds)))))


(defn string-and-math
  [dataset]
  (let [initial-ds
        (-> dataset
            (dsp/string->number "Utilities" [["NA" -1] "ELO" "NoSeWa" "NoSewr" "AllPub"])
            (dsp/string->number "LandSlope" ["Gtl" "Mod" "Sev" "NA"])
            (dsp/string->number ["ExterQual"
                                 "ExterCond"
                                 "BsmtQual"
                                 "BsmtCond"
                                 "HeatingQC"
                                 "KitchenQual"
                                 "FireplaceQu"
                                 "GarageQual"
                                 "GarageCond"
                                 "PoolQC"]   ["Ex" "Gd" "TA" "Fa" "Po" "NA"])
            (dsp/assoc-metadata ["MSSubClass" "OverallQual" "OverallCond"]
                                :categorical? true)
            (dsp/string->number "MasVnrType" {"BrkCmn" 1
                                              "BrkFace" 1
                                              "CBlock" 1
                                              "Stone" 1
                                              "None" 0
                                              "NA" -1})
            (dsp/string->number "SaleCondition" {"Abnorml" 0
                                                 "Alloca" 0
                                                 "AdjLand" 0
                                                 "Family" 0
                                                 "Normal" 0
                                                 "Partial" 1
                                                 "NA" -1})
            ;; ;;Auto convert the rest that are still string columns
            (dsp/string->number))]
    (if (ds/has-column? initial-ds "SalePrice")
      (-> initial-ds
          (dsp/new-column "SalePriceDup" #(ds/column % "SalePrice"))
          (dsp/update-column "SalePrice" dfn/log1p)
          (ds/set-inference-target "SalePrice"))
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
                              (ds/->flyweight :error-on-missing-values? false))

        dataset (-> src-ds
                    missing-pipeline
                    string-and-math)

        post-pipeline-columns (c-set/difference inference-columns #{"Id"})
        sane-dataset-for-flyweight (ds/select dataset post-pipeline-columns
                                                    (range 10))
        final-flyweight (-> sane-dataset-for-flyweight
                            (ds/->flyweight))]
    (is (= [81 1460] (dtype/shape src-dataset)))
    (is (= [81 1460] (dtype/shape dataset)))

    (is (= 45 (count (cf/categorical? dataset))))
    (is (= #{"MSSubClass" "OverallQual" "OverallCond"}
           (c-set/intersection #{"MSSubClass" "OverallQual" "OverallCond"}
                               (set (cf/categorical? dataset)))))
    (is (= []
           (vec (cf/string? dataset))))
    (is (= ["SalePrice"]
           (vec (cf/target? dataset))))
    (is (= []
           (vec (cf/not cf/numeric? dataset))))
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


    (let [pre-pipeline (map ds-col/metadata (ds/columns src-ds))
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
  (-> (missing-pipeline dataset)
      (dsp/string->number "Utilities" [["NA" -1] "ELO" "NoSeWa" "NoSewr" "AllPub"])
      (dsp/string->number "LandSlope" ["Gtl" "Mod" "Sev" "NA"])
      (dsp/string->number ["ExterQual"
                               "ExterCond"
                               "BsmtQual"
                               "BsmtCond"
                               "HeatingQC"
                               "KitchenQual"
                               "FireplaceQu"
                               "GarageQual"
                               "GarageCond"
                               "PoolQC"]   ["Ex" "Gd" "TA" "Fa" "Po" "NA"])
      (dsp/assoc-metadata ["MSSubClass" "OverallQual" "OverallCond"]
                              :categorical? true)
      (dsp/string->number "MasVnrType" {"BrkCmn" 1
                                            "BrkFace" 1
                                            "CBlock" 1
                                            "Stone" 1
                                            "None" 0
                                            "NA" -1})
      (dsp/string->number "SaleCondition" {"Abnorml" 0
                                               "Alloca" 0
                                               "AdjLand" 0
                                               "Family" 0
                                               "Normal" 0
                                               "Partial" 1
                                               "NA" -1})
      ;; ;;Auto convert the rest that are still string columns
      (dsp/string->number)
      (dsp/update-column "SalePrice" dfn/log1p)
      (ds/set-inference-target "SalePrice")
      (m= "OverallGrade" #(dfn/* (col "OverallQual") (col "OverallCond")))
      ;; Overall quality of the garage
      (m= "GarageGrade"  #(dfn/* (col "GarageQual") (col "GarageCond")))
      ;; Overall quality of the exterior
      (m= "ExterGrade" #(dfn/* (col "ExterQual") (col "ExterCond")))
      ;; Overall kitchen score
      (m=  "KitchenScore" #(dfn/* (col "KitchenAbvGr") (col "KitchenQual")))
      ;; Overall fireplace score
      (m= "FireplaceScore" #(dfn/* (col "Fireplaces") (col "FireplaceQu")))
      ;; Overall garage score
      (m= "GarageScore" #(dfn/* (col "GarageArea") (col "GarageQual")))
      ;; Overall pool score
      (m= "PoolScore" #(dfn/* (col "PoolArea") (col "PoolQC")))
      ;; Simplified overall quality of the house
      (m= "SimplOverallGrade" #(dfn/* (col "OverallQual") (col "OverallCond")))
      ;; Simplified overall quality of the exterior
      (m= "SimplExterGrade" #(dfn/* (col "ExterQual") (col "ExterCond")))
      ;; Simplified overall pool score
      (m= "SimplPoolScore" #(dfn/* (col "PoolArea") (col "PoolQC")))
      ;; Simplified overall garage score
      (m= "SimplGarageScore" #(dfn/* (col "GarageArea") (col "GarageQual")))
      ;; Simplified overall fireplace score
      (m= "SimplFireplaceScore" #(dfn/* (col "Fireplaces") (col "FireplaceQu")))
      ;; Simplified overall kitchen score
      (m= "SimplKitchenScore" #(dfn/* (col "KitchenAbvGr") (col "KitchenQual")))
      ;; Total number of bathrooms
      (m= "TotalBath" #(dfn/+ (col "BsmtFullBath")
                                   (dfn/* 0.5 (col "BsmtHalfBath"))
                                   (col "FullBath")
                                   (dfn/* 0.5 (col "HalfBath"))))
      ;; Total SF for house (incl. basement)
      (m= "AllSF" #(dfn/+ (col "GrLivArea") (col "TotalBsmtSF")))
      ;; Total SF for 1st + 2nd floors
      (m= "AllFlrsSF" #(dfn/+ (col "1stFlrSF") (col "2ndFlrSF")))
      ;; Total SF for porch
      (m= "AllPorchSF" #(dfn/+ (col "OpenPorchSF") (col "EnclosedPorch")
                                    (col "3SsnPorch") (col "ScreenPorch")))))


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
   "ExterGrade"])


(defn full-ames-pt-2
  [dataset]
  ;;Drop SalePrice column of course.
  (->> (rest ames-top-columns)
       (reduce (fn [dataset colname]
                 (-> dataset
                     (m= (str colname "-s2") #(dfn/pow (col colname) 2))
                     (m= (str colname "-s3") #(dfn/pow (col colname) 3))
                     (m= (str colname "-sqrt") #(dfn/sqrt (col colname)))))
               dataset)))


(defn full-ames-pt-3
  [dataset]
  (-> dataset
      (m= (skew-column-filter dataset)
          #(dfn/log1p (col)))
      (dsp/std-scale)))


(deftest full-ames-pipeline-test
  (let [src-dataset src-ds]
    (testing "Pathway through ames pt one is sane.  Checking skew."
      (let [dataset (full-ames-pt-1 src-dataset)]
       (is (= ames-top-columns
               (->> (get (ds/correlation-table dataset) "SalePrice")
                    (take 11)
                    (mapv first))))
       (let [[n-cols n-rows] (dtype/shape src-dataset)
             [n-new-cols n-new-rows] (-> (dsp/filter src-dataset
                                                         "GrLivArea"
                                                         #(dfn/< (col) 4000))
                                         dtype/shape)
             num-over-the-line (->> (ds/column src-dataset "GrLivArea")
                                    (dtype/->reader)
                                    (filter #(>= (int %) 4000))
                                    count)]
         ;;Ensure our test isn't pointless.
         (is (not= 0 num-over-the-line))
         (is (= n-new-rows
                (- n-rows num-over-the-line))))
       (let [new-ds (m= src-dataset "SimplOverallQual"
                        #(int-map {1 1 2 1 3 1
                                   4 2 5 2 6 2
                                   7 3 8 3 9 3 10 3}
                                  (col "OverallQual")))]
         (is (= #{1 2 3}
                (->> (ds/column new-ds "SimplOverallQual")
                     (ds-col/unique)
                     (map int)
                     set))))))
    (testing "Pathway through ames pt 2 is sane.  Checking skew."
      (let [dataset (-> src-ds
                        full-ames-pt-1
                        full-ames-pt-2)
            skewed-set (set (skew-column-filter dataset))]
        ;;This count seems rather high...a diff against the python stuff would be wise.
        (is (= 64 (count skewed-set)))
        (is (= 45 (count (cf/categorical? dataset))))
        ;;Sale price cannot be in the set as it was explicitly removed.
        (is (not (contains? skewed-set "SalePrice")))))

    (testing "Full ames pathway is sane"
      (let [dataset (-> src-ds
                        full-ames-pt-1
                        full-ames-pt-2
                        full-ames-pt-3)
            std-set (set (cf/numeric-and-non-categorical-and-not-target dataset))
            mean-var-seq (->> std-set
                              (map (comp #(ds-col/stats % [:mean :variance])
                                         (partial ds/column dataset))))]
        ;;Are means 0?
        (is (dfn/equals (mapv :mean mean-var-seq)
                        (vec (repeat (count mean-var-seq) 0))
                        0.001))
        (let [pca-ds (dsp/pca dataset)]
          (is (= 127 (count (ds/columns dataset))))
          (is (= 45 (count (cf/categorical? pca-ds))))
          (is (= 75 (count (ds/columns pca-ds))))
          (is (= 1 (count (cf/target? pca-ds)))))
        (let [pca-ds (dsp/pca dataset
                              cf/numeric-and-non-categorical-and-not-target
                              :n-components 10)]
          (is (= 45 (count (cf/categorical? dataset))))
          (is (= 45 (count (cf/categorical? pca-ds))))
          (is (= 127 (count (ds/columns dataset))))
          (is (= 56 (count (ds/columns pca-ds))))
          (is (= 1 (count (cf/target? pca-ds)))))))))


(deftest tostring-regression
  (testing "tostring has to work with missing values"
    (is (string?
         (.toString ^Object src-ds)))))


(deftest desc-stats-and-correlation
  []
  (let [stats-data (ds/descriptive-stats src-ds)
        corr-data (ds-pipe/correlation-table src-ds :colname-seq ["SalePrice"])]
    (is (= #{:min :n-missing :col-name :mean :datatype :skew :mode
             :standard-deviation :n-valid :max}
           (set (ds/column-names stats-data))))
    (is (= 81
           (->> corr-data
                first
                second
                count)))))
