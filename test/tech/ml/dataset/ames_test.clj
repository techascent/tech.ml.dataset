(ns tech.ml.dataset.ames-test
  (:require [tech.ml.dataset.pipeline :as ds-pipe]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.column-filters :as col-filters]
            [tech.ml.dataset-test
             :refer [mapseq-fruit-dataset]
             :as ds-test]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [clojure.set :as c-set]
            [tech.libs.tablesaw :as tablesaw]
            [clojure.test :refer :all]
            [clojure.core.matrix :as m]))


(deftest tablesaw-col-subset-test
  (let [test-col (dtype/make-container :tablesaw-column :int32
                                       (range 10))
        select-vec [3 5 7 3 2 1]
        new-col (ds-col/select test-col select-vec)]
    (is (= select-vec
           (dtype/->vector new-col)))))


(def src-ds (tablesaw/path->tablesaw-dataset
             "data/ames-house-prices/train.csv"))


(defn missing-pipeline
  [dataset]
  (-> (ds/->dataset dataset)
      (ds/remove-column "Id")
      (ds-pipe/replace-missing col-filters/string? "NA")
      (ds-pipe/replace col-filters/string? {"" "NA"})
      (ds-pipe/replace-missing col-filters/numeric? 0)
      (ds-pipe/replace-missing col-filters/boolean? false)
      (ds-pipe/->datatype :column-filter #(col-filters/or %
                                                          col-filters/numeric?
                                                          col-filters/boolean?))))


(deftest basic-pipeline-test
  (let [dataset (missing-pipeline src-ds)]
    (is (= 19 (count (ds/columns-with-missing-seq src-ds))))
    (is (= 0 (count (ds/columns-with-missing-seq dataset))))
    (is (= 42 (count (col-filters/categorical? dataset))))
    (is (= #{:string :float64}
           (->> (ds/columns dataset)
                (map dtype/get-datatype)
                set)))))


(defn string-and-math
  [dataset]
  (-> dataset
      (ds-pipe/string->number "Utilities" [["NA" -1] "ELO" "NoSeWa" "NoSewr" "AllPub"])
      (ds-pipe/string->number "LandSlope" ["Gtl" "Mod" "Sev" "NA"])
      (ds-pipe/string->number ["ExterQual"
                               "ExterCond"
                               "BsmtQual"
                               "BsmtCond"
                               "HeatingQC"
                               "KitchenQual"
                               "FireplaceQu"
                               "GarageQual"
                               "GarageCond"
                               "PoolQC"]   ["Ex" "Gd" "TA" "Fa" "Po" "NA"])
      (ds-pipe/assoc-metadata ["MSSubClass" "OverallQual" "OverallCond"] :categorical? true)
      (ds-pipe/string->number "MasVnrType" {"BrkCmn" 1
                                            "BrkFace" 1
                                            "CBlock" 1
                                            "Stone" 1
                                            "None" 0
                                            "NA" -1})
      (ds-pipe/string->number "SaleCondition" {"Abnorml" 0
                                               "Alloca" 0
                                               "AdjLand" 0
                                               "Family" 0
                                               "Normal" 0
                                               "Partial" 1
                                               "NA" -1})
      ;; ;;Auto convert the rest that are still string columns
      (ds-pipe/string->number)
      (ds-pipe/new-column "SalePriceDup" #(ds/column % "SalePrice"))
      (ds-pipe/update-column "SalePrice" dtype-fn/log1p)
      (ds/set-inference-target "SalePrice")))


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

    (is (= 45 (count (col-filters/categorical? dataset))))
    (is (= #{"MSSubClass" "OverallQual" "OverallCond"}
           (c-set/intersection #{"MSSubClass" "OverallQual" "OverallCond"}
                               (set (col-filters/categorical? dataset)))))
    (is (= []
           (vec (col-filters/string? dataset))))
    (is (= ["SalePrice"]
           (vec (col-filters/inference? dataset))))
    (is (= []
           (vec (->> (col-filters/numeric? dataset)
                     (col-filters/not dataset)))))
    (let [sale-price (ds/column dataset "SalePriceDup")
          sale-price-l1p (ds/column dataset "SalePrice")
          sp-stats (ds-col/stats sale-price [:mean :min :max])
          sp1p-stats (ds-col/stats sale-price-l1p [:mean :min :max])]
      (is (m/equals (mapv sp-stats [:mean :min :max])
                    [180921.195890 34900 755000]
                    0.01))
      (is (m/equals (mapv sp1p-stats [:mean :min :max])
                    [12.024 10.460 13.534]
                    0.01)))

    (is (= 10 (count inference-dataset)))
    (is (= 10 (count final-flyweight)))

    (let [pre-pipeline (map ds-col/metadata (ds/columns src-ds))
          exact-columns (tablesaw/map-seq->tablesaw-dataset
                         inference-dataset
                         {:column-definitions pre-pipeline})
          ;;Just checking that this works at all..
          autoscan-columns (tablesaw/map-seq->tablesaw-dataset inference-dataset {})]

      ;;And the definition of exact is...
      (is (= (mapv :datatype (->> pre-pipeline
                                  (sort-by :name)))
             (->> (ds/columns exact-columns)
                  (map ds-col/metadata)
                  (sort-by :name)
                  (mapv :datatype))))
      (let [inference-ds (-> exact-columns
                             missing-pipeline
                             string-and-math)]
        ;;spot check a few of the items
        (is (m/equals (dtype/->vector (ds/column sane-dataset-for-flyweight
                                                 "MSSubClass"))
                      (dtype/->vector (ds/column inference-ds "MSSubClass"))))
        ;;did categorical values get encoded identically?
        (is (m/equals (dtype/->vector (ds/column sane-dataset-for-flyweight
                                                 "OverallQual"))
                      (dtype/->vector (ds/column inference-ds "OverallQual"))))))))
