(ns tech.libs.tablesaw-test
  (:require [tech.libs.tablesaw :as tablesaw]
            [tech.libs.tablesaw.datatype.tablesaw :as dtype-tbl]
            [tech.ml.dataset.etl :as etl]
            [tech.ml.dataset.etl.column-filters :as col-filters]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.svm :as ds-svm]
            [tech.ml.dataset.options :as ds-opts]
            [tech.datatype :as dtype]
            [tech.datatype.java-unsigned :as unsigned]
            [clojure.core.matrix :as m]
            [clojure.set :as c-set]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [clojure.test :refer :all]))


(deftest tablesaw-col-subset-test
  (let [test-col (tablesaw/make-column
                  (dtype-tbl/make-column :int32 (range 10)) {})
        select-vec [3 5 7 3 2 1]
        new-col (ds-col/select test-col select-vec)]
    (is (= select-vec
           (dtype/->vector new-col)))))


(def basic-pipeline
  '[[remove "Id"]
    ;;Replace missing values or just empty csv values with NA
    [replace-missing string? "NA"]
    [replace-string string? "" "NA"]
    [replace-missing numeric? 0]
    [replace-missing boolean? false]
    [->etl-datatype [or numeric? boolean?]]
    [string->number "Utilities" [["NA" -1] "ELO" "NoSeWa" "NoSewr" "AllPub"]]
    [string->number "LandSlope" ["Gtl" "Mod" "Sev" "NA"]]
    [string->number ["ExterQual"
                     "ExterCond"
                     "BsmtQual"
                     "BsmtCond"
                     "HeatingQC"
                     "KitchenQual"
                     "FireplaceQu"
                     "GarageQual"
                     "GarageCond"
                     "PoolQC"]   ["Ex" "Gd" "TA" "Fa" "Po" "NA"]]
    [set-attribute ["MSSubClass" "OverallQual" "OverallCond"] :categorical? true]
    [string->number "MasVnrType" {"BrkCmn" 1
                                 "BrkFace" 1
                                 "CBlock" 1
                                 "Stone" 1
                                 "None" 0
                                 "NA" -1}]
    [string->number "SaleCondition" {"Abnorml" 0
                                     "Alloca" 0
                                     "AdjLand" 0
                                     "Family" 0
                                     "Normal" 0
                                     "Partial" 1
                                     "NA" -1}]
    ;;Auto convert the rest that are still string columns
    [string->number string?]
    [m= "SalePriceDup" (col "SalePrice")]
    [m= "SalePrice" (log1p (col "SalePrice"))]])


(deftest base-etl-test
  (let [src-dataset (tablesaw/path->tablesaw-dataset "data/ames-house-prices/train.csv")
        ;;For inference, we won't have the target but we will have everything else.
        inference-columns (c-set/difference
                           (set (map ds-col/column-name
                                     (ds/columns src-dataset)))
                           #{"SalePrice"})
        inference-dataset (-> (ds/select src-dataset
                                               inference-columns
                                               (range 10))
                              (ds/->flyweight :error-on-missing-values? false))
        {:keys [dataset pipeline options]}
        (-> src-dataset
            (etl/apply-pipeline basic-pipeline
                                {:target "SalePrice"}))
        post-pipeline-columns (c-set/difference inference-columns #{"Id"})
        sane-dataset-for-flyweight (ds/select dataset post-pipeline-columns
                                                    (range 10))
        final-flyweight (-> sane-dataset-for-flyweight
                            (ds/->flyweight))]
    (is (= [81 1460] (m/shape src-dataset)))
    (is (= [81 1460] (m/shape dataset)))

    (is (= 45
           (count (col-filters/execute-column-filter dataset :categorical?))))
    (is (= #{"MSSubClass" "OverallQual" "OverallCond"}
           (c-set/intersection #{"MSSubClass" "OverallQual" "OverallCond"}
                               (set (col-filters/execute-column-filter dataset :categorical?)))))
    (is (= []
           (vec (col-filters/execute-column-filter dataset :string?))))
    (is (= ["SalePrice"]
           (vec (col-filters/execute-column-filter dataset :target?))))
    (is (= []
           (vec (col-filters/execute-column-filter dataset [:not [:numeric?]]))))
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

    (let [exact-columns (tablesaw/map-seq->tablesaw-dataset
                         inference-dataset
                         {:column-definitions (get-in options [:dataset-column-metadata :pre-pipeline])})
          ;;Just checking that this works at all..
          autoscan-columns (tablesaw/map-seq->tablesaw-dataset inference-dataset {})]

      ;;And the definition of exact is...
      (is (= (mapv :datatype (->> (get-in options [:dataset-column-metadata :pre-pipeline])
                                  (sort-by :name)))
             (->> (ds/columns exact-columns)
                  (map ds-col/metadata)
                  (sort-by :name)
                  (mapv :datatype))))
      (let [inference-ds (-> (etl/apply-pipeline exact-columns pipeline
                                                 (assoc options :inference? true))
                             :dataset)]
        ;;spot check a few of the items
        (is (m/equals (dtype/->vector (ds/column sane-dataset-for-flyweight "MSSubClass"))
                      (dtype/->vector (ds/column inference-ds "MSSubClass"))))
        ;;did categorical values get encoded identically?
        (is (m/equals (dtype/->vector (ds/column sane-dataset-for-flyweight "OverallQual"))
                      (dtype/->vector (ds/column inference-ds "OverallQual"))))))))


(def full-ames-pt-1
  '[[remove "Id"]
    ;;Replace missing values or just empty csv values with NA
    [replace-missing string? "NA"]
    [replace-string string? "" "NA"]
    [replace-missing numeric? 0]
    [replace-missing boolean? false]
    [->etl-datatype [or numeric? boolean?]]
    [string->number "Utilities" [["NA" -1] "ELO" "NoSeWa" "NoSewr" "AllPub"]]
    [string->number "LandSlope" ["Gtl" "Mod" "Sev" "NA"]]
    [string->number ["ExterQual"
                     "ExterCond"
                     "BsmtQual"
                     "BsmtCond"
                     "HeatingQC"
                     "KitchenQual"
                     "FireplaceQu"
                     "GarageQual"
                     "GarageCond"
                     "PoolQC"]   ["NA" "Po" "Fa" "TA" "Gd" "Ex"]]
    [set-attribute ["MSSubClass" "OverallQual" "OverallCond"] :categorical? true]
    [string->number "MasVnrType" {"BrkCmn" 1
                                 "BrkFace" 1
                                 "CBlock" 1
                                 "Stone" 1
                                 "None" 0
                                 "NA" -1}]
    [string->number "SaleCondition" {"Abnorml" 0
                                     "Alloca" 0
                                     "AdjLand" 0
                                     "Family" 0
                                     "Normal" 0
                                     "Partial" 1
                                     "NA" -1}]
    ;;Auto convert the rest that are still string columns
    [string->number string?]
    [m= "SalePrice" (log1p (col "SalePrice"))]
    [m= "OverallGrade" (* (col "OverallQual") (col "OverallCond"))]
    ;; Overall quality of the garage
    [m= "GarageGrade"  (* (col "GarageQual") (col "GarageCond"))]
    ;; Overall quality of the exterior
    [m= "ExterGrade" (* (col "ExterQual") (col "ExterCond"))]
    ;; Overall kitchen score
    [m=  "KitchenScore" (* (col "KitchenAbvGr") (col "KitchenQual"))]
    ;; Overall fireplace score
    [m= "FireplaceScore" (* (col "Fireplaces") (col "FireplaceQu"))]
    ;; Overall garage score
    [m= "GarageScore" (* (col "GarageArea") (col "GarageQual"))]
    ;; Overall pool score
    [m= "PoolScore" (* (col "PoolArea") (col "PoolQC"))]
    ;; Simplified overall quality of the house
    [m= "SimplOverallGrade" (* (col "OverallQual") (col "OverallCond"))]
    ;; Simplified overall quality of the exterior
    [m= "SimplExterGrade" (* (col "ExterQual") (col "ExterCond"))]
    ;; Simplified overall pool score
    [m= "SimplPoolScore" (* (col "PoolArea") (col "PoolQC"))]
    ;; Simplified overall garage score
    [m= "SimplGarageScore" (* (col "GarageArea") (col "GarageQual"))]
    ;; Simplified overall fireplace score
    [m= "SimplFireplaceScore" (* (col "Fireplaces") (col "FireplaceQu"))]
    ;; Simplified overall kitchen score
    [m= "SimplKitchenScore" (* (col "KitchenAbvGr") (col "KitchenQual"))]
    ;; Total number of bathrooms
    [m= "TotalBath" (+ (col "BsmtFullBath")
                       (* 0.5 (col "BsmtHalfBath"))
                       (col "FullBath")
                       (* 0.5 "HalfBath"))]
    ;; Total SF for house (incl. basement)
    [m= "AllSF" (+ (col "GrLivArea") (col "TotalBsmtSF"))]
    ;; Total SF for 1st + 2nd floors
    [m= "AllFlrsSF" (+ (col "1stFlrSF") (col "2ndFlrSF"))]
    ;; Total SF for porch
    [m= "AllPorchSF" (+ (col "OpenPorchSF") (col "EnclosedPorch")
                        (col "3SsnPorch") (col "ScreenPorch"))]])


(def ames-top-columns ["SalePrice"
                        "OverallQual"
                        "AllSF"
                        "AllFlrsSF"
                        "GrLivArea"
                        "GarageCars"
                        "ExterQual"
                        "KitchenQual"
                        "GarageScore"
                        "SimplGarageScore"
                        "GarageArea"])


(def full-ames-pt-2
  ;;Drop SalePrice column of course.
  (->> (rest ames-top-columns)
       (mapcat (fn [colname]
                 [['m= (str colname "-s2") ['** ['col colname] 2]]
                  ['m= (str colname "-s3") ['** ['col colname] 3]]
                  ['m= (str colname "-sqrt") ['sqrt ['col colname]]]]))
       (concat full-ames-pt-1)
       vec))


(def full-ames-pt-3
  (->> (concat full-ames-pt-2
               '[[m= [and
                      [not categorical?]
                      [not target?]
                      [> [abs [skew [col]]] 0.5]]
                  (log1p (col))]

                 [std-scaler [and
                              [not categorical?]
                              [not target?]]]])
       (vec)))


(deftest full-ames-pipeline-test
  (let [src-dataset (tablesaw/path->tablesaw-dataset
                     "data/ames-house-prices/train.csv")]
    (testing "Pathway through ames pt one is sane.  Checking skew."
      (let [{:keys [dataset pipeline options]}
            (etl/apply-pipeline src-dataset full-ames-pt-1 {:target "SalePrice"})]
       (is (= ames-top-columns
               (->> (get (ds/correlation-table dataset) "SalePrice")
                    (take 11)
                    (mapv first))))))
    (testing "Pathway through ames pt 2 is sane.  Checking skew."
      (let [{:keys [dataset pipeline options]}
            (etl/apply-pipeline src-dataset full-ames-pt-2 {:target "SalePrice"})
            skewed-set (set (col-filters/execute-column-filter
                             dataset '[and
                                       [not categorical?]
                                       [not target?]
                                       [> [abs [skew [col]]] 0.5]]))]
        ;;This count seems rather high...a diff against the python stuff would be wise.
        (is (= 67 (count skewed-set)))
        ;;Sale price cannot be in the set as it was explicitly removed.
        (is (not (contains? skewed-set "SalePrice")))))

    (testing "Full ames pathway is sane"
      (let [{:keys [dataset pipeline options]}
            (etl/apply-pipeline src-dataset full-ames-pt-3 {:target "SalePrice"})
            std-set (set (col-filters/execute-column-filter
                          dataset '[and
                                    [not categorical?]
                                    [not target?]]))
            mean-var-seq (->> std-set
                              (map (comp #(ds-col/stats % [:mean :variance])
                                         (partial ds/column dataset))))]
        ;;Are means 0?
        (is (m/equals (mapv :mean mean-var-seq)
                      (vec (repeat (count mean-var-seq) 0))
                      0.001))))))


(def mapseq-fruit-dataset
  (memoize
   (fn []
     (let [fruit-ds (slurp (io/resource "fruit_data_with_colors.txt"))
           dataset (->> (s/split fruit-ds #"\n")
                        (mapv #(s/split % #"\s+")))
           ds-keys (->> (first dataset)
                        (mapv (comp keyword ->kebab-case)))]
       (->> (rest dataset)
            (map (fn [ds-line]
                   (->> ds-line
                        (map (fn [ds-val]
                               (try
                                 (Double/parseDouble ^String ds-val)
                                 (catch Throwable e
                                   (-> (->kebab-case ds-val)
                                       keyword)))))
                        (zipmap ds-keys)))))))))


;;A sequence of maps is actually hard because keywords aren't represented
;;in tablesaw so we have to do a lot of work.  Classification also imposes
;;the necessity of mapping back from the label column to a sequence of
;;keyword labels.
(deftest mapseq-classification-test
  (let [pipeline '[[remove [:fruit-subtype :fruit-label]]
                   [string->number string?]
                   ;;Range numeric data to -1 1
                   [range-scaler (not categorical?)]]
        src-ds (ds/->dataset (mapseq-fruit-dataset) {})

        {:keys [dataset pipeline options]}
        (etl/apply-pipeline src-ds pipeline
                            {:target :fruit-name})

        origin-ds (mapseq-fruit-dataset)
        src-keys (set (keys (first (mapseq-fruit-dataset))))
        result-keys (set (->> (ds/columns dataset)
                              (map ds-col/column-name)))
        non-categorical (col-filters/execute-column-filter dataset [:not :categorical?])]

    (is (= #{59}
           (->> (ds/columns dataset)
                (map m/ecount)
                set)))

    ;;Column names can be keywords.
    (is (= (set (keys (first (mapseq-fruit-dataset))))
           (set (->> (ds/columns src-ds)
                     (map ds-col/column-name)))))

    (is (= (c-set/difference src-keys #{:fruit-subtype :fruit-label})
           result-keys))

    ;; Map back from values to keys for labels.  For tablesaw, column values
    ;; are never keywords.
    (is (= (mapv (comp name :fruit-name) (mapseq-fruit-dataset))
           (ds/labels dataset options)))

    (is (= {:fruit-name :classification}
           (ds/options->model-type options)))

    (is (= {:fruit-name :classification,
            :mass :regression,
            :width :regression,
            :height :regression,
            :color-score :regression}
           (ds-opts/model-type-map options (->> (ds/columns dataset)
                                                (map ds-col/column-name)))))

    (is (= (mapv (comp name :fruit-name) (mapseq-fruit-dataset))
           (->> (ds/->flyweight dataset :options options)
                (mapv :fruit-name))))


    (is (= (->> (ds/select dataset :all (range 10))
                ds/->flyweight
                (group-by :fruit-name))
           (->> (ds/select dataset :all (range 10))
                (ds/ds-group-by :fruit-name)
                (map (fn [[k group-ds]]
                       [k (vec (ds/->flyweight group-ds))]))
                (into {}))))

    ;;forward map from input value to encoded value.
    ;;After ETL, column values are all doubles
    (let [apple-value (double (get-in options [:label-map :fruit-name "apple"]))]
      (is (= #{"apple"}
             (->> dataset
                  (ds/ds-filter #(= apple-value (:fruit-name %)))
                  ;;Use full version of ->flyweight to do reverse mapping of numeric
                  ;;fruit name back to input label.
                  (#(ds/->flyweight % :options options))
                  (map :fruit-name)
                  set))))

    ;;dataset starts with apple apple apple mandarin mandarin
    (let [apple-v (double (get-in options [:label-map :fruit-name "apple"]))
          mand-v (double (get-in options [:label-map :fruit-name "mandarin"]))]
      (is (= [apple-v apple-v apple-v mand-v mand-v]
             ;;Order columns
             (->> (ds/select dataset [:mass :fruit-name :width] :all)
                  (ds/ds-column-map #(ds-col/set-metadata
                                      %
                                      {:name (name (ds-col/column-name %))}))
                  (take 2)
                  (drop 1)
                  (ds/from-prototype dataset "new-table")
                  (#(ds/select % :all (range 5)))
                  ;;Note the backward conversion failed in this case because we
                  ;;change the column names.
                  (#(ds/->flyweight % :options options))
                  (map #(get % "fruit-name"))
                  vec))))



    ;; Ensure range map works
    (is (= (vec (repeat (count non-categorical) [-1 1]))
           (->> non-categorical
                (mapv (fn [colname]
                        (let [{col-min :min
                               col-max :max} (-> (ds/column dataset colname)
                                                 (ds-col/stats [:min :max]))]
                          [(long col-min) (long col-max)]))))))

    ;;Concatenation should work
    (is (= (mapv (comp name :fruit-name)
                 (concat (mapseq-fruit-dataset)
                         (mapseq-fruit-dataset)))
           (->> (-> (ds/ds-concat dataset dataset)
                    (ds/->flyweight :options options))
                (mapv :fruit-name))))))


(deftest one-hot
  (testing "Testing one-hot into multiple column groups"
    (let [pipeline '[[remove [:fruit-subtype :fruit-label]]
                     [one-hot :fruit-name {:main ["apple" "mandarin"]
                                           :other :rest}]
                     [string->number string?]]
          src-ds (mapseq-fruit-dataset)
          {:keys [dataset pipeline options]}
          (etl/apply-pipeline src-ds pipeline
                              {:target :fruit-name})]
      (is (= {:fruit-name
              {"apple" [:fruit-name-main 1],
               "mandarin" [:fruit-name-main 2],
               "orange" [:fruit-name-other 1],
               "lemon" [:fruit-name-other 2]}}
             (:label-map options)))
      (is (= #{:mass :fruit-name-main :fruit-name-other :width :color-score :height}
             (->> (ds/columns dataset)
                  (map ds-col/column-name)
                  set)))
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv (comp name :fruit-name)))
             (->> (ds/column-values->categorical dataset :fruit-name options)
                  (take 20)
                  vec)))
      ;;Check that flyweight conversion is correct.
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv (comp name :fruit-name)))
             (->> (ds/->flyweight dataset :options options)
                  (map :fruit-name)
                  (take 20)
                  vec)))
      (is (= {:fruit-name :classification
              :mass :regression
              :width :regression
              :height :regression
              :color-score :regression}
           (ds-opts/model-type-map options (->> (ds/columns dataset)
                                                (map ds-col/column-name)))))))

  (testing "one hot-figure it out"
    (let [pipeline '[[remove [:fruit-subtype :fruit-label]]
                     [one-hot :fruit-name]
                     [string->number string?]]
          src-ds (mapseq-fruit-dataset)
          {:keys [dataset pipeline options]}
          (etl/apply-pipeline src-ds pipeline
                              {:target :fruit-name})]
      (is (= {:fruit-name
              {"apple" [:fruit-name-apple 1],
               "orange" [:fruit-name-orange 1],
               "lemon" [:fruit-name-lemon 1],
               "mandarin" [:fruit-name-mandarin 1]}}
             (:label-map options)))
      (is (= #{:mass :fruit-name-mandarin :width :fruit-name-orange :color-score
               :fruit-name-lemon :fruit-name-apple :height}
             (->> (ds/columns dataset)
                (map ds-col/column-name)
                set)))
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv (comp name :fruit-name)))
             (->> (ds/column-values->categorical dataset :fruit-name options)
                  (take 20)
                  vec)))

      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv (comp name :fruit-name)))
             (->> (ds/->flyweight dataset :options options)
                  (map :fruit-name)
                  (take 20)
                  vec)))

      (is (= {:fruit-name :classification
              :mass :regression
              :width :regression
              :height :regression
              :color-score :regression}
             (ds-opts/model-type-map options (->> (ds/columns dataset)
                                                  (map ds-col/column-name)))))))

  (testing "one hot - defined values"
    (let [pipeline '[[remove [:fruit-subtype :fruit-label]]
                     [one-hot :fruit-name ["apple" "mandarin" "orange" "lemon"]]
                     [string->number string?]]
          src-ds (mapseq-fruit-dataset)
          {:keys [dataset pipeline options]}
          (etl/apply-pipeline src-ds pipeline
                              {:target :fruit-name})]
      (is (= {:fruit-name
              {"apple" [:fruit-name-apple 1],
               "orange" [:fruit-name-orange 1],
               "lemon" [:fruit-name-lemon 1],
               "mandarin" [:fruit-name-mandarin 1]}}
             (:label-map options)))
      (is (= #{:mass :fruit-name-mandarin :width :fruit-name-orange :color-score
               :fruit-name-lemon :fruit-name-apple :height}
             (->> (ds/columns dataset)
                  (map ds-col/column-name)
                  set)))
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv (comp name :fruit-name)))
             (->> (ds/column-values->categorical dataset :fruit-name options)
                  (take 20)
                  vec)))

      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv (comp name :fruit-name)))
             (->> (ds/->flyweight dataset :options options)
                  (map :fruit-name)
                  (take 20)
                  vec)))

      (is (= {:fruit-name :classification
              :mass :regression
              :width :regression
              :height :regression
              :color-score :regression}
             (ds-opts/model-type-map options (->> (ds/columns dataset)
                                                  (map ds-col/column-name))))))))


(deftest svm-missing-regression
  (let [basic-svm-pipeline '[[string->number string?]
                             [replace-missing * 0]
                             ;;scale everything [-1 1]
                             [range-scaler [not categorical?]]]
        label-map {-1.0 :negative
                   1.0 :positive}
        train-fname "data/test-svm-dataset.svm"
        {train-ds :dataset
          options :options
         pipeline :pipeline}
        (-> (ds-svm/parse-svm-file train-fname :label-map label-map)
            :dataset
            (etl/apply-pipeline basic-svm-pipeline {:target :label}))]

    (is (= nil
           (->> train-ds
                ds/columns
                (map (fn [col]
                       (when (seq (ds-col/missing col))
                         (assoc (ds-col/metadata col)
                                :missing (count (ds-col/missing col))))))
                (remove nil?)
                seq)))))


(deftest impute-missing-k-means
  (let [src-dataset (tablesaw/path->tablesaw-dataset "data/ames-house-prices/train.csv")
        largest-missing-column (->> (ds/columns src-dataset)
                                    (sort-by (comp count ds-col/missing) >)
                                    ;;Numeric
                                    (filter #((set unsigned/datatypes)
                                              (dtype/get-datatype %)))
                                    first)
        missing-name (ds-col/column-name largest-missing-column)
        missing-count (count (ds-col/missing largest-missing-column))
        src-pipeline '[[remove "Id"]
                       ;;Replace missing values or just empty csv values with NA
                       [replace-missing string? "NA"]
                       [replace-string string? "" "NA"]
                       [replace-missing boolean? false]
                       [impute-missing [not target?] {:method :k-means}]]
        {:keys [pipeline dataset options]} (etl/apply-pipeline src-dataset src-pipeline {:target "SalePrice"})
        infer-dataset (:dataset (etl/apply-pipeline src-dataset pipeline {:inference? true}))
        stats-vec [:mean :min :max]]
    ;;The source stats
    (is (m/equals [70.049 21.0 313.0]
                  (mapv (-> (ds/column src-dataset missing-name)
                            (ds-col/stats stats-vec))
                        stats-vec)
                  0.01))

    ;;Because k-means random initialization, the mean drifts by roughly 0.5 all the time.
    (is (m/equals [70.886 21.0 313.0]
                  (mapv (-> (ds/column dataset missing-name)
                            (ds-col/stats stats-vec))
                        stats-vec)
                  1))

    (is (m/equals [70.886 21.0 313.0]
                  (mapv (-> (ds/column infer-dataset missing-name)
                            (ds-col/stats stats-vec))
                        stats-vec)
                  1))
    (is (= 0 (count (ds-col/missing (ds/column dataset missing-name)))))
    (is (= 0 (count (ds-col/missing (ds/column infer-dataset missing-name)))))))


(defn cause-g-means-error
  "The root of this is that g/x means are not setup to work with nan values."
  []
  (let [src-dataset (tablesaw/path->tablesaw-dataset "data/ames-house-prices/train.csv")
        src-pipeline '[[remove "Id"]
                       ;;Replace missing values or just empty csv values with NA
                       [replace-missing string? "NA"]
                       [replace-string string? "" "NA"]
                       [replace-missing boolean? false]]
        preprocessed-dataset (-> (etl/apply-pipeline
                                  src-dataset
                                  src-pipeline
                                  {:target "SalePrice"})
                                 :dataset)]
    (ds/g-means preprocessed-dataset)))
