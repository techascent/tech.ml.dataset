(ns tech.ml.dataset.mapseq-test
  (:require [tech.ml.dataset.pipeline :as ds-pipe]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.column-filters :as col-filters]
            [tech.ml.dataset.options :as ds-opts]
            [tech.ml.dataset-test
             :refer [mapseq-fruit-dataset]
             :as ds-test]
            [tech.v2.datatype :as dtype]
            [clojure.set :as c-set]
            [clojure.test :refer :all]))


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

        dataset (as-> src-ds dataset
                  (ds/remove-columns dataset [:fruit-subtype :fruit-label])
                  (ds-pipe/string->number dataset)
                  (ds-pipe/range-scale
                   dataset
                   :column-name-seq (-> (col-filters/categorical? dataset)
                                        (col-filters/not dataset)))
                  (ds/set-inference-target dataset :fruit-name))

        origin-ds (mapseq-fruit-dataset)
        src-keys (set (keys (first (mapseq-fruit-dataset))))
        result-keys (set (->> (ds/columns dataset)
                              (map ds-col/column-name)))
        non-categorical (->> (ds/columns dataset)
                             (remove #(:categorical? (ds-col/metadata %))))]

    (is (= #{59}
           (->> (ds/columns dataset)
                (map dtype/ecount)
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
                (mapv :fruit-name))))

    (let [new-ds (-> (etl/apply-pipeline
                      (ds/->dataset (map hash-map (repeat :mass) (range 20)))
                      '[[m= :mass-avg (rolling 5 :mean (col :mass))]]
                      {})
                     :dataset)]
      (is (= [{:mass 0.0, :mass-avg 0.6}
              {:mass 1.0, :mass-avg 1.2}
              {:mass 2.0, :mass-avg 2.0}
              {:mass 3.0, :mass-avg 3.0}
              {:mass 4.0, :mass-avg 4.0}
              {:mass 5.0, :mass-avg 5.0}
              {:mass 6.0, :mass-avg 6.0}
              {:mass 7.0, :mass-avg 7.0}
              {:mass 8.0, :mass-avg 8.0}
              {:mass 9.0, :mass-avg 9.0}]
             (-> (ds/select new-ds [:mass :mass-avg] (range 10))
                 ds/->flyweight)))
      (let [sorted-ds (ds/ds-sort-by :mass-avg > new-ds)]
        (is (= [{:mass 19.0, :mass-avg 18.4}
                {:mass 18.0, :mass-avg 17.8}
                {:mass 17.0, :mass-avg 17.0}
                {:mass 16.0, :mass-avg 16.0}
                {:mass 15.0, :mass-avg 15.0}
                {:mass 14.0, :mass-avg 14.0}
                {:mass 13.0, :mass-avg 13.0}
                {:mass 12.0, :mass-avg 12.0}
                {:mass 11.0, :mass-avg 11.0}
                {:mass 10.0, :mass-avg 10.0}]
               (-> (ds/select sorted-ds [:mass :mass-avg] (range 10))
                   ds/->flyweight))))
      (let [nth-db (ds/ds-take-nth 5 src-ds)]
        (is (= [7 12] (m/shape nth-db)))
        (is (= [{:mass 192.0, :width 8}
                {:mass 80.0, :width 5}
                {:mass 166.0, :width 6}
                {:mass 156.0, :width 7}
                {:mass 160.0, :width 7}
                {:mass 356.0, :width 9}
                {:mass 158.0, :width 7}
                {:mass 150.0, :width 7}
                {:mass 154.0, :width 7}
                {:mass 186.0, :width 7}]
               (->> (-> (ds/select nth-db [:mass :width] (range 10))
                        ds/->flyweight)
                    (map #(update % :width int)))))))))

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
