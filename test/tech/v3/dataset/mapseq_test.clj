(ns tech.v3.dataset.mapseq-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.column-filters :as cf]
            [tech.v3.dataset.math :as ds-math]
            [tech.v3.dataset.modelling :as ds-mod]
            [tech.v3.dataset.categorical :as ds-cat]
            [tech.v3.dataset.test-utils :as test-utils]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dtype-fn]
            [tech.v3.tensor :as dtt]
            [clojure.set :as set]
            [clojure.test :refer [deftest is]]))


(deftest mapseq-classification-test
  (let [src-ds (test-utils/mapseq-fruit-dataset)
        dataset (ds/bind-> src-ds ds
                  (ds/remove-columns [:fruit-subtype :fruit-label])
                  (ds/string->number cf/categorical)
                  (ds/update (cf/difference ds (cf/categorical ds))
                             #(ds-math/transform-minmax % (ds-math/fit-minmax %)))
                  (ds-mod/set-inference-target :fruit-name))
        mapseq-ds (ds/mapseq-reader (test-utils/mapseq-fruit-dataset))

        src-keys (set (keys (first mapseq-ds)))
        result-keys (->> (ds/columns dataset)
                         (map ds-col/column-name)
                         (set))
        non-categorical (ds/column-names
                         (cf/difference dataset (cf/categorical dataset)))]


    (is (= #{59}
           (->> (ds/columns dataset)
                (map dtype/ecount)
                set)))


      ;;Column names can be keywords.
    (is (= src-keys
           (set (->> (ds/columns src-ds)
                     (map ds-col/column-name)))))

    (is (= (set/difference src-keys #{:fruit-subtype :fruit-label})
           result-keys))

    ;; Map back from values to keys for labels.  For tablesaw, column values
    ;; are never keywords.
    (is (= (mapv :fruit-name mapseq-ds)
           (ds-mod/labels dataset)))

    (is (= {:fruit-name :classification}
           (ds-mod/model-type dataset)))

    (is (= {:fruit-name :classification,
            :mass :regression,
            :width :regression,
            :height :regression,
            :color-score :regression}
           (ds-mod/model-type dataset (ds/column-names dataset))))

    ;;Does the post-transformation value of fruit-name map to the
    ;;pre-transformation value of fruit-name?
    (is (= (mapv :fruit-name mapseq-ds)
           (->> (ds-cat/reverse-map-categorical-xforms dataset)
                (ds/mapseq-reader)
                (mapv :fruit-name))))


    (is (= (as-> (ds/select dataset :all (range 10)) dataset
             (ds/mapseq-reader dataset)
             (group-by :fruit-name dataset))
           (as-> (ds/select dataset :all (range 10)) ds
             (ds/group-by-column ds :fruit-name)
             (map (fn [[k group-ds]]
                    [k (vec (ds/mapseq-reader group-ds))])
                  ds)
             (into {} ds))))

    ;;forward map from input value to encoded value.
    ;;After ETL, column values are all doubles
    (let [apple-value (-> (get (ds-mod/inference-target-label-map dataset) :apple)
                          double)]
      (is (= #{:apple}
             (as-> dataset ds
                 (ds/filter ds #(= apple-value (:fruit-name %)))
                  ;;Use full version of ->flyweight to do reverse mapping of numeric
                 ;;fruit name back to input label.
                 (ds-cat/reverse-map-categorical-xforms ds)
                 (ds/mapseq-reader ds)
                 (map :fruit-name ds)
                 (set ds)))))




    ;; Ensure range map works
    (is (= (vec (repeat (count non-categorical) [-1 1]))
           (->> non-categorical
                (mapv (fn [colname]
                        (let [{col-min :min
                               col-max :max} (-> (ds/column dataset colname)
                                                 (ds-col/stats [:min :max]))]
                          [(long col-min) (long col-max)]))))))

    ;;Concatenation should work
    (is (= (mapv :fruit-name
                 (concat mapseq-ds mapseq-ds))
           (->> (-> (ds/concat dataset dataset)
                    (ds-cat/reverse-map-categorical-xforms)
                    (ds/mapseq-reader))
                (mapv :fruit-name))))

    (let [new-ds (as-> (ds/->dataset (map hash-map (repeat :mass) (range 20))) dataset
                   ;;The mean should happen in double or floating point space.
                   (assoc dataset :mass-avg
                          (dtype-fn/fixed-rolling-window (dataset :mass) 5 dtype-fn/mean )))]
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
                 ds/mapseq-reader)))
      (let [sorted-ds (ds/sort-by :mass-avg > new-ds)]
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
                   ds/mapseq-reader)))))
    (let [nth-db (ds/take-nth 5 src-ds)]
      (is (= [7 12] (dtype/shape nth-db)))
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
                      ds/mapseq-reader)
                  (map #(update % :width int))))))))

#_(deftest one-hot
  (testing "Testing one-hot into multiple column groups"
    (let [src-ds (ds/->dataset (mapseq-fruit-dataset))

          dataset (-> src-ds
                      (ds/remove-columns [:fruit-subtype :fruit-label])
                      (ds-pipe/one-hot :fruit-name
                                       {:main [:apple :mandarin]
                                        :other :rest})
                      (ds-pipe/string->number)
                      (ds/set-inference-target :fruit-name))]
      (is (= {:fruit-name
              {:apple [:fruit-name-main 1],
               :mandarin [:fruit-name-main 2],
               :orange [:fruit-name-other 1],
               :lemon [:fruit-name-other 2]}}
             (ds/dataset-label-map dataset)))
      (is (= #{:mass :fruit-name-main :fruit-name-other :width :color-score :height}
             (->> (ds/columns dataset)
                  (map ds-col/column-name)
                  set)))
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv :fruit-name))
             (->> (ds/labels dataset)
                  (take 20)
                  vec)))
      ;;Check that flyweight conversion is correct.
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv :fruit-name))
             (->> (ds/->flyweight dataset :number->string? true)
                  (map :fruit-name)
                  (take 20)
                  vec)))
      (is (= {:fruit-name :classification
              :mass :regression
              :width :regression
              :height :regression
              :color-score :regression}
             (ds/model-type dataset (ds/column-names dataset))))))

  (testing "one hot-figure it out"
    (let [src-ds (ds/->dataset (mapseq-fruit-dataset))
          dataset (-> src-ds
                      (ds-pipe/remove-columns [:fruit-subtype :fruit-label])
                      (ds-pipe/one-hot :fruit-name)
                      (ds-pipe/string->number))]
      (is (= {:fruit-name
              {:apple [:fruit-name-apple 1],
               :orange [:fruit-name-orange 1],
               :lemon [:fruit-name-lemon 1],
               :mandarin [:fruit-name-mandarin 1]}}
             (ds/dataset-label-map dataset)))
      (is (= #{:mass :fruit-name-mandarin :width :fruit-name-orange :color-score
               :fruit-name-lemon :fruit-name-apple :height}
             (->> (ds/columns dataset)
                  (map ds-col/column-name)
                  set)))
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv :fruit-name))
             (->> (ds/column-values->categorical dataset :fruit-name)
                  (take 20)
                  vec)))

      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv :fruit-name))
             (->> (ds/->flyweight dataset :number->string? true)
                  (map :fruit-name)
                  (take 20)
                  vec)))

      (is (= {:fruit-name :classification
              :mass :regression
              :width :regression
              :height :regression
              :color-score :regression}
             (ds/model-type dataset (ds/column-names dataset))))))

  (testing "one hot - defined values"
    (let [src-ds (ds/->dataset (mapseq-fruit-dataset))
          dataset (-> src-ds
                      (ds-pipe/remove-columns [:fruit-subtype :fruit-label])
                      (ds-pipe/one-hot :fruit-name [:apple :mandarin
                                                    :orange :lemon])
                      (ds-pipe/string->number))]
      (is (= {:fruit-name
              {:apple [:fruit-name-apple 1],
               :orange [:fruit-name-orange 1],
               :lemon [:fruit-name-lemon 1],
               :mandarin [:fruit-name-mandarin 1]}}
             (ds/dataset-label-map dataset)))
      (is (= #{:mass :fruit-name-mandarin :width :fruit-name-orange :color-score
               :fruit-name-lemon :fruit-name-apple :height}
             (->> (ds/columns dataset)
                  (map ds-col/column-name)
                  set)))
      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv :fruit-name))
             (->> (ds/column-values->categorical dataset :fruit-name)
                  (take 20)
                  vec)))

      (is (= (->> (mapseq-fruit-dataset)
                  (take 20)
                  (mapv :fruit-name))
             (->> (ds/->flyweight dataset :number->string? true)
                  (map :fruit-name)
                  (take 20)
                  vec)))

      (is (= {:fruit-name :classification
              :mass :regression
              :width :regression
              :height :regression
              :color-score :regression}
             (ds/model-type dataset (ds/column-names dataset)))))))


(deftest generalized-mapseq-ds
  (let [ds (ds/->dataset [{:a 1 :b {:a 1 :b 2}}
                          {:a 2}])]
    (is (= #{:int64 :object}
           (set (map dtype/get-datatype (vals ds)))))))


(deftest tensors-in-mapseq
  (let [ds (ds/->dataset [{:a (dtt/->tensor (partition 3 (range 9)))
                           :b "hello"}
                          {:a (dtt/->tensor (partition 3 (range 9)))
                           :b "goodbye"}])]
    (is (= #{:object :string}
           (set (map dtype/get-datatype (vals ds)))))))


(deftest datetime-missing
  (let [ds (ds/->dataset [{:d "1971-01-01"}
                          {:d "1970-01-01"}
                          {:d nil}
                          {:d "0001-01-01"}])]
    (is (= 1 (dtype/ecount (ds-col/missing (ds :d)))))))
