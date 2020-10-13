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
            [clojure.test :refer [deftest is testing]]))


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
           (vec (first (vals (ds-mod/labels dataset))))))

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
    (is (= (vec (repeat (count non-categorical) [-0.5 0.5]))
           (->> non-categorical
                (mapv (fn [colname]
                        (let [{col-min :min
                               col-max :max} (-> (ds/column dataset colname)
                                                 (ds-col/stats [:min :max]))]
                          [col-min col-max]))))))

    ;;Concatenation should work
    (is (= (mapv :fruit-name
                 (concat mapseq-ds mapseq-ds))
           (->> (-> (ds/concat dataset dataset)
                    (ds-cat/reverse-map-categorical-xforms)
                    (ds/mapseq-reader))
                (mapv :fruit-name))))

    (let [new-ds (ds/bind-> (ds/->dataset (map hash-map (repeat :mass) (range 20))) dataset
                            ;;The mean should happen in double or floating point space.
                            (assoc :mass-avg
                                   (dtype-fn/fixed-rolling-window
                                    (dtype/elemwise-cast (dataset :mass) :float64)
                                    5 dtype-fn/mean)))]
      (is (= [{:mass 0, :mass-avg 0.6}
              {:mass 1, :mass-avg 1.2}
              {:mass 2, :mass-avg 2.0}
              {:mass 3, :mass-avg 3.0}
              {:mass 4, :mass-avg 4.0}
              {:mass 5, :mass-avg 5.0}
              {:mass 6, :mass-avg 6.0}
              {:mass 7, :mass-avg 7.0}
              {:mass 8, :mass-avg 8.0}
              {:mass 9, :mass-avg 9.0}]
             (-> (ds/select new-ds [:mass :mass-avg] (range 10))
                 ds/mapseq-reader)))
      (let [sorted-ds (ds/sort-by-column new-ds :mass-avg >)]
        (is (= [{:mass 19, :mass-avg 18.4}
                {:mass 18, :mass-avg 17.8}
                {:mass 17, :mass-avg 17.0}
                {:mass 16, :mass-avg 16.0}
                {:mass 15, :mass-avg 15.0}
                {:mass 14, :mass-avg 14.0}
                {:mass 13, :mass-avg 13.0}
                {:mass 12, :mass-avg 12.0}
                {:mass 11, :mass-avg 11.0}
                {:mass 10, :mass-avg 10.0}]
               (-> (ds/select sorted-ds [:mass :mass-avg] (range 10))
                   ds/mapseq-reader)))))
    (let [nth-db (ds/take-nth src-ds 5)]
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

(deftest one-hot
  (testing "Testing one-hot into multiple column groups"
    (let [src-ds (test-utils/mapseq-fruit-dataset)
          dataset (-> src-ds
                      (ds/remove-columns [:fruit-subtype :fruit-label])
                      (ds-mod/set-inference-target :fruit-name)
                      (ds/string->one-hot [:fruit-name]))]
      (is (= {:one-hot-table
              {:orange :fruit-name-0,
               :mandarin :fruit-name-1,
               :apple :fruit-name-2,
               :lemon :fruit-name-3},
              :src-column :fruit-name,
              :result-datatype :float64}
             (into {} (first (ds-cat/dataset->one-hot-maps dataset)))))
      (is (= #{:mass :fruit-name-1 :fruit-name-0 :width :fruit-name-2 :color-score
	     :fruit-name-3 :height}
             (->> (ds/columns dataset)
                  (map ds-col/column-name)
                  set)))
      (is (= (->> (ds/mapseq-reader src-ds)
                  (take 20)
                  (mapv :fruit-name))
             (->> (first (vals (ds-mod/labels dataset)))
                  (take 20)
                  vec)))

      (is (= {:color-score :regression,
              :fruit-name-0 :classification,
              :fruit-name-1 :classification,
              :fruit-name-2 :classification,
              :fruit-name-3 :classification,
              :height :regression
              :width :regression,
              :mass :regression,
              }
             (ds-mod/model-type dataset (ds/column-names dataset)))))))


(deftest generalized-mapseq-ds
  (let [ds (ds/->dataset [{:a 1 :b {:a 1 :b 2}}
                          {:a 2}])]
    (is (= #{:int64 :persistent-map}
           (set (map dtype/get-datatype (vals ds)))))))


(deftest tensors-in-mapseq
  (let [ds (ds/->dataset [{:a (dtt/->tensor (partition 3 (range 9)))
                           :b "hello"}
                          {:a (dtt/->tensor (partition 3 (range 9)))
                           :b "goodbye"}])]
    (is (= #{:tensor :string}
           (set (map dtype/get-datatype (vals ds)))))))


(deftest datetime-missing
  (let [ds (ds/->dataset [{:d "1971-01-01"}
                          {:d "1970-01-01"}
                          {:d nil}
                          {:d "0001-01-01"}]
                         {:parser-fn {:d :local-date}})]
    (is (= 1 (dtype/ecount (ds-col/missing (ds :d)))))))
