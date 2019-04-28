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
              [clojure.test :refer :all]))


(deftest tablesaw-col-subset-test
  (let [test-col (dtype/make-container :tablesaw-column :int32
                                       (range 10))
        select-vec [3 5 7 3 2 1]
        new-col (ds-col/select test-col select-vec)]
    (is (= select-vec
           (dtype/->vector new-col)))))


(def src-ds (tablesaw/path->tablesaw-dataset
             "data/ames-house-prices/train.csv"))


(defn basic-pipeline
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
  (let [dataset (basic-pipeline src-ds)]
    (is (= 19 (count (ds/columns-with-missing-seq src-ds))))
    (is (= 0 (count (ds/columns-with-missing-seq dataset))))
    (is (= #{:string :float64}
           (->> (ds/columns dataset)
                (map dtype/get-datatype)
                set)))))
