(ns tech.ml.dataset
  "Column major dataset abstraction for efficiently manipulating
  in memory datasets."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional.impl :as fn-impl]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.utils :as ml-utils]
            [tech.parallel.require :as parallel-req]
            [clojure.set :as c-set]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.pipeline.column-filters :as col-filters]
            [tech.ml.dataset.options :as options]
            [tech.ml.dataset.base]
            [tech.ml.dataset.modelling]
            [tech.ml.dataset.math]
            [clojure.math.combinatorics :as comb])
  (:import [smile.clustering KMeans GMeans XMeans PartitionClustering]))


(set! *warn-on-reflection* true)

(fn-impl/export-symbols tech.ml.dataset.base
                        dataset-name
                        set-dataset-name
                        metadata
                        set-metadata
                        maybe-column
                        column
                        columns
                        column-map
                        column-names
                        columns-with-missing-seq
                        add-column
                        new-column
                        remove-column
                        remove-columns
                        update-column
                        order-column-names
                        update-columns
                        select
                        select-columns
                        add-or-update-column
                        ds-group-by
                        ds-sort-by
                        ds-filter
                        ds-concat
                        ds-take-nth
                        ds-map-values
                        ds-column-map
                        ->dataset
                        from-prototype)


(defn n-permutations
  "Return n datasets with all permutations n of the columns possible.
  N must be less than (count (columns dataset))."
  [dataset n]
  (when-not (< n (first (dtype/shape dataset)))
    (throw (ex-info (format "%d permutations of %d columns"
                            n (first (dtype/shape dataset))))))
  (->> (comb/combinations (column-names dataset) n)
       (map (partial select-columns dataset))))


(defn n-feature-permutations
  "Given a dataset with at least one inference target column, produce all datasets
  with n feature columns and the label columns."
  [dataset n]
  (let [label-columns (col-filters/target? dataset)
        feature-columns (col-filters/not label-columns dataset)]
    (when-not (seq label-columns)
      (throw (ex-info "No label columns indicated" {})))
    (->> (n-permutations (select-columns dataset feature-columns) n)
         (map
          #(ds-proto/from-prototype
            dataset
            (dataset-name dataset)
            (concat
             (columns %)
             (columns (select-columns dataset label-columns))))))))


(fn-impl/export-symbols tech.ml.dataset.modelling
                        set-inference-target
                        column-label-map
                        inference-target-label-map
                        dataset-label-map
                        inference-target-label-inverse-map
                        num-inference-classes
                        feature-ecount
                        model-type
                        column-values->categorical
                        reduce-column-names
                        has-column-label-map?
                        ->k-fold-datasets
                        ->train-test-split
                        ->row-major)


(fn-impl/export-symbols tech.ml.dataset.math
                        correlation-table
                        k-means
                        g-means
                        x-means
                        compute-centroid-and-global-means
                        impute-missing-by-centroid-averages)



(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown on
  missing values or if nil should be inserted in the map.  IF a label map is passed in
  then for the columns that are present in the label map a reverse mapping is done such
  that the flyweight maps contain the labels and not their encoded values."
  [dataset & {:keys [column-name-seq
                     error-on-missing-values?
                     number->string?]
              :or {error-on-missing-values? true}}]
  (let [label-map (when number->string?
                    (dataset-label-map dataset))
        target-columns-and-vals
        (->> (or column-name-seq
                 (->> (columns dataset)
                      (map ds-col/column-name)
                      ((fn [colname-seq]
                         (if number->string?
                           (reduce-column-names dataset colname-seq)
                           colname-seq)))))
             (map (fn [colname]
                    {:column-name colname
                     :column-values
                     (if (contains? label-map colname)
                       (let [retval
                             (categorical/column-values->categorical
                              dataset colname label-map)]
                         retval)
                       (let [current-column (column dataset colname)]
                         (when (and error-on-missing-values?
                                    (not= 0 (count (ds-col/missing current-column))))
                           (throw (ex-info (format "Column %s has missing values"
                                                   (ds-col/column-name current-column))
                                           {})))
                         (dtype/->reader current-column)))})))]
    ;;Transpose the sequence of columns into a sequence of rows
    (->> target-columns-and-vals
         (map :column-values)
         (apply interleave)
         (partition (count target-columns-and-vals))
         ;;Move to flyweight
         (map zipmap
              (repeat (map :column-name target-columns-and-vals))))))


(defn labels
  "Given a dataset and an options map, generate a sequence of label-values.
  If label count is 1, then if there is a label-map associated with column
  generate sequence of labels by reverse mapping the column(s) back to the original
  dataset values.  If there are multiple label columns results are presented in
  flyweight (sequence of maps) format."
  [dataset]
  (when-not (seq (col-filters/target? dataset))
    (throw (ex-info "No label columns indicated" {})))
  (let [original-label-column-names (->> (col-filters/inference? dataset)
                                         (reduce-column-names dataset))
        flyweight-labels (->flyweight dataset
                                      :column-name-seq original-label-column-names
                                      :number->string? true)]
    (if (= 1 (count original-label-column-names))
      (map #(get % (first original-label-column-names)) flyweight-labels)
      flyweight-labels)))


(defn dataset->string
  ^String [ds]
  (with-out-str
    ((parallel-req/require-resolve 'tech.ml.dataset.print/print-dataset) ds)))
