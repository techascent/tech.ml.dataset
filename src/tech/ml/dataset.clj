(ns tech.ml.dataset
  "Column major dataset abstraction for efficiently manipulating
  in memory datasets."
  (:require [tech.v2.datatype :as dtype]
            [tech.parallel.utils :as par-util]
            [tech.v2.datatype.functional :as dfn]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.pipeline.column-filters :as col-filters]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.base]
            [tech.ml.dataset.modelling]
            [tech.ml.dataset.math]
            [tech.v2.datatype.casting :as casting]
            [clojure.math.combinatorics :as comb])
  (:import [smile.clustering KMeans GMeans XMeans PartitionClustering])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth]))


(set! *warn-on-reflection* true)

(par-util/export-symbols tech.ml.dataset.base
                        dataset-name
                        set-dataset-name
                        ds-row-count
                        row-count
                        ds-column-count
                        column-count
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
                        rename-columns
                        select
                        select-columns
                        add-or-update-column
                        value-reader
                        mapseq-reader
                        ds-group-by
                        ds-group-by-column
                        group-by->indexes
                        group-by-column->indexes
                        sort-by
                        sort-by-column
                        ds-sort-by
                        ds-sort-by-column
                        filter
                        filter-column
                        ds-filter
                        ds-filter-column
                        unique-by
                        unique-by-column
                        aggregate-by
                        aggregate-by-column
                        ds-concat
                        concat
                        ds-take-nth
                        take-nth
                        ->dataset
                        ->>dataset
                        from-prototype
                        dataset->string
                        join-by-column)


(par-util/export-symbols tech.ml.dataset.impl.dataset
                         new-dataset
                         name-values-seq->dataset)


(defn n-permutations
  "Return n datasets with all permutations n of the columns possible.
  N must be less than (count (columns dataset))."
  [dataset n]
  (when-not (< n (first (dtype/shape dataset)))
    (throw (ex-info (format "%d permutations of %d columns"
                            n (first (dtype/shape dataset)))
                    {})))
  (->> (comb/combinations (column-names dataset) n)
       (map set)
       ;;assume order doesn't matter
       distinct
       (map (partial select-columns dataset))))


(defn n-feature-permutations
  "Given a dataset with at least one inference target column, produce all datasets
  with n feature columns and the label columns."
  [dataset n]
  (let [label-columns (col-filters/target? dataset)
        feature-columns (col-filters/not label-columns dataset)]
    (when-not (seq label-columns)
      (throw (ex-info "No label columns indicated" {})))
    (->> (comb/combinations feature-columns n)
         (map set)
         ;;assume order doesn't matter
         distinct
         (map (comp (partial select-columns dataset)
                    (partial clojure.core/concat label-columns))))))


(par-util/export-symbols tech.ml.dataset.modelling
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


(par-util/export-symbols tech.ml.dataset.math
                        correlation-table
                        k-means
                        g-means
                        x-means
                        compute-centroid-and-global-means
                        impute-missing-by-centroid-averages)


(defn descriptive-stats
  "Get descriptive statistics across the columns of the dataset.
  In addition to the standard stats"
  [dataset]
  (let [stat-names [:col-name :datatype :n-valid :n-missing
                    :mean :mode :min :max :standard-deviation :skew]
        stats-ds
        (->> (->dataset dataset)
             (pmap (fn [ds-col]
                     (let [n-missing (count (ds-col/missing ds-col))
                           n-valid (- (dtype/ecount ds-col)
                                      n-missing)
                           col-dtype (dtype/get-datatype ds-col)
                           col-reader (dtype/->reader ds-col
                                                      col-dtype
                                                      {:missing-policy :elide})]
                       (merge
                        {:col-name (ds-col/column-name ds-col)
                         :datatype col-dtype
                         :n-valid n-valid
                         :n-missing n-missing}
                        (if (and (not (:categorical? (ds-col/metadata ds-col)))
                                 (casting/numeric-type? col-dtype))
                          (dfn/descriptive-stats col-reader
                                                 #{:min :mean :max
                                                   :standard-deviation :skew})
                          {:mode (->> col-reader
                                      frequencies
                                      (clojure.core/sort-by second >)
                                      ffirst)})))))
             (clojure.core/sort-by :col-name)
             ->dataset)
        existing-colname-set (->> (column-names stats-ds)
                                  set)]
    ;;This orders the columns by the ordering of stat-names but if for instance
    ;;there were no numeric or no string columns it still works.
    (select-columns stats-ds (->> stat-names
                                  (clojure.core/filter existing-colname-set)))))


(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown
  on missing values or if nil should be inserted in the map.  IF a label map is passed
  in then for the columns that are present in the label map a reverse mapping is done
  such that the flyweight maps contain the labels and not their encoded values."
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
