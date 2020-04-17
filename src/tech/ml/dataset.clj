(ns tech.ml.dataset
  "Column major dataset abstraction for efficiently manipulating
  in memory datasets."
  (:require [tech.v2.datatype :as dtype]
            [tech.parallel.utils :as par-util]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.datetime :as dtype-dt]
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
                        has-column?
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
                        drop-columns
                        select-rows
                        drop-rows
                        missing
                        add-or-update-column
                        value-reader
                        mapseq-reader
                        ds-group-by
                        ds-group-by-column
                        group-by->indexes
                        group-by-column->indexes
                        group-by
                        group-by-column
                        sort-by
                        sort-by-column
                        ds-sort-by
                        ds-sort-by-column
                        ->sort-by
                        ->sort-by-column
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
                        write-csv!)


(par-util/export-symbols tech.ml.dataset.join
                         hash-join
                         inner-join
                         right-join
                         left-join)


(par-util/export-symbols tech.ml.dataset.impl.dataset
                         new-dataset
                         name-values-seq->dataset)


(defn column->dataset
  "Transform a column into a sequence of maps using transform-fn.
  Return dataset created out of the sequence of maps."
  ([dataset colname transform-fn options]
   (->> (pmap transform-fn (dataset colname))
        (->>dataset options)))
  ([dataset colname transform-fn]
   (column->dataset dataset colname transform-fn {})))


(defn append-columns
  [dataset column-seq]
  (new-dataset (dataset-name dataset)
               (metadata dataset)
               (clojure.core/concat (columns dataset) column-seq)))


(defn column-labeled-mapseq
  "Given a dataset, return a sequence of maps where several columns are all stored
  in a :value key and a :label key contains a column name.  Used for quickly creating
  timeseries or scatterplot labeled graphs.  Returns a lazy sequence, not a reader!
  Return a sequence of maps with
  {... - columns not in colname-seq
   :value - value from one of the value columns
   :label - name of the column the value came from
  }"
  [dataset value-colname-seq]
  (let [colname-set (set value-colname-seq)
        untouched-cols (remove colname-set (column-names dataset))]
    ;;Ensure the value columns actually exist.
    (select-columns dataset value-colname-seq)
    (->> (mapseq-reader dataset)
         (mapcat (fn [data]
                   (let [mindata (select-keys data untouched-cols)]
                     (->> value-colname-seq
                          (map (fn [colname]
                                 (assoc mindata
                                        :value (get data colname)
                                        :label colname))))))))))


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
                        impute-missing-by-centroid-averages
                        loess-interpolate)


(defn descriptive-stats
  "Get descriptive statistics across the columns of the dataset.
  In addition to the standard stats"
  [dataset]
  (let [stat-names [:col-name :datatype :n-valid :n-missing
                    :mean :mode :min :max :standard-deviation :skew]
        stats-ds
        (->> (->dataset dataset)
             (pmap (fn [ds-col]
                     (let [n-missing (dtype/ecount (ds-col/missing ds-col))
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
                        (cond
                          (dtype-dt/datetime-datatype? col-dtype)
                          (dtype-dt-ops/millisecond-descriptive-stats col-reader)
                          (and (not (:categorical? (ds-col/metadata ds-col)))
                               (casting/numeric-type? col-dtype))
                          (dfn/descriptive-stats col-reader
                                                 #{:min :mean :max
                                                   :standard-deviation :skew})
                          :else
                          {:mode (->> col-reader
                                      frequencies
                                      (clojure.core/sort-by second >)
                                      ffirst)})))))
             (clojure.core/sort-by (comp str :col-name))
             ->dataset)
        existing-colname-set (->> (column-names stats-ds)
                                  set)]
    ;;This orders the columns by the ordering of stat-names but if for instance
    ;;there were no numeric or no string columns it still works.
    (-> stats-ds
        (select-columns (->> stat-names
                             (clojure.core/filter existing-colname-set)))
        (set-dataset-name (str (dataset-name dataset) ": descriptive-stats")))))


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
                                    (not= 0 (dtype/ecount
                                             (ds-col/missing current-column))))
                           (throw (ex-info (format "Column %s has missing values"
                                                   (ds-col/column-name current-column))
                                           {})))
                         (dtype/->reader current-column :object)))})))]
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
