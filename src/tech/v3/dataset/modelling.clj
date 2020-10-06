(ns ^:no-doc tech.v3.dataset.modelling
  "Methods related specifically to machine learning such as setting
  the inference target."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.base
             :refer [column-names update-columns
                     select]
             :as ds-base]
            [tech.v3.dataset.column :as ds-col]
            [clojure.set :as c-set]
            [tech.v3.dataset.categorical :as categorical])
  (:import [tech.v3.datatype ObjectReader]))


(defn inference-column?
  [col]
  (= :inference (:column-type (meta col))))


(defn set-inference-target
  [dataset target-name-or-target-name-seq]
  (let [colnames (if (sequential? target-name-or-target-name-seq)
                   target-name-or-target-name-seq
                   [target-name-or-target-name-seq])]
    (ds-base/update-columns dataset colnames
                            #(vary-meta % assoc :column-type :inference))))


(defn inference-target-column-names
  [ds]
  (->> (map meta (vals ds))
       (filter #(= :inference (:column-type %)))
       (map :name)))


(defn inference-target-label-map
  [dataset & [label-columns]]
  (let [label-columns (or label-columns (inference-target-column-names dataset))]
    (errors/when-not-errorf
     (= 1 (count label-columns))
     "Multiple label columns found: %s" label-columns)
    (-> (ds-base/column dataset (first label-columns))
        (meta)
        (get-in [:categorical-map :lookup-table]))))


(defn dataset-label-map
  [dataset]
  (->> (vals dataset)
       (map meta)
       (filter :categorical-map)
       (map (juxt :name #(get-in % [:categorical-map :lookup-table])))
       (into {})))


(defn inference-target-label-inverse-map
  "Given options generated during ETL operations and annotated with :label-columns
  sequence container 1 label column, generate a reverse map that maps from a dataset
  value back to the label that generated that value."
  [dataset & [label-columns]]
  (c-set/map-invert (inference-target-label-map dataset label-columns)))


(defn num-inference-classes
  "Given a dataset and correctly built options from pipeline operations,
  return the number of classes used for the label.  Error if not classification
  dataset."
  ^long [dataset]
  (count (inference-target-label-map dataset)))


(defn feature-ecount
  "When columns aren't scalars then this will change.
  For now, just the number of feature columns."
  ^long [dataset]
  (count (remove #(= :inference (:column-type (meta %)))
                 (vals dataset))))


(defn model-type
  "Check the label column after dataset processing.
  Return either
  :regression
  :classification"
  [dataset & [column-name-seq]]
  (if (->> (or column-name-seq
               (inference-target-column-names dataset))
           (ds-base/select-columns dataset)
           (vals)
           (map meta)
           (filter :categorical?)
           seq)
    :classification
    :regression))


(defn column-values->categorical
  "Given a column encoded via either string->number or one-hot, reverse
  map to the a sequence of the original string column values.
  In the case of one-hot mappings, src-column must be the original
  column name before the one-hot map"
  [dataset src-column]
  (if-let [cmap (->> (categorical/dataset->categorical-maps dataset)
                     (filter #(= src-column (:src-column %)))
                     (first))]
    (-> (categorical/invert-categorical-map dataset cmap)
        (ds-base/column src-column))
    (if-let [one-hot-map (->> (categorical/dataset->one-hot-maps dataset)
                              (filter #(= src-column (:src-column %)))
                              (first))]
      (-> (categorical/invert-one-hot dataset one-hot-map)
          (ds-base/column src-column))
      (errors/throwf "Column %s does not appear to have either a categorical or one hot map"
                     src-column))))


(defn k-fold-datasets
  "Given 1 dataset, prepary K datasets using the k-fold algorithm.
  Randomize dataset defaults to true which will realize the entire dataset
  so use with care if you have large datasets."
  ([dataset k {:keys [randomize-dataset?]
               :or {randomize-dataset? true}}]
   (let [[_n-cols n-rows] (dtype/shape dataset)
         indexes (cond-> (range n-rows)
                   randomize-dataset? shuffle)
         fold-size (inc (quot (long n-rows) k))
         folds (vec (partition-all fold-size indexes))]
     (for [i (range k)]
       {:test-ds (select dataset :all (nth folds i))
        :train-ds (select dataset :all (->> (keep-indexed #(if (not= %1 i) %2) folds)
                                            (apply concat )))})))
  ([dataset k]
   (k-fold-datasets dataset k {})))


(defn train-test-split
  ([dataset {:keys [randomize-dataset? train-fraction]
             :or {randomize-dataset? true
                  train-fraction 0.7}}]
   (let [[_n-cols n-rows] (dtype/shape dataset)
         indexes (cond-> (range n-rows)
                   randomize-dataset? shuffle)
         n-elems (long n-rows)
         n-training (long (Math/round (* n-elems
                                         (double train-fraction))))]
     {:train-ds (select dataset :all (take n-training indexes))
      :test-ds (select dataset :all (drop n-training indexes))}))
  ([dataset]
   (train-test-split dataset {})))


(defn reverse-map-categorical-columns
  "Given a dataset where we have converted columns from a categorical representation
  to either a numeric reprsentation or a one-hot representation, reverse map
  back to the original dataset given the reverse mapping of label->number in
  the column's metadata."
  [dataset]
  (->> (concat (categorical/dataset->categorical-maps dataset)
               (categorical/dataset->one-hot-maps dataset))
       (reduce (fn [dataset cat-map]
                 ))
       )


  (select-columns dataset column-name-seq))


(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown
  on missing values or if nil should be inserted in the map.  If the dataset has a label
  and number->string? is true then columns that have been converted from categorical to
  numeric will be reverse-mapped back to string columns."
  [dataset & {:keys [column-name-seq
                     error-on-missing-values?
                     number->string?]
              :or {error-on-missing-values? true}}]
  (let [column-name-seq (or column-name-seq
                            (column-names dataset))
        dataset (if number->string?
                  (reverse-map-categorical-columns dataset {:column-name-seq
                                                            column-name-seq})
                  (select-columns dataset column-name-seq))]
    (when-let [missing-columns
               (when error-on-missing-values?
                 (seq (columns-with-missing-seq dataset)))]
      (throw (Exception. (format "Columns with missing data detected: %s"
                                 missing-columns))))
    (mapseq-reader dataset)))


(defn labels
  "Given a dataset and an options map, generate a sequence of label-values.
  If label count is 1, then if there is a label-map associated with column
  generate sequence of labels by reverse mapping the column(s) back to the original
  dataset values.  If there are multiple label columns results are presented in
  a dataset.
  Return a reader of labels"
  [dataset]
  (when-not (seq (col-filters/target? dataset))
    (throw (ex-info "No label columns indicated" {})))
  (let [original-label-column-names (->> (col-filters/inference? dataset)
                                         (reduce-column-names dataset))
        dataset (reverse-map-categorical-columns
                 dataset {:column-name-seq
                          original-label-column-names})]
    (if (= 1 (column-count dataset))
      (dtype/->reader (first (vals dataset)))
      dataset)))
