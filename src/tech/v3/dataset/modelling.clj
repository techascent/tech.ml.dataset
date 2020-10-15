(ns tech.v3.dataset.modelling
  "Methods related specifically to machine learning such as setting the inference
  target.  This file integrates tightly with tech.v3.dataset.categorical which provides
  categorical -> number and one-hot transformation pathways"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.categorical :as categorical]
            [clojure.set :as c-set])
  (:import [tech.v3.datatype ObjectReader]
           [java.util List]))


(set! *warn-on-reflection* true)


(defn inference-column?
  [col]
  (:inference-target? (meta col)))


(defn set-inference-target
  "Set the inference target on the column.  This sets the :column-type member
  of the column metadata to :inference-target?."
  [dataset target-name-or-target-name-seq]
  (let [colnames (if (sequential? target-name-or-target-name-seq)
                   target-name-or-target-name-seq
                   [target-name-or-target-name-seq])]
    (ds-base/update-columns dataset colnames
                            #(vary-meta % assoc :inference-target? true))))


(defn inference-target-column-names
  "Return the names of the columns that are inference targets."
  [ds]
  (->> (map meta (vals ds))
       (filter :inference-target?)
       (map :name)
       (seq)))


(defn inference-target-label-map
  [dataset & [label-columns]]
  (let [label-columns (or label-columns (inference-target-column-names dataset))]
    (errors/when-not-errorf
     (= 1 (count label-columns))
     "Multiple label columns found: %s" label-columns)
    (-> (ds-base/column dataset (first label-columns))
        (meta)
        (get-in [:categorical-map :lookup-table]))))


(defn inference-target-label-inverse-map
  "Given options generated during ETL operations and annotated with :label-columns
  sequence container 1 label column, generate a reverse map that maps from a dataset
  value back to the label that generated that value."
  [dataset & [label-columns]]
  (c-set/map-invert (inference-target-label-map dataset label-columns)))



(defn dataset->categorical-xforms
  "Given a dataset, return a map of column-name->xform information."
  [ds]
  (->> (concat (categorical/dataset->categorical-maps ds)
               (categorical/dataset->one-hot-maps ds))
       (map (juxt :src-column identity))
       (into {})))


(defn num-inference-classes
  "Given a dataset and correctly built options from pipeline operations,
  return the number of classes used for the label.  Error if not classification
  dataset."
  ^long [dataset]
  (count (inference-target-label-map dataset)))


(defn feature-ecount
  "Number of feature columns.  Feature columns are columns that are not
  inference targets."
  ^long [dataset]
  (count (remove #(= :inference-target? (meta %))
                 (vals dataset))))


(defn model-type
  "Check the label column after dataset processing.
  Return either
  :regression
  :classification"
  [dataset & [column-name-seq]]
  (->> (or column-name-seq
           (inference-target-column-names dataset))
       (ds-base/select-columns dataset)
       (vals)
       (map (fn [column]
              (let [colmeta (meta column)]
                [(:name colmeta) (if (:categorical? colmeta)
                                   :classification
                                   :regression)])))
       (into {})))


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
      (-> (categorical/invert-one-hot-map dataset one-hot-map)
          (ds-base/column src-column))
      (errors/throwf
       "Column %s does not appear to have either a categorical or one hot map"
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
       {:test-ds (ds-base/select-rows dataset
                                      (nth folds i))
        :train-ds (ds-base/select-rows dataset
                                       (->> (keep-indexed #(if (not= %1 i) %2) folds)
                                            (apply concat )))})))
  ([dataset k]
   (k-fold-datasets dataset k {})))


(defn train-test-split
  "Probabilistically split the dataset returning a map of `{:train-ds :test-ds}`."
  ([dataset {:keys [randomize-dataset? train-fraction]
             :or {randomize-dataset? true
                  train-fraction 0.7}}]
   (let [[_n-cols n-rows] (dtype/shape dataset)
         indexes (cond-> (range n-rows)
                   randomize-dataset? shuffle)
         n-elems (long n-rows)
         n-training (long (Math/round (* n-elems
                                         (double train-fraction))))]
     {:train-ds (ds-base/select-rows dataset (take n-training indexes))
      :test-ds (ds-base/select-rows dataset (drop n-training indexes))}))
  ([dataset]
   (train-test-split dataset {})))


(defn inference-target-ds
  "Given a dataset return reverse-mapped inference target columns or nil
  in the case where there are no inference targets."
  [dataset]
  (when-let [target-cols (inference-target-column-names dataset)]
    (-> (ds-base/select-columns dataset target-cols)
        (categorical/reverse-map-categorical-xforms))))


(defn labels
  "Return the labels.  The labels sequence is the reverse mapped inference
  column.  This returns a single column of data or errors out."
  [dataset]
  (let [
        rev-mapped (inference-target-ds dataset)]
    (errors/when-not-errorf
        (== 1 (ds-base/column-count rev-mapped))
      "Incorrect number of columns (%d) in dataset for labels transformation"
      (ds-base/column-count rev-mapped))
    rev-mapped))


(defn probability-distributions->label-column
  "Given a dataset that has columns in which the column names describe labels and the rows describe
  a probability distribution, create a label column by taking the max value in each row and assign
  column that row value."
  [prob-ds dst-colname]
  (let [^List cnames (vec (ds-base/column-names prob-ds))
        row-rdr (ds-readers/value-reader prob-ds)
        col-dtype (reduce casting/widest-datatype
                          (map dtype/elemwise-datatype cnames))]
    (assoc prob-ds dst-colname
           (dtype/emap #(cnames (argops/argmax %))
                       col-dtype
                       row-rdr))))
