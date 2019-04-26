(ns tech.ml.dataset.modelling
  (:require [tech.v2.datatype :as dtype]
            [tech.ml.dataset.base
             :refer [column-names update-columns column columns
                     ->dataset select ds-map-values]
             :as ds-base]
            [tech.ml.dataset.column-filters :as col-filters]
            [tech.ml.dataset.column :as ds-col]
            [clojure.set :as c-set]
            [tech.ml.dataset.categorical :as categorical]))


(defn set-inference-target
  [dataset target-name-or-target-name-seq]
  (let [target (->> (if (sequential? target-name-or-target-name-seq)
                      target-name-or-target-name-seq
                      [target-name-or-target-name-seq])
                    set)]
    (update-columns dataset (column-names dataset)
                    (fn [col]
                      (ds-col/set-metadata
                       col
                       (assoc (ds-col/metadata col)
                              :column-type
                              (if (contains? target (ds-col/column-name col))
                                :inference
                                :feature)))))))


(defn column-label-map
  [dataset column-name]
  (-> (column dataset column-name)
      (ds-col/metadata)
      :label-map))


(defn has-column-label-map?
  [dataset column-name]
  (boolean (column-label-map dataset column-name)))


(defn inference-target-label-map
  [dataset]
  (let [label-columns (col-filters/inference? dataset)]
    (when-not (= 1 (count label-columns))
      (throw (ex-info (format "Multiple label columns found: %s" label-columns)
                      {:label-columns label-columns})))
    (column-label-map dataset (first label-columns))))


(defn dataset-label-map
  [dataset]
  (->> (columns dataset)
       (filter (comp :label-map ds-col/metadata))
       (map (juxt ds-col/column-name
                  (comp :label-map ds-col/metadata)))
       (into {})))


(defn inference-target-label-inverse-map
  "Given options generated during ETL operations and annotated with :label-columns
  sequence container 1 label column, generate a reverse map that maps from a dataset
  value back to the label that generated that value."
  [dataset]
  (c-set/map-invert (inference-target-label-map dataset)))


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
  (count (col-filters/feature? dataset)))


(defn model-type
  "Check the label column after dataset processing.
  Return either
  :regression
  :classification"
  [dataset & [column-name-seq]]
  (->> (or column-name-seq (col-filters/inference? dataset))
       (map (juxt identity
                  (fn [colname]
                    (let [col-metadata (-> (column dataset colname)
                                           ds-col/metadata)]
                      (cond
                        (:categorical? col-metadata) :classification
                        :else
                        :regression)))))
       (into {})))


(defn column-values->categorical
  "Given a column encoded via either string->number or one-hot, reverse
  map to the a sequence of the original string column values."
  [dataset src-column]
  (categorical/column-values->categorical
   dataset src-column (dataset-label-map dataset)))


(defn reduce-column-names
  "Reverse map from the one-hot encoded columns
  to the original source column."
  [dataset colname-seq]
  (let [colname-set (set colname-seq)
        reverse-map (->> (dataset-label-map dataset)
                         (mapcat (fn [[colname colmap]]
                                   ;;If this is one hot *and* every one hot is represented in the
                                   ;;column name sequence, then we can recover the original column.
                                   (when (and (categorical/is-one-hot-label-map? colmap)
                                              (every? colname-set (->> colmap
                                                                       vals
                                                                       (map first))))
                                     (->> (vals colmap)
                                          (map (fn [[derived-col col-idx]]
                                                 [derived-col colname]))))))
                         (into {}))]
    (->> colname-seq
         (map (fn [derived-name]
                (if-let [original-name (get reverse-map derived-name)]
                  original-name
                  derived-name)))
         distinct)))


(defn ->k-fold-datasets
  "Given 1 dataset, prepary K datasets using the k-fold algorithm.
  Randomize dataset defaults to true which will realize the entire dataset
  so use with care if you have large datasets."
  [dataset k {:keys [randomize-dataset?]
              :or {randomize-dataset? true}
              :as options}]
  (let [dataset (->dataset dataset options)
        [n-cols n-rows] (dtype/shape dataset)
        indexes (cond-> (range n-rows)
                  randomize-dataset? shuffle)
        fold-size (inc (quot (long n-rows) k))
        folds (vec (partition-all fold-size indexes))]
    (for [i (range k)]
      {:test-ds (select dataset :all (nth folds i))
       :train-ds (select dataset :all (->> (keep-indexed #(if (not= %1 i) %2) folds)
                                           (apply concat )))})))


(defn ->train-test-split
  [dataset {:keys [randomize-dataset? train-fraction]
            :or {randomize-dataset? true
                 train-fraction 0.7}
            :as options}]
  (let [dataset (->dataset dataset options)
        [n-cols n-rows] (dtype/shape dataset)
        indexes (cond-> (range n-rows)
                  randomize-dataset? shuffle)
        n-elems (long n-rows)
        n-training (long (Math/round (* n-elems (double train-fraction))))]
    {:train-ds (select dataset :all (take n-training indexes))
     :test-ds (select dataset :all (drop n-training indexes))}))


(defn ->row-major
  "Given a dataset and a map if desired key names to sequences of columns,
  produce a sequence of maps where each key name points to contiguous vector
  composed of the column values concatenated.
  If colname-seq-map is not provided then each row defaults to
  {:features [feature-columns]
   :label [label-columns]}"
  ([dataset key-colname-seq-map {:keys [datatype]
                                 :or {datatype :float64}}]
   (let [key-val-seq (seq key-colname-seq-map)
         all-col-names (mapcat second key-val-seq)
         item-col-count-map (->> key-val-seq
                                 (map (fn [[item-k item-col-seq]]
                                        (when (seq item-col-seq)
                                          [item-k (count item-col-seq)])))
                                 (remove nil?)
                                 vec)]
     (ds-map-values
      dataset
      (fn [& column-values]
        (->> item-col-count-map
             (reduce (fn [[flyweight column-values] [item-key item-count]]
                       (let [contiguous-array (dtype/make-array-of-type
                                               datatype (take item-count
                                                              column-values))]
                         (when-not (= (dtype/ecount contiguous-array)
                                      (long item-count))
                           (throw
                            (ex-info "Failed to get correct number of items"
                                     {:item-key item-key})))
                         [(assoc flyweight item-key contiguous-array)
                          (drop item-count column-values)]))
                     [{} column-values])
             first))
      all-col-names)))
  ([dataset options]
   (->row-major dataset (merge {:features (col-filters/feature? dataset)}
                               (when-let [label-colnames (col-filters/inference? dataset)]
                                 {:label label-colnames}))
                options)))
