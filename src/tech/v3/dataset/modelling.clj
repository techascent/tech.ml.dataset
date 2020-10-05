(ns ^:no-doc tech.v3.dataset.modelling
  "Methods related specifically to machine learning such as setting
  the inference target."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.base
             :refer [column-names update-columns
                     select]
             :as ds-base]
            [tech.v3.dataset.column :as ds-col]
            [clojure.set :as c-set]
            [tech.v3.dataset.categorical :as categorical])
  (:import [tech.v3.datatype ObjectReader]))


(declare dataset-label-map reduce-column-names)


(defn inference-column?
  [col]
  (= :inference (:column-type (meta col))))


(defn set-inference-target
  [dataset target-name-or-target-name-seq]
  (let [label-map (dataset-label-map dataset)
        target (->> (if (sequential? target-name-or-target-name-seq)
                      target-name-or-target-name-seq
                      [target-name-or-target-name-seq])
                    ;;expand column names after 1-hot mapping
                    (mapcat (fn [colname]
                              (let [col-label-map (get label-map colname)]
                                (if (and col-label-map
                                         (categorical/is-one-hot-label-map?
                                          col-label-map))
                                  (->> (vals col-label-map)
                                       (map first))
                                  [colname]))))
                    set)]
    (update-columns dataset (column-names dataset)
                    (fn [col]
                      (vary-meta col assoc
                                 :column-type (if (contains? target (ds-col/column-name col))
                                                :inference
                                                :feature))))))

(defn inference-target-column-names
  [ds]
  (->> (map meta ds)
       (filter #(= :inference (:column-type %)))
       (map :name)))


(defn column-label-map
  [dataset column-name]
  (get-in (metadata dataset) [:label-map column-name]))


(defn has-column-label-map?
  [dataset column-name]
  (boolean (column-label-map dataset column-name)))


(defn inference-target-label-map
  [dataset & [label-columns]]
  (let [label-columns (or label-columns (col-filters/inference? dataset))]
    (when-not (= 1 (count label-columns))
      (throw (ex-info (format "Multiple label columns found: %s" label-columns)
                      {:label-columns label-columns})))
    (column-label-map dataset (first label-columns))))


(defn dataset-label-map
  [dataset]
  (get (metadata dataset) :label-map))


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
  (count (col-filters/feature? dataset)))


(defn model-type
  "Check the label column after dataset processing.
  Return either
  :regression
  :classification"
  [dataset & [column-name-seq]]
  (let [col-label-map (dataset-label-map dataset)]
    (->> (or column-name-seq (col-filters/inference? dataset))
         (reduce-column-names dataset)
         (map (juxt identity
                    (fn [colname]
                      (if-let [column-data (maybe-column dataset colname)]
                        (let [col-metadata (ds-col/metadata column-data)]
                          (cond
                            (:categorical? col-metadata) :classification
                            :else
                            :regression))
                        (if (contains? col-label-map colname)
                          :classification
                          :regression)))))
         (into {}))))


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
        reverse-map
        (->> (dataset-label-map dataset)
             (mapcat (fn [[colname colmap]]
                       ;;If this is one hot *and* every one hot is represented in the
                       ;;column name sequence, then we can recover the original column.
                       (when (and (categorical/is-one-hot-label-map? colmap)
                                  (every? colname-set (->> colmap
                                                           vals
                                                           (map first))))
                         (->> (vals colmap)
                              (map (fn [[derived-col _col-idx]]
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
  ([dataset k {:keys [randomize-dataset?]
               :or {randomize-dataset? true}
               :as options}]
   (let [dataset (->dataset dataset options)
         [_n-cols n-rows] (dtype/shape dataset)
         indexes (cond-> (range n-rows)
                   randomize-dataset? shuffle)
         fold-size (inc (quot (long n-rows) k))
         folds (vec (partition-all fold-size indexes))]
     (for [i (range k)]
       {:test-ds (select dataset :all (nth folds i))
        :train-ds (select dataset :all (->> (keep-indexed #(if (not= %1 i) %2) folds)
                                            (apply concat )))})))
  ([dataset k]
   (->k-fold-datasets dataset k {})))


(defn ->train-test-split
  ([dataset {:keys [randomize-dataset? train-fraction]
             :or {randomize-dataset? true
                  train-fraction 0.7}
             :as options}]
   (let [dataset (->dataset dataset options)
         [_n-cols n-rows] (dtype/shape dataset)
         indexes (cond-> (range n-rows)
                   randomize-dataset? shuffle)
         n-elems (long n-rows)
         n-training (long (Math/round (* n-elems
                                         (double train-fraction))))]
     {:train-ds (select dataset :all (take n-training indexes))
      :test-ds (select dataset :all (drop n-training indexes))}))
  ([dataset]
   (->train-test-split dataset {})))


(defn ->row-major
  "Given a dataset and a map of desired key names to sequences of columns,
  produce a sequence of maps where each key name points to contiguous vector
  composed of the column values concatenated.
  If colname-seq-map is not provided then each row defaults to
  {:features [feature-columns]
   :label [label-columns]}"
  ([dataset key-colname-seq-map {:keys [datatype]
                                 :or {datatype :float64}}]
   (let [key-reader-seq
         (->> (seq key-colname-seq-map)
              (map (fn [[k v]]
                     (when (seq v)
                       [k (->> v
                               (mapv #(-> (ds-base/column dataset %)
                                          (dtype/->reader datatype))))])))
              (remove nil?)
              vec)
         n-elems (long (ds-base/row-count dataset))]
     (reify ObjectReader
       (lsize [rdr] n-elems)
       (read [rdr idx]
         (->> key-reader-seq
              (map (fn [[k v]]
                     [k (dtype/make-container
                         :java-array datatype
                         (mapv #(% idx) v))]))
              (into {}))))))
  ([dataset options]
   (->row-major dataset (merge
                         {:features (col-filters/feature? dataset)}
                         (when-let [label-colnames (col-filters/inference? dataset)]
                           {:label label-colnames}))
                options))
  ([dataset]
   (->row-major dataset {})))

(defn reverse-map-categorical-columns
  "Given a dataset where we have converted columns from a categorical representation
  to either a numeric reprsentation or a one-hot representation, reverse map
  back to the original dataset given the reverse mapping of label->number in
  the column's metadata."
  [dataset {:keys [column-name-seq]}]
  (let [label-map (dataset-label-map dataset)
        column-name-seq (or column-name-seq
                            (column-names dataset))
        column-name-seq (reduce-column-names dataset column-name-seq)
        dataset
        (clojure.core/reduce
         (fn [dataset colname]
           (if (contains? label-map colname)
             (add-or-update-column dataset colname
                                   (categorical/column-values->categorical
                                    dataset colname label-map))
             dataset))
         dataset
         column-name-seq)]
    (select-columns dataset column-name-seq)))


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
