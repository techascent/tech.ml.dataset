(ns tech.ml.dataset
  "The most simple dataset description we have figured out is a sequence of maps.

  Using this definition, things like k-fold have natural interpretations.

  Care has been taken to keep certain operations lazy so that datasets of unbounded
  length can be manipulated.  Operatings like auto-scaling, however, will read the
  dataset into memory."
  (:require [tech.datatype :as dtype]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.parallel :as parallel]
            [clojure.core.matrix :as m]
            [clojure.set :as c-set]))


(set! *warn-on-reflection* true)


(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn maybe-column
  "Return either column if exists or nil."
  [dataset column-name]
  (ds-proto/maybe-column dataset column-name))


(defn column
  "Return the column or throw if it doesn't exist."
  [dataset column-name]
  (if-let [retval (maybe-column dataset column-name)]
    retval
    (throw (ex-info (format "Failed to find column: %s" column-name)
                    {:column-name column-name}))))

(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (ds-proto/columns dataset))

(defn add-column
  "Add a new column. Error if name collision"
  [dataset column]
  (ds-proto/add-column dataset column))

(defn remove-column
  "Fails quietly"
  [dataset col-name]
  (ds-proto/remove-column dataset col-name))

(defn update-column
  "Update a column returning a new dataset.  update-fn is a column->column
  transformation.  Error if column does not exist."
  [dataset col-name update-fn]
  (ds-proto/update-column dataset col-name update-fn))

(defn add-or-update-column
  "If column exists, replace.  Else append new column."
  [dataset column]
  (ds-proto/add-or-update-column dataset column))


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
colname-seq - either keyword :all or list of column names with no duplicates.
index-seq - either keyword :all or list of indexes.  May contain duplicates."
  [dataset colname-seq index-seq]
  (ds-proto/select dataset colname-seq index-seq))


(defn select-columns
  [dataset col-name-seq]
  (select dataset col-name-seq :all))


(defn index-value-seq
  "Get a sequence of tuples:
  [idx col-value-vec]

Values are in order of column-name-seq.  Duplicate names are allowed and result in
duplicate values."
  [dataset]
  (ds-proto/index-value-seq dataset))


(defn supported-column-stats
  "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."
  [dataset]
  (ds-proto/supported-column-stats dataset))


(defn from-prototype
  "Create a new dataset that is the same type as this one but with a potentially
different table name and column sequence.  Take care that the columns are all of
the correct type."
  [dataset table-name column-seq]
  (ds-proto/from-prototype dataset table-name column-seq))


(defn ds-filter
  [dataset predicate & [column-name-seq]]
  ;;interleave, partition count would also work.
  (->> (index-value-seq (select dataset (or column-name-seq :all) :all))
       (filter (fn [[idx col-values]]
                 (apply predicate col-values)))
       (map first)
       (select dataset :all)))


(defn ds-group-by
  [dataset key-fn & [column-name-seq]]
  (->> (index-value-seq (select dataset (or column-name-seq :all) :all))
       (group-by (fn [[idx col-values]]
                   (apply key-fn col-values)))
       (map first)
       (select dataset :all)))


(defn ds-concat
  [dataset & other-datasets]
  (let [column-list (->> (concat [dataset] (remove nil? other-datasets))
                         (mapcat (fn [dataset]
                                   (->> (columns dataset)
                                        (mapv (fn [col]
                                                (assoc (ds-col/metadata col)
                                                       :column
                                                       col
                                                       :table-name (dataset-name dataset)))))))
                         (group-by :name))]
    (when-not (= 1 (count (->> (vals column-list)
                               (map count)
                               distinct)))
      (throw (ex-info "Dataset is missing a column" {})))
    (->> column-list
         (mapv (fn [[colname columns]]
                 (let [columns (map :column columns)
                       newcol-ecount (apply + 0 (map m/ecount columns))
                       first-col (first columns)
                       new-col (ds-col/new-column first-col
                                                     (dtype/get-datatype first-col)
                                                     newcol-ecount
                                                     (ds-col/metadata first-col))]
                   (dtype/copy-raw->item! (map ds-col/column-values columns)
                                          new-col 0
                                          {:unchecked? true})
                   new-col)))
         (ds-proto/from-prototype dataset (dataset-name dataset)))))


(defn ds-map
  "Note this returns a sequence, not a dataset."
  [dataset map-fn & [column-name-seq]]
  (->> (index-value-seq (select dataset (or column-name-seq :all) :all))
       (map (fn [[idx col-values]]
              (apply map-fn col-values)))))


(defn correlation-table
  "Return a map of colname->list of sorted tuple of [colname, coefficient].
  Sort is:
  (sort-by (comp #(Math/abs (double %)) second) >)

  Thus the first entry is:
  [colname, 1.0]

  There are three possible correlation types:
  :pearson
  :spearman
  :kendall

  :pearson is the default."
  [dataset & [correlation-type]]
  (let [colseq (columns dataset)
        correlation-type (or :pearson correlation-type)]
    (->> (for [lhs colseq]
           [(ds-col/column-name lhs)
            (->> colseq
                 (map (fn [rhs]
                        [(ds-col/column-name rhs)
                         (ds-col/correlation lhs rhs correlation-type)]))
                 (sort-by (comp #(Math/abs (double %)) second) >))])
         (into {}))))


(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown on
  missing values or if nil should be inserted in the map.  IF a label map is passed in
  then for the columns that are present in the label map a reverse mapping is done such
  that the flyweight maps contain the labels and not their encoded values."
  [dataset & {:keys [column-name-seq
                     error-on-missing-values?
                     label-map]
              :or {column-name-seq :all
                   error-on-missing-values? true}}]
  (let [dataset (select dataset column-name-seq :all)
        column-name-seq (map ds-col/column-name (columns dataset))
        inverse-label-map (->> label-map
                               (map (juxt first (comp c-set/map-invert second)))
                               (into {}))
        retval
        (if error-on-missing-values?
          (ds-map dataset (fn [& args]
                            (zipmap column-name-seq args)))
          ;;Much slower algorithm
          (if-let [ds-columns (seq (columns dataset))]
            (let [ecount (long (apply min (map dtype/ecount ds-columns)))
                  columns (columns dataset)]
              (for [idx (range ecount)]
                (->> (for [col columns]
                       [(ds-col/column-name col)
                        (when-not (ds-col/is-missing? col idx)
                          (ds-col/get-column-value col idx))])
                     (remove nil?)
                     (into {}))))))]
    (if (seq inverse-label-map)
      (->> retval
           (map (fn [row-map]
                  (->> inverse-label-map
                       (reduce (fn [row-map [colname value-map]]
                                 (if (contains? row-map colname)
                                   (if-let [mapped-val (get value-map
                                                            (long (get row-map colname)))]
                                     (assoc row-map colname mapped-val)
                                     (throw (ex-info (format "Failed to find column %s value %s in label map %s"
                                                             colname (get row-map colname) value-map) {})))
                                   row-map))
                               row-map)))))
      retval)))


(declare ->dataset)


(defn ->k-fold-datasets
  "Given 1 dataset, prepary K datasets using the k-fold algorithm.
  Randomize dataset defaults to true which will realize the entire dataset
  so use with care if you have large datasets."
  [dataset k {:keys [randomize-dataset?]
              :or {randomize-dataset? true}
              :as options}]
  (let [dataset (->dataset dataset options)
        [n-rows n-cols] (m/shape dataset)
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
        [n-rows n-cols] (m/shape dataset)
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
     (ds-map dataset
             (fn [& column-values]
               (->> item-col-count-map
                    (reduce (fn [[flyweight column-values] [item-key item-count]]
                              (let [contiguous-array (dtype/make-array-of-type datatype (take item-count column-values))]
                                (when-not (= (dtype/ecount contiguous-array)
                                             (long item-count))
                                  (throw (ex-info "Failed to get correct number of items" {:item-key item-key})))
                                [(assoc flyweight item-key contiguous-array)
                                 (drop item-count column-values)]))
                            [{} column-values])
                    first))
             all-col-names)))
  ([dataset options]
   (->row-major dataset (merge {:features (get options :feature-columns)}
                               (when (seq (get options :label-columns))
                                 {:label (get options :label-columns)}))
                options)))




(defn column-name->label-map
  [column-name options]
  (if-let [col-label-map (get-in options [:label-map column-name])]
    col-label-map
    (throw (ex-info (format "Failed to find label map for column %s"
                            column-name)
                    {:label-column column-name
                     :label-map-keys (keys (:label-map options))}))))


(defn options->label-map
  [{:keys [label-columns label-map] :as options}]
  (when-not (= 1 (count label-columns))
    (throw (ex-info (format "Multiple label columns found: %s" label-columns)
                    {:label-columns label-columns})))
  (column-name->label-map (first label-columns) options))


(defn options->label-inverse-map
  "Given options generated during ETL operations and annotated with :label-columns
  sequence container 1 label column, generate a reverse map that maps from a dataset
  value back to the label that generated that value."
  [options]
  (c-set/map-invert (options->label-map options)))


(defn options->num-classes
  "Given a dataset and correctly built options from pipeline operations,
  return the number of classes used for the label.  Error if not classification
  dataset."
  ^long [options]
  (count (options->label-map options)))


(defn options->feature-ecount
  "When columns aren't scalars then this will change.
  For now, just the number of feature columns."
  ^long [options]
  (count (:feature-columns options)))


(defn options->model-type
  "Check the label column after dataset processing.
  Return either
  :regression
  :classification"
  [{:keys [label-columns] :as options}]
  (if (or (not= 1 (count label-columns))
          (nil? (get-in options [:label-map (first label-columns)])))
    :regression
    :classification))


(defn labels
  "Given a dataset and an options map, generate a sequence of labels.
  If label count is 1, then if there is a label-map associated with column
  generate sequence of labels."
  [dataset {:keys [label-columns label-map] :as options}]
  (when-not label-columns
    (throw (ex-info "No label columns indicated" {})))
  (if-let [label-column (when (= (count label-columns) 1)
                          (first label-columns))]
    (let [column-values (-> (column dataset label-column)
                            ds-col/column-values)]
      (if-let [label-map (get label-map label-column)]
        (let [inverse-map (c-set/map-invert label-map)]
          (->> column-values
               (mapv (fn [col-val]
                       (if-let [col-label (get inverse-map (long col-val))]
                         col-label
                         (throw (ex-info (format "Failed to find label for column value %s" col-val)
                                         {:inverse-label-map inverse-map})))))))
        column-values))
    (->> (->row-major dataset {:labels label-columns} options)
         (map :labels))))


(defn map-seq->dataset
  "Given a sequence of maps, construct a dataset.  Defaults to a tablesaw-based
  dataset."
  [map-seq {:keys [scan-depth
                   column-definitions
                   table-name
                   dataset-constructor]
            :or {scan-depth 100
                 table-name "_unnamed"
                 dataset-constructor 'tech.libs.tablesaw/map-seq->tablesaw-dataset}
            :as options}]
  ((parallel/require-resolve dataset-constructor)
   map-seq options))


(defn ->dataset
  ([dataset {:keys [table-name]
             :or {table-name "_unnamed"}
             :as options}]
   (if (satisfies? ds-proto/PColumnarDataset dataset)
     dataset
     (if (and (sequential? dataset)
              (or (not (seq dataset))
                  (map? (first dataset))))
       (map-seq->dataset dataset options)
       (throw (ex-info "Dataset appears to be empty or not convertible to a dataset"
                       {:dataset dataset})))))
  ([dataset]
   (->dataset dataset {})))
