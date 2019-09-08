(ns tech.ml.dataset.base
  (:require [tech.v2.datatype :as dtype]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.utils :as ml-utils]
            [tech.parallel.require :as parallel-req])
  (:import [java.io InputStream]))


(set! *warn-on-reflection* true)


(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn set-dataset-name
  [dataset ds-name]
  (ds-proto/set-dataset-name dataset ds-name))

(defn metadata
  [dataset]
  (ds-proto/metadata dataset))

(defn set-metadata
  [dataset meta-map]
  (ds-proto/set-metadata dataset meta-map))

(defn maybe-column
  "Return either column if exists or nil."
  [dataset column-name]
  (ds-proto/maybe-column dataset column-name))


(defn column
  "Return the column or throw if it doesn't exist."
  [dataset column-name]
  (ds-proto/column dataset column-name))

(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (ds-proto/columns dataset))


(defn column-map
  "clojure map of column-name->column"
  [datatypes]
  (->> (ds-proto/columns datatypes)
       (map (juxt ds-col/column-name identity))
       (into {})))


(defn column-names
  "In-order sequence of column names"
  [dataset]
  (->> (ds-proto/columns dataset)
       (map ds-col/column-name)))


(defn columns-with-missing-seq
  "Return a sequence of:
  {:column-name column-name
   :missing-count missing-count
  }
  or nil of no columns are missing data."
  [dataset]
  (->> (columns dataset)
       (map (fn [col]
              (let [missing-count (count (ds-col/missing col))]
                (when-not (= 0 missing-count)
                  {:column-name (ds-col/column-name col)
                   :missing-count missing-count}))))
       (remove nil?)
       seq))


(defn add-column
  "Add a new column. Error if name collision"
  [dataset column]
  (ds-proto/add-column dataset column))


(defn new-column
  "Create a new column from some values."
  ([dataset column-name values {:keys [datatype
                                       container-type]
                                :or {container-type :tablesaw-column}
                                :as options}]
   (let [datatype (or datatype (dtype/get-datatype values))]
     (->> (if (ds-col/is-column? values)
            (ds-col/set-name values column-name)
            (if-let [col (first (columns dataset))]
              (ds-col/new-column col datatype values {:name column-name})
              (dtype/make-container container-type datatype values
                                    (assoc options
                                           :column-name column-name))))
          (add-column dataset))))
  ([dataset column-name values]
   (new-column dataset column-name values {})))


(defn remove-column
  "Fails quietly"
  [dataset col-name]
  (ds-proto/remove-column dataset col-name))


(defn remove-columns
  [dataset colname-seq]
  (reduce ds-proto/remove-column dataset colname-seq))


(defn update-column
  "Update a column returning a new dataset.  update-fn is a column->column
  transformation.  Error if column does not exist."
  [dataset col-name update-fn]
  (ds-proto/update-column dataset col-name update-fn))


(defn order-column-names
  "Order a sequence of columns names so they match the order in the
  original dataset.  Missing columns are placed last."
  [dataset colname-seq]
  (let [colname-set (set colname-seq)
        ordered-columns (->> (columns dataset)
                             (map ds-col/column-name)
                             (filter colname-set))]
    (concat ordered-columns
            (remove (set ordered-columns) colname-seq))))


(defn update-columns
  "Update a sequence of columns."
  [dataset column-name-seq update-fn]
  (reduce (fn [dataset colname]
            (update-column dataset colname update-fn))
          dataset
          column-name-seq))


(defn add-or-update-column
  "If column exists, replace.  Else append new column."
  ([dataset colname column]
   (ds-proto/add-or-update-column dataset colname column))
  ([dataset column]
   (add-or-update-column dataset (ds-col/column-name column) column)))


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
colname-seq - either keyword :all or list of column names with no duplicates.
index-seq - either keyword :all or list of indexes.  May contain duplicates."
  [dataset colname-seq index-seq]
  (ds-proto/select dataset colname-seq index-seq))


(defn unordered-select
  "Perform a selection but use the order of the columns in the existing table; do
  *not* reorder the columns based on colname-seq.  Useful when doing selection based
  on sets."
  [dataset colname-seq index-seq]
  (select dataset
          (order-column-names dataset colname-seq)
          index-seq))


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
  "dataset->dataset transformation"
  [predicate dataset & [column-name-seq]]
  ;;interleave, partition count would also work.
  (let [column-name-seq (or column-name-seq
                            (->> (columns dataset)
                                 (mapv ds-col/column-name)))]
    (->> (index-value-seq (select dataset column-name-seq :all))
         (filter (fn [[idx col-values]]
                   (predicate (zipmap column-name-seq
                                      col-values))))
         (map first)
         (select dataset :all))))


(defn ds-group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  Y values where Y is the count of column-name-seq or :all."
  [key-fn dataset & [column-name-seq]]
  (let [column-name-seq (vec (or column-name-seq
                                 (->> (columns dataset)
                                      (map ds-col/column-name))))]
    (->> (index-value-seq (select dataset column-name-seq :all))
         (group-by (fn [[idx col-values]]
                     (->> (zipmap column-name-seq
                                  col-values)
                          key-fn)))
         (map (fn [[k v]]
                [k (select dataset :all (map first v))]))
         (into {}))))


(defn ds-sort-by
  ([key-fn compare-fn dataset column-name-seq]
   (let [column-name-seq (when-not (= :all column-name-seq)
                           column-name-seq)
         column-name-seq (or column-name-seq
                             (->> (columns dataset)
                                  (mapv ds-col/column-name)))]
     (->> (index-value-seq (select dataset column-name-seq :all))
          (sort-by (fn [[idx col-values]]
                     (->> (zipmap column-name-seq
                                  col-values)
                          key-fn))
                   compare-fn)
          (map first)
          (select dataset :all))))
  ([key-fn compare-fn dataset]
   (ds-sort-by key-fn compare-fn dataset :all))
  ([key-fn dataset]
   (ds-sort-by key-fn < dataset :all)))


(defn ds-concat
  [dataset & other-datasets]
  (let [datasets  (concat [dataset] (remove nil? other-datasets))
        column-list
        (->> datasets
             (mapcat (fn [dataset]
                       (->> (columns dataset)
                            (mapv (fn [col]
                                    (assoc (ds-col/metadata col)
                                           :column
                                           col
                                           :table-name (dataset-name dataset)))))))
             (group-by :name))
        label-map (->> datasets
                       (map (comp :label-map metadata))
                       (apply merge))]
    (when-not (= 1 (count (->> (vals column-list)
                               (map count)
                               distinct)))
      (throw (ex-info "Dataset is missing a column" {})))
    (->> column-list
         (mapv (fn [[colname columns]]
                 (let [columns (map :column columns)
                       newcol-ecount (apply + 0 (map dtype/ecount columns))
                       first-col (first columns)
                       new-col (ds-col/new-column first-col
                                                  (dtype/get-datatype first-col)
                                                  newcol-ecount
                                                  (ds-col/metadata first-col))]
                   (dtype/copy-raw->item! columns
                                          new-col 0
                                          {:unchecked? true})
                   new-col)))
         (ds-proto/from-prototype dataset (dataset-name dataset))
         (#(set-metadata % {:label-map label-map})))))


(defn ds-take-nth
  [n-val dataset]
  (select dataset :all (->> (range (second (dtype/shape dataset)))
                            (take-nth n-val))))


(defn ds-map-values
  "Note this returns a sequence, not a dataset."
  [dataset map-fn & [column-name-seq]]
  (->> (index-value-seq (select dataset (or (seq column-name-seq) :all) :all))
       (map (fn [[idx col-values]]
              (apply map-fn col-values)))))


(defn ds-column-map
  "Map a function columnwise across datasets and produce a new dataset.
  column sequence.  Note this does not produce a new dataset as that would
  preclude remove,filter on nil values."
  [map-fn first-ds & ds-seq]
  (let [all-datasets (concat [first-ds] ds-seq)]
       ;;first order the columns
       (->> all-datasets
            (map columns)
            (apply map map-fn))))

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
  ((parallel-req/require-resolve dataset-constructor)
   map-seq options))


(defn ->dataset
  ([dataset
    {:keys [table-name]
     :as options}]
   (let [dataset
         (cond
           (satisfies? ds-proto/PColumnarDataset dataset)
           dataset
           (or (instance? InputStream dataset)
               (string? dataset))
           (apply
            (parallel-req/require-resolve 'tech.libs.tablesaw/path->tablesaw-dataset)
            dataset (apply concat options))
           :else
           (map-seq->dataset dataset options))]
     (if table-name
       (ds-proto/set-dataset-name dataset table-name)
       dataset)))
  ([dataset]
   (->dataset dataset {})))

(defn ->>dataset
  ([options dataset]
   (->dataset dataset options))
  ([dataset]
   (->dataset dataset)))
