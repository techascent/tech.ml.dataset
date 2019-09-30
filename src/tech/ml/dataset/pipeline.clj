(ns tech.ml.dataset.pipeline
  "A set of common 'pipeline' operations you probably will want to run on a dataset."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional.impl :as fn-impl]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline.column-filters :as cf]
            [tech.ml.dataset.pipeline.pipeline-operators :as pipe-ops]
            [tech.ml.dataset.pipeline.base :as pipe-base])
  (:refer-clojure :exclude [replace filter]))


(fn-impl/export-symbols tech.ml.dataset.pipeline.base
                        col
                        int-map)


(fn-impl/export-symbols tech.ml.dataset.pipeline.pipeline-operators
                        store-variables
                        read-var
                        training?
                        pif
                        pwhen)


(defn remove-columns
  "Remove columns selected by column-filter"
  [dataset column-filter]
  (pipe-ops/inline-perform-operator pipe-ops/remove-columns dataset column-filter nil))


(defn string->number
  "Convert all string columns to numeric recording the lookup table
  in the column metadata.

  Replace any string values with numeric values.  Updates the label map
  of the options.  Arguments may be notion or a vector of either expected
  strings or tuples of expected strings to their hardcoded values."
  ([dataset column-filter table-value-list & {:as op-args}]
   (pipe-ops/inline-perform-operator
    pipe-ops/string->number dataset column-filter
    (assoc op-args
           :table-value-list table-value-list)))
  ([dataset column-filter]
   (string->number dataset column-filter nil))
  ([dataset]
   (string->number dataset cf/string?)))


(defn one-hot
  "Replace string columns with one-hot encoded columns.  table value list Argument can
  be nothing or a map containing keys representing the new derived column names and
  values representing which original values to encode to that particular column.  The
  special keyword :rest indicates any remaining unencoded columns.
  example argument:
  {:main [\"apple\" \"mandarin\"]
 :other :rest}"
  ([dataset column-filter table-value-list & {:as op-args}]
   (pipe-ops/inline-perform-operator
    pipe-ops/one-hot dataset column-filter
    (assoc op-args
           :table-value-list table-value-list)))
  ([dataset column-filter]
   (one-hot dataset column-filter nil))
  ([dataset]
   (one-hot dataset cf/string?)))


(defn replace-missing
  "Replace all the missing values in the dataset.  Can take a sclar missing value
or a callable fn.  If callable fn, the fn is passed the dataset and column-name"
  [dataset column-filter missing-value]
  (pipe-ops/inline-perform-operator
   pipe-ops/replace-missing dataset column-filter
   {:missing-value missing-value}))


(defn remove-missing
  "Remove any missing values from the dataset"
  [dataset]
  (let [missing-indexes (->> (ds/columns-with-missing-seq dataset)
                             (mapcat (fn [{:keys [column-name]}]
                                       (-> (ds/column dataset column-name)
                                           ds-col/missing)))
                             set)]
    (ds/select dataset :all (->> (range (second (dtype/shape dataset)))
                                 (remove missing-indexes)))))


(defn replace
  "Map a function across a column or set of columns.  Map-fn may be a map?.
  Result column names are identical to src column names but metadata like a label
  map is removed.
  If map-setup-fn is provided, map-fn must be nil and map-setup-fn will be called
  with the dataset and column name to produce map-fn."
  [dataset column-filter replace-value-or-fn & {:keys [result-datatype]}]
  (pipe-ops/inline-perform-operator
   pipe-ops/replace dataset column-filter {:missing-value replace-value-or-fn
                                           :result-datatype result-datatype}))


(defn update-dataset-column
  "Update a column via a function.  Function takes a dataset and a column and returns
  either a column, an iterable, or a reader."
  [dataset column-filter dataset-column-fn]
  (pipe-ops/inline-perform-operator
   pipe-ops/update-column dataset
   column-filter dataset-column-fn))


(defn update-column
  "Update a column via a function.  Function takes a column and returns a either a
  column, an iterable, or a reader."
  [dataset column-filter column-fn]
  (update-dataset-column dataset column-filter #(column-fn %2)))


(defn new-column
  "Create a new column.  fn takes dataset and returns a reader, an iterable, or
  a new column."
  [dataset result-colname dataset-column-fn]
  (ds/new-column dataset result-colname (dataset-column-fn dataset)))


(defn ->datatype
  "Marshall columns to be the etl datatype.  This changes numeric columns to be a
  unified backing store datatype."
  ([dataset column-filter datatype]
   (pipe-ops/inline-perform-operator
    pipe-ops/->datatype dataset column-filter {:datatype datatype}))
  ([dataset column-filter]
   (->datatype dataset column-filter pipe-base/*pipeline-datatype*))
  ([dataset]
   (->datatype dataset #(cf/or cf/numeric? cf/boolean?) nil)))


(defn range-scale
  "Range-scale a set of columns to be within either [-1 1] or the range provided
  by the first argument.  Will fail if columns have missing values."
  ([dataset column-filter value-range & {:as op-args}]
   (pipe-ops/inline-perform-operator
    pipe-ops/range-scaler dataset column-filter
    (assoc op-args
           :value-range value-range)))
  ([dataset column-filter]
   (range-scale dataset column-filter [-1 1]))
  ([dataset]
   (range-scale dataset cf/numeric-and-non-categorical-and-not-target)))


(defn std-scale
  "Scale columns to have 0 mean and 1 std deviation.  Will fail if columns
  contain missing values."
  ([dataset column-filter & {:keys [use-mean? use-std?]
                              :or {use-mean? true
                                   use-std? true}
                             :as op-args}]
   (pipe-ops/inline-perform-operator
    pipe-ops/std-scaler dataset column-filter (assoc op-args
                                                     :use-mean? use-mean?
                                                     :use-std? use-std?)))
  ([dataset]
   (std-scale dataset cf/numeric-and-non-categorical-and-not-target)))


(defn assoc-metadata
  "Assoc a new value into the metadata."
  [dataset column-filter att-name att-value]
  (pipe-ops/inline-perform-operator
   pipe-ops/assoc-metadata dataset column-filter
   {:key att-name
    :value att-value}))


(defn m=
  "Perform some math.  Sets up variables such that the 'col' operator
  works."
  [dataset column-filter operation]
  (pipe-ops/inline-perform-operator
   pipe-ops/m= dataset column-filter operation))


(defn impute-missing
  "Group columns into K groups and impute missing values from the means calculated from
  those groups."
  ([dataset column-filter k]
   (pipe-ops/inline-perform-operator
    pipe-ops/impute-missing dataset column-filter {:k k}))
  ([dataset column-filter]
   (impute-missing dataset column-filter 5))
  ([dataset]
   (impute-missing dataset nil 5)))


(defn filter
  "Filter out indexes for which filter-fn produces a 0 or false value."
  ([dataset column-filter filter-fn]
   (pipe-ops/inline-perform-operator
    pipe-ops/filter dataset column-filter filter-fn)))


(defn pca
  ([dataset column-filter & {:as op-args}]
   (pipe-ops/inline-perform-operator
    pipe-ops/pca dataset column-filter op-args))
  ([dataset]
   (pca dataset cf/numeric-and-non-categorical-and-not-target)))


(defn correlation-table
  "See dataset/correlation table.  This version removes missing values
  and converts all columns to be numeric.  So this will always work."
  [dataset & {:keys [colname-seq
                     correlation-type]}]
  (-> dataset
      string->number
      ->datatype
      remove-missing
      (ds/correlation-table :colname-seq colname-seq
                            :correlation-type correlation-type)))
