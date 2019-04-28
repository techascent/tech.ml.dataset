(ns tech.ml.dataset.pipeline
  "A set of common 'pipeline' operations you probably will want to run on a dataset."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.options :as options]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline.pipeline-operators
             :refer [def-multiple-column-etl-operator]
             :as pipe-ops]
            [tech.ml.dataset.column-filters :as col-filters])
  (:refer-clojure :exclude [replace]))


(defmacro def-pipeline-fn
  [op-name documentation-string op-varname destructure-map]
  `(defn ~op-name
     ~documentation-string
     [~'dataset & ~destructure-map]
     (pipe-ops/inline-perform-operator ~op-varname ~'dataset ~'column-filter ~'op-args)))


(defn remove-columns
  "Remove columns selected by column-filter"
  [dataset column-filter]
  (pipe-ops/inline-perform-operator pipe-ops/remove-columns dataset column-filter nil))


(def-pipeline-fn string->number
  "Convert all string columns to numeric recording the lookup table
  in the column metadata.

  Replace any string values with numeric values.  Updates the label map
  of the options.  Arguments may be notion or a vector of either expected
  strings or tuples of expected strings to their hardcoded values."
  pipe-ops/string->number {:keys [datatype column-filter table-value-list]
                           :or {column-filter col-filters/string?}
                           :as op-args})


(def-pipeline-fn one-hot
  "Replace string columns with one-hot encoded columns.  table value list Argument can
  be nothing or a map containing keys representing the new derived column names and
  values representing which original values to encode to that particular column.  The
  special keyword :rest indicates any remaining unencoded columns.
  example argument:
  {:main [\"apple\" \"mandarin\"]
 :other :rest}"
  pipe-ops/one-hot {:keys [datatype column-filter table-value-list]
                    :or {column-filter col-filters/string?}
                    :as op-args})


(def-pipeline-fn replace-missing
  "Replace all the missing values in the dataset.  Can take a sclar missing value
or a callable fn.  If callable fn, the fn is passed the dataset and column-name"
  pipe-ops/replace-missing {:keys [column-filter missing-value-or-fn] :as op-args})


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


(def-pipeline-fn replace
  "Map a function across a column or set of columns.  Map-fn may be a map?.
  Result column names are identical to src column names but metadata like a label
  map is removed.
  If map-setup-fn is provided, map-fn must be nil and map-setup-fn will be called
  with the dataset and column name to produce map-fn."
  pipe-ops/replace {:keys [column-filter result-datatype map-setup-fn map-fn] :as op-args})


(defn update-dataset-column
  "Update a column via a function.  Function takes a dataset and a column and returns
  either a column, an iterable, or a reader."
  [dataset dataset-column-fn & {:keys [column-filter]}]
  (pipe-ops/inline-perform-operator pipe-ops/update-column dataset
                                    column-filter dataset-column-fn))


(defn update-column
  "Update a column via a function.  Function takes a column and returns a either a
  column, an iterable, or a reader."
  [dataset column-fn & {:keys [column-filter]}]
  (update-dataset-column dataset #(column-fn %2) {:column-filter column-filter}))


(defn new-column
  "Create a new column.  fn takes dataset and returns a reader, an iterable, or
  a new column."
  [dataset result-colname dataset-column-fn]
  (ds/new-column dataset result-colname (dataset-column-fn dataset)))


(def-pipeline-fn ->datatype
  "Marshall columns to be the etl datatype.  This changes numeric columns to be a
  unified backing store datatype."
  pipe-ops/->datatype {:keys [column-filter datatype] :as op-args})


(def-pipeline-fn range-scale
  "Range-scale a set of columns to be within either [-1 1] or the range provided
  by the first argument.  Will fail if columns have missing values."
  pipe-ops/range-scaler {:keys [column-filter value-range]
                         :or {value-range [-1 1]} :as op-args})


(def-pipeline-fn std-scale
  "Scale columns to have 0 mean and 1 std deviation.  Will fail if columns
  contain missing values."
  pipe-ops/range-scaler  {:keys [column-filter use-mean? use-std?]
                          :or {use-mean? true use-std? true} :as op-args})
