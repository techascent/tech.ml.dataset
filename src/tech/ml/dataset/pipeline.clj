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
            [tech.ml.dataset.column-filters :as col-filters]))


(defn string->number
  "Convert all string columns to numeric recording the lookup table
  in the column metadata.

  Replace any string values with numeric values.  Updates the label map
  of the options.  Arguments may be notion or a vector of either expected
  strings or tuples of expected strings to their hardcoded values."
  [dataset & {:keys [datatype column-name-seq table-value-list] :as op-args}]
  (pipe-ops/inline-perform-operator
   pipe-ops/string->number dataset (or column-name-seq (col-filters/string? dataset)) op-args))


(defn one-hot
  "Replace string columns with one-hot encoded columns.  table value list Argument can
  be nothing or a map containing keys representing the new derived column names and
  values representing which original values to encode to that particular column.  The
  special keyword :rest indicates any remaining unencoded columns.
  example argument:
  {:main [\"apple\" \"mandarin\"]
 :other :rest}"
  [dataset & {:keys [datatype column-name-seq table-value-list] :as op-args}]
  (pipe-ops/inline-perform-operator
   pipe-ops/one-hot dataset (or column-name-seq (col-filters/string? dataset)) op-args))


(defn replace-missing
  "Replace all the missing values in the dataset.  Input can be a value
  or a fn.  If a fn, it gets passed the dataset and the column-name."
  [dataset replace-value-or-fn & {:keys [column-name-seq]}]
  (pipe-ops/inline-perform-operator
   pipe-ops/replace-missing dataset (or column-name-seq (ds/column-names dataset))
   replace-value-or-fn))


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


(defn replace-string
  [dataset src-str replace-str & {:keys [column-name-seq]}]
  (pipe-ops/inline-perform-operator
   pipe-ops/replace-string dataset (or column-name-seq (col-filters/string? dataset))
   [src-str replace-str]))


(defn ->datatype
  "Marshall columns to be the etl datatype.  This changes numeric columns to be a
  unified backing store datatype."
  [dataset & {:keys [column-name-seq datatype]
              :or {datatype :float64}}]
  (pipe-ops/inline-perform-operator
   pipe-ops/->datatype dataset (or column-name-seq (ds/column-names dataset))
   datatype))


(defn range-scale
  "Range-scale a set of columns to be within either [-1 1] or the range provided
  by the first argument.  Will fail if columns have missing values."
  [dataset & {:keys [column-name-seq value-range]
              :or {value-range [-1 1]} :as op-args}]
  (pipe-ops/inline-perform-operator
   pipe-ops/range-scaler dataset (or column-name-seq (ds/column-names dataset))
   op-args))


(defn std-scale
  "Scale columns to have 0 mean and 1 std deviation.  Will fail if columns
  contain missing values."
  [dataset & {:keys [column-name-seq use-mean? use-std?]
              :or {use-mean? true use-std? true} :as op-args}]
  (pipe-ops/inline-perform-operator
   pipe-ops/range-scaler dataset (or column-name-seq (ds/column-names dataset))
   op-args))
