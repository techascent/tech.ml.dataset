(ns tech.ml.protocols.dataset
  (:require [clojure.set :as c-set]
            [tech.ml.protocols.column :as col-proto]
            [tech.v2.datatype :as dtype]))


(defprotocol PColumnarDataset
  (dataset-name [dataset])
  (set-dataset-name [dataset name])
  (metadata [dataset])
  (set-metadata [dataset meta-map])
  (maybe-column [dataset column-name]
    "Return either column if exists or nil.")
  (columns [dataset])
  (add-column [dataset column]
    "Error if columns exists")
  (remove-column [dataset col-name]
    "Failes quietly")
  (update-column [dataset col-name update-fn]
    "Update a column returning a new dataset.  update-fn is a column->column transformation.
Error if column does not exist.")
  (add-or-update-column [dataset col-name col-data]
    "If column exists, replace.  Else append new column.")
  (select [dataset colname-seq index-seq]
    "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
colname-seq - either keyword :all or list of column names with no duplicates.
index-seq - either keyword :all or list of indexes.  May contain duplicates.")
  (select-columns-by-index [dataset num-seq]
    "Select a subset of columns by index.")
  (supported-column-stats [dataset]
    "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}.")
  (from-prototype [dataset table-name column-seq]
    "Create a new dataset that is the same type as this one but with a potentially
different table name and column sequence.  Take care that the columns are all of
the correct type."))

(defn column
  [dataset column-name]
  (if-let [retval (maybe-column dataset column-name)]
    retval
    (throw (ex-info (format "Failed to find column: %s" column-name)
                    {:column-name column-name}))))

(defn column-names
  [dataset]
  (map col-proto/column-name (columns dataset)))
