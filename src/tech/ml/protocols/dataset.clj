(ns tech.ml.protocols.dataset
  (:require [clojure.set :as c-set]
            [tech.ml.protocols.column :as col-proto]
            [tech.datatype :as dtype]
            [clojure.core.matrix.protocols :as mp]))


(defprotocol PColumnarDataset
  (dataset-name [dataset])
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
  (add-or-update-column [dataset column]
    "If column exists, replace.  Else append new column.")
  (select [dataset colname-seq index-seq]
    "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
colname-seq - either keyword :all or list of column names with no duplicates.
index-seq - either keyword :all or list of indexes.  May contain duplicates.")
  (index-value-seq [dataset]
    "Get a sequence of tuples:
[idx col-value-vec]

Values are in order of column-name-seq.  Duplicate names are allowed and result in
duplicate values.")
  (supported-column-stats [dataset]
    "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}.")
  (from-prototype [dataset table-name column-seq]
    "Create a new dataset that is the same type as this one but with a potentially
different table name and column sequence.  Take care that the columns are all of
the correct type."))
