(ns ^:no-doc tech.v3.protocols.dataset)


(defprotocol PColumnarDataset
  (dataset-name [dataset])
  (set-dataset-name [dataset name])
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
  (supported-column-stats [dataset]
    "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."))
