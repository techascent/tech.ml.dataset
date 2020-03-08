(ns tech.ml.dataset.column
  (:require [tech.ml.protocols.column :as col-proto]))


(defn is-column?
  "Return true if this item is a column."
  [item]
  (and (satisfies? col-proto/PColumn item)
       (col-proto/is-column? item)))

(defn column-name
  [col]
  (col-proto/column-name col))


(defn set-name
  "Return a new column."
  [col name]
  (col-proto/set-name col name))


(defn supported-stats
  "List of available stats for the column"
  [col]
  (col-proto/supported-stats col))


(defn metadata
 "Return the metadata map for this column.
  Metadata must contain :name :datatype :size.  Categorical columns must have
  :categorical? true and the inference target should have :target? true."
  [col]
  (col-proto/metadata col))


(defn set-metadata
  "Set the metadata on the column returning a new column.
  Beware this could change the name."
  [col data-map]
  (col-proto/set-metadata col data-map))


(defn merge-metadata
  "Merge metadata in column with this map.
  Beware this could change the name of the column."
  [col data-map]
  (->> (merge (metadata col)
              data-map)
       (set-metadata col)))


(defn missing
  "Indexes of missing values"
  [col]
  (col-proto/missing col))


(defn is-missing?
  "Return true if this index is missing."
  [col idx]
  (col-proto/is-missing? col idx))


(defn unique
  "Set of all unique values"
  [col]
  (col-proto/unique col))


(defn stats
  "Return a map of stats.  Stats set is a set of the desired stats in keyword
form.  Guaranteed support across implementations for :mean :variance :median :skew.
Implementations should check their metadata before doing calculations."
  [col stats-set]
  (col-proto/stats col stats-set))


(defn correlation
  "Correlation coefficient for given 2 columns.  Available correlation types
  are:
  :pearson
  :spearman
  :kendall

  Returns floating point number between [-1 1]"
  [lhs rhs correlation-type]
  (col-proto/correlation lhs rhs correlation-type))


(defn select
  "Return a new column with the subset of indexes"
  [col idx-seq]
  (col-proto/select col idx-seq))


(defn empty-column
  "Return a new column of this supertype where all values are missing."
  [col datatype elem-count & [metadata]]
  (col-proto/empty-column col datatype elem-count
                          (or metadata (col-proto/metadata col))))


(defn new-column
  "Return a new column of this supertype with these values"
  [col datatype elem-count-or-values & [metadata]]
  (col-proto/new-column col datatype elem-count-or-values
                        (or metadata (col-proto/metadata col))))


(defn clone
  "Clone this column not changing anything."
  [col]
  (col-proto/clone col))


(defn to-double-array
  "Convert to a java primitive array of a given datatype.  For strings,
  an implicit string->double mapping is expected.  For booleans, true=1 false=0.
  Finally, any missing values should be indicated by a NaN of the expected type."
  ^doubles [col & [error-on-missing?]]
  (col-proto/to-double-array col error-on-missing?))
