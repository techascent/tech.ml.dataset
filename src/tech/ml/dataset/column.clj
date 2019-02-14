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


(defn cache
  "Return the cache map for this column.  Cache maps are
never duplcated or copied."
  [col]
  (col-proto/cache col))


(defn set-cache
  "Set the cache on the column returning a new column. Cache maps
are never duplicated or copied."
  [col cache-map]
  (col-proto/set-cache col cache-map))


(defn merge-cache
  "Merge the existing cache with new cache map.  Cache maps
  are never duplicated or copied."
  [col cache-map]
  (->> (merge (cache col)
              cache-map)
       (set-cache col)))


(defn missing
  "Indexes of missing values"
  [col]
  (col-proto/missing col))


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


(defn column-values
  "Return a 'thing convertible to a sequence' of values for this column.
May be a java array or something else.  Likely to error on missing."
  [col]
  (col-proto/column-values col))


(defn is-missing?
  "Return true if this index is missing."
  [col idx]
  (col-proto/is-missing? col idx))


(defn get-column-value
  "Get a value fro mthe column.  Error on missing values."
  [col idx]
  (col-proto/get-column-value col idx))


(defn set-values
  "Set values in the column returning a new column with same name and datatype.  Values
which cannot be simply coerced to the datatype are an error."
  [col idx-val-seq]
  (if (seq idx-val-seq)
    (col-proto/set-values col idx-val-seq)
    col))


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


(defn math-context
  "Return an implementation of PColumnMathContext for operating items of this
  column type."
  [col]
  (col-proto/math-context col))


(defn unary-op
  "Perform a unary operation (operation of one argument)"
  [math-context op-env op-arg op-kwd]
  (col-proto/unary-op math-context op-env op-arg op-kwd))


(defn binary-op
  "Perform a binary operation (operation logically of two arguments).
  op-args is at least 2 in length -
  (+ 1 2 3 4 5) is allowed.
  op-scalar is a function taking 2 scalar values and returning a scalar value."
  [math-context op-env op-args op-scalar op-kwd]
  (col-proto/binary-op math-context op-env op-args op-scalar op-kwd))
