(ns tech.ml.protocols.column)

(defprotocol PIsColumn
  (is-column? [item]))


(extend-protocol PIsColumn
  Object
  (is-column? [item] false))


(defprotocol PColumn
  (column-name [col])
  (set-name [col name]
    "Return a new column.")
  (supported-stats [col]
    "List of available stats for the column")
  (metadata [col]
    "Return the metadata map for this column.
    Metadata must contain :name :type :size.  Categorical
columns must have :categorical? true and the inference target
should have :target? true.")
  (set-metadata [col data-map]
    "Set the metadata on the column returning a new column.")

  (cache [col]
    "Return the cache map for this column.  Cache maps are
never duplcated or copied.")
  (set-cache [col data-map]
    "Set the cache on the column returning a new column. Cache maps
are never duplicated or copied.")
  (missing [col]
    "Indexes of missing values")
  (unique [col]
    "Set of all unique values")
  (stats [col stats-set]
    "Return a map of stats.  Stats set is a set of the desired stats in keyword
form.  Guaranteed support across implementations for :mean :variance :median :skew.
Implementations should check their metadata before doing calculations.")
  (correlation [col other-column correlation-type]
    "Return the correlation coefficient
Supported types are:
:pearson
:spearman
:kendall")
  (column-values [col]
    "Return a 'thing convertible to a sequence' of values for this column.
May be a java array or something else.  Likely to error on missing.")
  (is-missing? [col idx]
    "Return true if this index is missing.")
  (get-column-value [col idx]
    "Get a value fro mthe column.  Error on missing values.")
  (set-values [col idx-val-seq]
    "Set values in the column returning a new column with same name and datatype.  Values
which cannot be simply coerced to the datatype are an error.")
  (select [col idx-seq]
    "Return a new column with the subset of indexes")
  (empty-column [col datatype elem-count metadata]
    "Return a new column of this supertype where all values are missing.")
  (new-column [col datatype elem-count-or-values metadata]
    "Return a new column of this supertype with these values")
  (clone [col]
    "Return a clone of this column.")
  (to-double-array [col error-on-missing?]
    "Convert to a java primitive array of a given datatype.  For strings,
an implicit string->double mapping is expected.  For booleans, true=1 false=0.
Finally, any missing values should be indicated by a NaN of the expected type.")
  (math-context [col]))


(defprotocol PColumnMathContext
  (is-tensor? [ctx op-arg]
    "Return true if this is a tensor and should use tensor ops.")
  (unary-op [ctx op-arg op-kwd]
    "Perform a unary operation (operation of one argument).")
  (unary-reduce [ctx op-arg op-kwd]
    "Perform a reduction across the argument returning a new tensor.")
  (binary-op [ctx op-args op-scalar-fn op-kwd]
    "Perform a binary operation (operation logically of two arguments).
  op-args is at least 2 in length -
  (+ 1 2 3 4 5) is allowed."))
