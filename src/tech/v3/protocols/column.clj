(ns ^:no-doc tech.v3.protocols.column
  (:import [org.roaringbitmap RoaringBitmap]))

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
  (^RoaringBitmap missing [col]
    "Indexes of missing values")
  (is-missing? [col idx]
    "Return true if this index is missing.")
  (set-missing [col long-rdr]
    "Set this group of indexes as the missing set")
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
  (select [col idx-seq]
    "Return a new column with the subset of indexes")
  (to-double-array [col error-on-missing?]
    "Convert to a java primitive array of a given datatype.  For strings,
an implicit string->double mapping is expected.  For booleans, true=1 false=0.
Finally, any missing values should be indicated by a NaN of the expected type."))
