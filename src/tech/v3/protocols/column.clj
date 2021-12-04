(ns ^:no-doc tech.v3.protocols.column)

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
  (^org.roaringbitmap.RoaringBitmap missing [col]
    "Indexes of missing values")
  (is-missing? [col idx]
    "Return true if this index is missing.")
  (set-missing [col long-rdr]
    "Set this group of indexes as the missing set")
  (buffer [col]
    "Return the data buffer (if there is one) backing this column")
  (as-map [col]
    "Return data map representation of this column.  That representation must
contain at least #:tech.v3.dataset{:name :data :missing :metadata}")
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


(defprotocol PHasIndexStructure
  (index-structure [this]
    "Returns an index-structure for the column. The index-structure will only be
generated the first time this is function is called.")
  (index-structure-realized? [this]
    "Returns true if the index-structure value has been produced. The index-structure
is only produced the first time it is requested.")
  (with-index-structure
    [this custom-make-index-structure-fn]
    "Returns a copy of the column that will return an index-structure using the
provided `custom-make-index-strucutre-fn`."))


(defprotocol PIndexStructure
  (select-from-index
    [index-structure mode selection-spec options]
    "Select a subset of the index. Supports a variety of modes."))
