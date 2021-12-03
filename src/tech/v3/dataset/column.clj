(ns tech.v3.dataset.column
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dt-proto])
  (:import [tech.v3.dataset.impl.column Column]
           [org.roaringbitmap RoaringBitmap]))


(declare new-column)


(defn is-column?
  "Return true if this item is a column."
  [item]
  (when item
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


(defn missing
  "Indexes of missing values.  Both iterable and reader."
  ^RoaringBitmap [col]
  (col-proto/missing col))


(defn is-missing?
  "Return true if this index is missing."
  [col ^long idx]
  (when-let [^RoaringBitmap bmp (missing col)]
    (.contains bmp idx)))


(defn set-missing
  "Set the missing indexes for a column.  This doesn't change any values in the
  underlying data store."
  [col idx-seq]
  (col-proto/set-missing col idx-seq))


(defn index-structure
  "Returns an index structure for the column. The index structure can help optimize
  certain operations as they can provide faster lookup and subsetting of a column
  or dataset.

  Also see: tech.v3.dataset.column/select-from-index."
  [col]
  (col-proto/index-structure col))


(defn index-structure-realized?
   "Returns true if the index-structure value has been produced. The index-structure
is only produced the first time it is requested."
  [col]
  (col-proto/index-structure-realized? col))


(defn with-index-structure
  "Returns a copy of the column that will return an index-structure using the
  provided `custom-make-index-strucutre-fn`. It also registers `klass` aliased
  to `datatype-keyword` so it can be identified by the typing system."
  [col custom-make-index-structure-fn]
  (col-proto/with-index-structure col custom-make-index-structure-fn))


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
  "Return a new column with the subset of indexes based on the provided `selection`.
  `selection` can be a list of indexes to select or boolean values where the index
  position of each true element indicates a index to select. When supplying a list
  of indices, duplicates are possible and will select the specified position more
  than once."
  [col selection]
  (col-proto/select col selection))

(defn clone
  "Clone this column not changing anything."
  [col]
  (dtype/clone col))


(defn string-table-keyset
  "Get the string table for this column.  Returns nil if this isn't a string column.
  This doesn't necessarily tell you the unique set of the column unless you have just
  parsed a file.  It is, when non-nil, a strict superset of the strings in the
  columns."
  [col]
  (when (and (= :string (dtype/get-datatype col))
             (instance? Column col))
    (try
      (->> (str-table/get-str-table (.data ^Column col))
           :str->int
           (keys)
           (set))
      (catch Throwable _e
        nil))))

;; TODO - match inference expectations
(defn parse-column
  "parse a text or a str column, returning a new column with the same name but with
  a different datatype.  This method is single-threaded.

  parser-fn-or-kwd is nil by default and can the keyword :relaxed?  or a function that
  must return one of parsed-value, :tech.v3.dataset/missing in which case a
  missing value will be added or :tech.v3.dataset/parse-failure in which case the
  a missing index will be added and the string value will be recorded in the metadata's
  :unparsed-data, :unparsed-indexes entries.

  Options:

  Same options roughly as ->dataset, specifically of interest may be `:text-temp-file`.
  "
  ([datatype col options]
   (let [colname (column-name col)
         col-reader (dtype/emap #(when % (str %)) :string col)
         col-parser (column-parsers/make-fixed-parser colname datatype options)
         n-elems (dtype/ecount col-reader)]
     (dotimes [iter n-elems]
       (column-parsers/add-value! col-parser iter (col-reader iter)))
     (new-column (assoc (column-parsers/finalize! col-parser n-elems)
                        :tech.v3.dataset/name colname))))
  ([datatype col]
   (parse-column datatype col nil)))


(defn new-column
  "Create a new column.  Data will scanned for missing values
  unless the full 4-argument pathway is used."
  ([name data]
   (col-impl/new-column name data))
  ([name data metadata]
   (col-impl/new-column name data metadata))
  ([name data metadata missing]
   (col-impl/new-column name data metadata missing))
  ([data-or-data-map]
   (col-impl/new-column data-or-data-map)))


(defn extend-column-with-empty
  [column n-empty]
  (col-impl/extend-column-with-empty column n-empty))


(defn prepend-column-with-empty
  [column n-empty]
  (col-impl/prepend-column-with-empty column n-empty))


(defn to-double-array
  "Convert to a java primitive array of a given datatype.  For strings,
  an implicit string->double mapping is expected.  For booleans, true=1 false=0.
  Finally, any missing values should be indicated by a NaN of the expected type."
  ^doubles [col & [error-on-missing?]]
  (col-proto/to-double-array col error-on-missing?))


(defn column-map
  "Map a scalar function across one or more columns.
  This is the semi-missing-set aware version of tech.v3.datatype/emap.

  If res-dtype is nil then the result is scanned to infer datatype and
  missing set.  res-dtype may also be a map of options:

  Options:

  * `:datatype` - Set the dataype of the result column.  If not given result is scanned
  to infer result datatype and missing set.
  * `:missing-fn` - if given, columns are first passed to missing-fn as a sequence and
  this dictates the missing set.  Else the missing set is by scanning the results
  during the inference process.  See `tech.v3.dataset.column/union-missing-sets` and
  `tech.v3.dataset.column/intersect-missing-sets` for example functions to pass in
  here."
  [map-fn res-dtype & args]
  (let [res-opt-map (if (keyword? res-dtype)
                      {:datatype res-dtype}
                      (or res-dtype {}))
        ;;object readers get scanned to infer datatype
        res-dtype (res-opt-map :datatype :object)
        missing-fn (res-opt-map :missing-fn)]
    (col-impl/new-column :_unnamed
                         (apply dtype/emap map-fn res-dtype args)
                         nil
                         (if missing-fn
                           (missing-fn args)
                           nil))))


(defn union-missing-sets
  "Union the missing sets of the columns returning a roaring bitmap"
  [col-seq]
  (reduce dt-proto/set-or (map col-proto/missing col-seq)))


(defn intersect-missing-sets
  "Intersect the missing sets of the columns returning a roaring bitmap"
  [col-seq]
  (reduce dt-proto/set-and (map col-proto/missing col-seq)))
