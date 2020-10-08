(ns tech.v3.dataset.column
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.column-data-process :as column-data-process]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.parallel.for :as pfor])
  (:import [java.util List]
           [tech.v3.dataset.impl.column Column]
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
  [col idx]
  (when-let [^RoaringBitmap bmp (missing col)]
    (.contains bmp idx)))


(defn set-missing
  "Set the missing indexes for a column.  This doesn't change any values in the
  underlying data store."
  [col idx-seq]
  (col-proto/set-missing col idx-seq))


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
      (catch Throwable e
        nil))))


(defn parse-column
  "parse a text or a str column, returning a new column with the same name but with
  a different datatype.  This method is single-threaded.

  parser-fn-or-kwd is nil by default and can the keyword :relaxed?  or a function that
  must return one of parsed-value, :tech.ml.dataset.parse/missing in which case a
  missing value will be added or :tech.ml.dataset.parse/parse-failure in which case the
  a missing index will be added and the string value will be recorded in the metadata's
  :unparsed-data, :unparsed-indexes entries."
  ([datatype col]
   (let [colname (column-name col)
         col-reader (dtype/emap #(when % (str %)) :string col)
         col-parser (column-parsers/make-fixed-parser datatype)
         n-elems (dtype/ecount col-reader)]
     (dotimes [iter n-elems]
       (column-parsers/add-value! col-parser iter (col-reader iter)))

     (let [{:keys [data missing metadata]}
           (column-parsers/finalize! col-parser n-elems)]
       (new-column colname data metadata missing)))))


(defn new-column
  "Create a new column.  Data will scanned for missing values
  unless the full 4-argument pathway is used."
  ([name data]
   (let [{coldata :data
          scanned-missing :missing}
         (column-data-process/scan-data-for-missing data)]
     (new-column name coldata nil scanned-missing)))
  ([name data metadata]
   (let [{coldata :data
          scanned-missing :missing}
         (column-data-process/scan-data-for-missing data)]
     (new-column name coldata metadata scanned-missing)))
  ([name data metadata missing]
   (col-impl/new-column name data metadata missing)))


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
  "Map a scalar function across one or more columns.  New missing set becomes
  the union of all the other missing sets.  Column is named :_unnamed.
  This is the missing-set aware version of tech.v3.datatype/emap."
  [map-fn res-dtype & args]
  (col-impl/new-column :_unnamed
                       (apply dtype/emap map-fn res-dtype args)
                       nil
                       (reduce dtype-proto/set-or (map col-proto/missing args))))
