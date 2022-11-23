(ns tech.v3.dataset.column
  (:require [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :as bitmap]
            [ham-fisted.set :as set]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [tech.v3.dataset.impl.column Column]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype Buffer LongReader DoubleReader ObjectReader]
           [clojure.lang IFn$LL IFn$LLL IFn$LLLL
            IFn$DD IFn$DDD IFn$DDDD]))


(declare new-column)


(defn is-column?
  "Return true if this item is a column."
  [item]
  (when item
    (ds-proto/is-column? item)))


(defn column-name
  [col]
  (ds-proto/column-name col))


(defn set-name
  "Return a new column."
  [col name]
  (ds-proto/set-name col name))


(defn supported-stats
  "List of available stats for the column"
  [col]
  stats/all-descriptive-stats-names)


(defn missing
  "Indexes of missing values.  Both iterable and reader."
  ^RoaringBitmap [col]
  (ds-proto/missing col))


(defn is-missing?
  "Return true if this index is missing."
  [col ^long idx]
  (when-let [^RoaringBitmap bmp (missing col)]
    (.contains bmp idx)))


(defn set-missing
  "Set the missing indexes for a column.  This doesn't change any values in the
  underlying data store."
  [col idx-seq]
  (Column. (bitmap/->bitmap idx-seq) (ds-proto/column-buffer col) (meta col) nil))


(defn unique
  "Set of all unique values"
  [col]
  (set/unique col))


(defn stats
  "Return a map of stats.  Stats set is a set of the desired stats in keyword
form.  Guaranteed support across implementations for :mean :variance :median :skew.
Implementations should check their metadata before doing calculations."
  [col stats-set]
  (stats/descriptive-statistics stats-set col))


(defn correlation
  "Correlation coefficient for given 2 columns.  Available correlation types
  are:
  :pearson
  :spearman
  :kendall

  Returns floating point number between [-1 1]"
  [lhs rhs correlation-type]
  (case correlation-type
    :pearsons (stats/pearsons-correlation lhs rhs)
    :spearmans (stats/spearmans-correlation lhs rhs)
    :kendall (stats/kendalls-correlation lhs rhs)))


(defn select
  "Return a new column with the subset of indexes based on the provided `selection`.
  `selection` can be a list of indexes to select or boolean values where the index
  position of each true element indicates a index to select. When supplying a list
  of indices, duplicates are possible and will select the specified position more
  than once."
  [col selection]
  (ds-proto/select-rows col selection))

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
  (when (and error-on-missing? (not (== 0 (dtype/ecount (ds-proto/missing col)))))
    (throw (RuntimeException. (str "Missing value detected on column: "
                                   (ds-proto/column-name col)))))
  (hamf/double-array (dtype/->reader col :float64)))


(defn column-map
  "Map a scalar function across one or more columns.
  This is the semi-missing-set aware version of tech.v3.datatype/emap.  This function
  is never lazy.

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
        res-dtype (res-opt-map :datatype)
        missing-fn (res-opt-map :missing-fn)
        ^RoaringBitmap missing (if missing-fn
                                 (bitmap/->bitmap (missing-fn args))
                                 nil)
        ^Buffer data (apply dtype/emap map-fn res-dtype args)
        data (if (or (nil? missing) (.isEmpty missing))
               ;;data will be scanned by the dataset to ascertain datatype and/or missing
               (dtype/clone data)
               (-> (col-impl/make-column-buffer missing data res-dtype)
                   (dtype/clone)))]
    (Column. missing data {:name :_unnamed} nil)))


(defn union-missing-sets
  "Union the missing sets of the columns returning a roaring bitmap"
  [col-seq]
  (bitmap/reduce-union (lznc/map ds-proto/missing col-seq)))


(defn intersect-missing-sets
  "Intersect the missing sets of the columns returning a roaring bitmap"
  [col-seq]
  (bitmap/reduce-intersection (lznc/map ds-proto/missing col-seq)))
