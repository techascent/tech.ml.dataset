(ns tech.ml.dataset.column
  (:require [tech.ml.protocols.column :as col-proto]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.string-table :as str-table]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast])
  (:import [it.unimi.dsi.fastutil.longs LongArrayList]
           [java.util List]
           [tech.ml.dataset.impl.column Column]
           [org.roaringbitmap RoaringBitmap]))


(declare new-column)


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
  "Indexes of missing values.  Both iterable and reader."
  [col]
  (col-proto/missing col))


(defn is-missing?
  "Return true if this index is missing."
  [col idx]
  (col-proto/is-missing? col idx))


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
  ([datatype options col]
   (let [colname (column-name col)
         parse-fn (:parse-fn options datatype)
         parser-scan-len (:parser-scan-len options 100)
         col-reader (typecast/datatype->reader
                     :object
                     (-> (dtype/->reader col)
                         (ds-parse/convert-reader-to-strings)))
         col-parser (ds-parse/make-parser parse-fn (column-name col)
                                          (take parser-scan-len col-reader))
         ^RoaringBitmap missing (dtype-proto/as-roaring-bitmap (missing col))
         n-elems (dtype/ecount col-reader)]
     (dotimes [iter n-elems]
       (if (.contains missing iter)
         (ds-parse/missing! col-parser)
         (ds-parse/parse! col-parser (.read col-reader iter))))
     (let [{:keys [data missing metadata]} (ds-parse/column-data col-parser)]
       (new-column colname data metadata missing))))
  ([datatype col]
   (parse-column datatype {} col)))


(defn new-column
  ([name data] (new-column name data nil nil))
  ([name data metadata] (new-column name data metadata nil))
  ([name data metadata missing]
   (let [coldata (col-impl/ensure-column-reader data)]
     (col-impl/new-column name coldata metadata missing))))


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
