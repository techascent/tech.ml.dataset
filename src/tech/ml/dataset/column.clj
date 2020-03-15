(ns tech.ml.dataset.column
  (:require [tech.ml.protocols.column :as col-proto]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.parallel.for :as parallel-for]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting])
  (:import [it.unimi.dsi.fastutil.longs LongArrayList]
           [java.util List]))


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
  (col-proto/clone col))


(def object-primitive-array-types
  {(Class/forName "[Ljava.lang.Boolean;") :boolean
   (Class/forName "[Ljava.lang.Byte;") :int8
   (Class/forName "[Ljava.lang.Short;") :int16
   (Class/forName "[Ljava.lang.Integer;") :int32
   (Class/forName "[Ljava.lang.Long;") :int64
   (Class/forName "[Ljava.lang.Float;") :float32
   (Class/forName "[Ljava.lang.Double;") :float64})


(defn process-object-primitive-array-data
  [obj-data]
  (let [dst-container-type (get object-primitive-array-types (type obj-data))
        ^objects obj-data obj-data
        n-items (dtype/ecount obj-data)
        sparse-val (get @col-impl/dtype->missing-val-map dst-container-type)
        ^List dst-data (dtype/make-container :list dst-container-type n-items)
        sparse-indexes (LongArrayList.)]
    (parallel-for/parallel-for
     idx
     n-items
     (let [obj-data (aget obj-data idx)]
       (if (not (nil? obj-data))
         (.set dst-data idx obj-data)
         (locking sparse-indexes
           (.add sparse-indexes idx)
           (.set dst-data idx sparse-val)))))
    {:data dst-data
     :missing sparse-indexes}))


(defn scan-object-data-for-missing
  [container-dtype ^List obj-data]
  (let [n-items (dtype/ecount obj-data)
        ^List dst-data (col-impl/make-container container-dtype n-items)
        sparse-indexes (LongArrayList.)
        sparse-val (get @col-impl/dtype->missing-val-map container-dtype)]
    (parallel-for/parallel-for
     idx
     n-items
     (let [obj-data (.get obj-data idx)]
       (if (not (nil? obj-data))
         (.set dst-data idx obj-data)
         (locking sparse-indexes
           (.add sparse-indexes idx)
           (.set dst-data idx sparse-val)))))
    {:data dst-data
     :missing sparse-indexes}))


(defn scan-object-numeric-data-for-missing
  [container-dtype ^List obj-data]
  (let [n-items (dtype/ecount obj-data)
        ^List dst-data (col-impl/make-container container-dtype n-items)
        sparse-indexes (LongArrayList.)
        sparse-val (get @col-impl/dtype->missing-val-map container-dtype)]
    (parallel-for/parallel-for
     idx
     n-items
     (let [obj-data (.get obj-data idx)]
       (if (and (not (nil? obj-data))
                (or (= container-dtype :boolean)
                    (not= sparse-val obj-data)))
         (.set dst-data idx (casting/cast obj-data container-dtype))
         (locking sparse-indexes
           (.add sparse-indexes idx)
           (.set dst-data idx sparse-val)))))
    {:data dst-data
     :missing sparse-indexes}))


(defn ensure-column-reader
  [values-seq]
  (let [values-seq (if (dtype/reader? values-seq)
                     values-seq
                     (dtype/make-container :list
                                           (dtype/get-datatype values-seq)
                                           values-seq))]
    (if (= :object (dtype/get-datatype values-seq))
      (cond
        (contains? object-primitive-array-types (type values-seq))
        (process-object-primitive-array-data values-seq)
        (boolean? (first values-seq))
        (scan-object-numeric-data-for-missing :boolean values-seq)
        (number? (first values-seq))
        (scan-object-numeric-data-for-missing :float64 values-seq)
        (string? (first values-seq))
        (scan-object-data-for-missing :string values-seq)
        (keyword? (first values-seq))
        (scan-object-data-for-missing :keyword values-seq)
        (symbol? (first values-seq))
        (scan-object-data-for-missing :symbol values-seq)
        :else
        {:data values-seq})
      {:data values-seq})))


(defn new-column
  ([name data] (new-column name data nil nil))
  ([name data metadata] (new-column name data metadata nil))
  ([name data metadata missing]
   (let [{coldata :data
          scanned-missing :missing} (ensure-column-reader data)]
     (col-impl/new-column name coldata metadata (or missing scanned-missing)))))


(defn ensure-column
  "Convert an item to a column if at all possible.  Currently columns either implement
  the required protocols "
  [item]
  (cond
    (col-proto/is-column? item)
    item
    (map? item)
    (let [{:keys [name data missing metadata]} item]
      (when-not (and name data)
        (throw (Exception. "Column data map must have name and data")))
      (col-impl/new-column name (:data (ensure-column-reader data)) metadata missing))
    :else
    (throw (ex-info "item is not convertible to a column without further information"
                    {:item item}))))


(defn ensure-column-seq
  [item-seq]
  ;;mapv to force errors here instead of later
  (mapv (fn [idx item]
          (cond
            (col-proto/is-column? item)
            item
            (map? item)
            (ensure-column item)
            (dtype/reader? item)
            (let [{:keys [data missing]} (ensure-column-reader item)]
              (col-impl/new-column idx data {} missing))
            (instance? Iterable item)
            (let [{:keys [data missing]} (ensure-column-reader item)]
              (col-impl/new-column idx data {} missing))
            :else
            (throw (ex-info "Item does not appear to be either randomly accessable or iterable"
                            {:item item}))))
        (range (count item-seq))
        item-seq))


(defn to-double-array
  "Convert to a java primitive array of a given datatype.  For strings,
  an implicit string->double mapping is expected.  For booleans, true=1 false=0.
  Finally, any missing values should be indicated by a NaN of the expected type."
  ^doubles [col & [error-on-missing?]]
  (col-proto/to-double-array col error-on-missing?))
