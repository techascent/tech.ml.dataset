(ns tech.v3.dataset.categorical
  "Conversions of categorical values into numbers and back.  Two forms of conversions
  are supported, a straight value->integer map and one-hot encoding."
  (:require [tech.v3.dataset.base :as ds-base]
            [tech.v3.protocols.column :as col-proto]
            [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [clojure.set :as set]))


;;This file uses categorical-map loosely.  Really they are lookup tables
;;from categorical object value to integer.
(defn- make-categorical-map-from-table-args
  "Make a mapping of value->index from a list of either string values or [valname idx]
  pairs.
  Returns map of value->index."
  [table-value-list]
  ;; First, any explicit mappings are respected.
  (let [[str-table value-list]
        (reduce (fn [[str-table value-list] item]
                  (if (sequential? item)
                    [(assoc str-table
                            (first item)
                            (second item))
                     value-list]
                    [str-table (conj value-list item)]))
                [{} []]
                table-value-list)]
    ;;Finally, auto-generate values for anything not mapped yet.
    (->> value-list
         (reduce (fn [str-table item]
                   (assoc str-table item
                          (first (remove (set (vals str-table))
                                         (range)))))
                 str-table))))


(defrecord CategoricalMap [lookup-table src-column result-datatype])


(defn create-categorical-map
  [lookup-table src-colname result-datatype]
    (map->CategoricalMap
   {:lookup-table lookup-table
    :src-column src-colname
    :result-datatype result-datatype}))


(defn fit-categorical-map
  "Given a column, map it into an numeric space via a discrete map of values
  to integers.  This fits the categorical transformation onto the column and returns
  the transformation.

  If `table-args` is not given, the distinct column values will be mapped into 0..x without any specific order.

  'table-args` allows to specify the precise mapping as a sequence of pairs of [val idx] or as a sorted seq of values.
"
  ^CategoricalMap [dataset colname & [table-args res-dtype]]
  (create-categorical-map
   (reduce (fn [categorical-map col-val]
             (if (get categorical-map col-val)
               categorical-map
               (assoc categorical-map col-val
                      (long (count categorical-map)))))
           (make-categorical-map-from-table-args table-args)
           (col-proto/unique (ds-base/column dataset colname)))
   colname
   (or res-dtype :float64)))


(defn transform-categorical-map
  "Apply a categorical mapping transformation fit with fit-categorical-map."
  [dataset fit-data]
  (let [colname (:src-column fit-data)
        result-datatype (or (:result-datatype fit-data) :float64)
        lookup-table (:lookup-table fit-data)
        column (ds-base/column dataset colname)
        missing (col-proto/missing column)
        col-meta (meta column)
        missing-value (col-base/datatype->missing-value result-datatype)]
    (assoc dataset colname
           (col-impl/new-column
            (:name col-meta)
            (dtype/emap (fn [col-val]
                          (if-not (nil? col-val)
                            (let [numeric (get lookup-table col-val)]
                              (errors/when-not-errorf
                               numeric
                               "Failed to find label entry for column value %s"
                               col-val)
                              numeric)
                            missing-value))
                        result-datatype
                        column)
            (assoc col-meta :categorical-map fit-data)
            missing))))


(extend-type CategoricalMap
  ds-proto/PDatasetTransform
  (transform [t dataset]
    (transform-categorical-map dataset t)))


(defn dataset->categorical-maps
  "Given a dataset, return a map of column names to categorical label maps.
  This aids in inverting all of the label maps in a dataset.
  The source column name is src-column."
  [dataset]
  (->> (vals dataset)
       (map (comp :categorical-map meta))
       (remove nil?)))


(defn invert-categorical-map
  "Invert a categorical map returning the column to the original set of values."
  [dataset {:keys [src-column lookup-table]}]
  (let [column (ds-base/column dataset src-column)
        res-dtype (reduce casting/widest-datatype
                          (map dtype/datatype (keys lookup-table)))
        inv-map (set/map-invert lookup-table)
        missing-val (col-base/datatype->missing-value res-dtype)]
    (assoc dataset src-column
           (col-impl/new-column
            (:name src-column)
            (dtype/emap (fn [col-val]
                          (if-not (nil? col-val)
                            (let [src-val (get inv-map (long col-val))]
                              (errors/when-not-errorf
                                  src-val
                                "Unable to find src value for numeric value %s"
                                col-val)
                              src-val)
                            missing-val))
                        res-dtype
                        column)
            (-> (meta column)
                (dissoc :categorical-map)
                (assoc :categorical? true))
            (col-proto/missing column)))))


(defn- safe-str
  [data]
  (if (keyword? data) (name data)
      (.replace (str data) " " "-")))


(defrecord OneHotMap [one-hot-table src-column result-datatype])


(defn fit-one-hot
  "Fit a one hot transformation to a column.  Returns a reusable transformation.
  Maps each unique value to a column with 1 every time the value appears in the
  original column and 0 otherwise."
  ^OneHotMap [dataset colname & [table-args res-dtype]]
  (let [{:keys [lookup-table result-datatype]}
        (fit-categorical-map dataset colname table-args res-dtype)
        column (ds-base/column dataset colname)
        src-meta (meta column)
        src-name (:name src-meta)
        name-fn (if (= (dtype/datatype src-name) :keyword)
                  (let [src-name (name src-name)]
                    #(keyword (str src-name "-" (safe-str %))))
                  #(str src-name "-" (safe-str %)))
        one-hot-map (->> lookup-table
                         (map (fn [[k _v]]
                                [k (name-fn k)]))
                         (into {}))]
    (map->OneHotMap
     {:one-hot-table one-hot-map
      :src-column src-name
      :result-datatype result-datatype})))


(defn transform-one-hot
  "Apply a one-hot transformation to a dataset"
  [dataset one-hot-fit-data]
  (let [{:keys [one-hot-table src-column result-datatype]}
        one-hot-fit-data
        column (ds-base/column dataset src-column)
        missing (col-proto/missing column)
        dataset (dissoc dataset src-column)]
    (->> one-hot-table
         (mapcat
          (fn [[k v]]
            [v (col-impl/new-column
                v
                (dtype/emap
                 #(if (= % k)
                    1
                    0)
                 result-datatype
                 column)
                (assoc (meta column)
                       :one-hot-map one-hot-fit-data)
                missing)]))
         (apply assoc dataset))))


(extend-type OneHotMap
  ds-proto/PDatasetTransform
  (transform [t dataset]
    (transform-one-hot dataset t)))


(defn dataset->one-hot-maps
  "Given a dataset, return a sequence of applied on-hot transformations."
  [dataset]
  (->> (vals dataset)
       (map (comp :one-hot-map meta))
       (remove nil?)
       (distinct)))


(defn invert-one-hot-map
  "Invert a one-hot transformation removing the one-hot columns and adding back the
  original column."
  [dataset {:keys [one-hot-table src-column]}]
  (let [cast-fn (get @casting/*cast-table* :boolean)
        invert-map (set/map-invert one-hot-table)
        colnames (vec (keys invert-map))
        one-hot-ds (ds-base/select-columns dataset colnames)
        rev-mapped-cols (->> invert-map
                             (map (fn [[colname colval]]
                                    (dtype/emap #(if (cast-fn %)
                                                   colval
                                                   nil)
                                                :object
                                                (ds-base/column one-hot-ds colname)))))
        dataset (apply dissoc dataset colnames)
        missing (reduce dtype-proto/set-or
                        (map col-proto/missing (vals one-hot-ds)))
        res-dtype (reduce casting/widest-datatype
                          (map dtype/datatype (keys one-hot-table)))]
    (assoc dataset
           src-column
           (col-impl/new-column
            src-column
            (apply dtype/emap (fn [& colvals]
                                (first (remove nil? colvals)))
                   res-dtype
                   rev-mapped-cols)
            (-> (meta (first (vals one-hot-ds)))
                (dissoc :one-hot-map)
                (assoc :categorical? true))
            missing))))

(defn reverse-map-categorical-xforms
  "Given a dataset where we have converted columns from a categorical representation
  to either a numeric reprsentation or a one-hot representation, reverse map
  back to the original dataset given the reverse mapping of label->number in
  the column's metadata."
  [dataset]
  (->> (concat (map vector
                    (dataset->categorical-maps dataset)
                    (repeat invert-categorical-map))
               (map vector
                    (dataset->one-hot-maps dataset)
                    (repeat invert-one-hot-map)))
       (reduce (fn [dataset [cat-map invert-fn]]
                 (invert-fn dataset cat-map))
               dataset)))
