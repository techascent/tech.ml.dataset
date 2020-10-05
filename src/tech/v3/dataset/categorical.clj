(ns ^:no-doc tech.v3.dataset.categorical
  "String->number and string->one-hot conversions and their inverses."
  (:require [tech.v3.dataset.base :as ds-base]
            [tech.v3.protocols.column :as ds-col]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.impl.column-base :as col-base]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [clojure.set :as set])
  (:import [java.util HashMap Map]
           [java.util.function Function BiFunction BiConsumer]))


;;This file uses categorical-map loosely.  Really they are lookup tables
;;from categorical object value to integer.
(defn- make-categorical-map-from-table-args
  "Make a mapping of value->index from a list of either string values or [valname idx]
  pairs.
  Returns map of value->index."
  ^Map [table-value-list]
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


(defrecord CategoricalMap [lookup-table src-column label-datatype])


(defn fit-categorical-map
  "Given a column, map it into an numeric space via a discrete map of values
  to integers.  This fits the categorical transformation onto the column and returns
  the transformation."
  ^CategoricalMap [dataset colname & [table-args res-dtype]]
  (map->CategoricalMap
   {:lookup-table (reduce (fn [categorical-map col-val]
                            (if (get categorical-map col-val)
                              categorical-map
                              (assoc categorical-map col-val
                                     (long (count categorical-map)))))
                          (make-categorical-map-from-table-args table-args)
                          (ds-col/unique (ds-base/column dataset colname)))
    :src-column colname
    :label-datatype (or res-dtype :float64)}))


(defn transform-categorical-map
  [dataset fit-data]
  (let [colname (:src-column fit-data)
        label-datatype (or (:label-datatype fit-data) :float64)
        lookup-table (:lookup-table fit-data)
        column (ds-base/column dataset colname)
        missing (ds-col/missing column)
        col-meta (meta column)
        missing-value (col-base/datatype->missing-value label-datatype)]
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
                        label-datatype
                        column)
            (assoc col-meta :categorical-map fit-data)
            missing))))


(defn column-has-categorical-map?
  [column]
  (boolean (:categorical-map (meta column))))


(defn dataset->categorical-maps
  "Given a dataset, return a map of column names to categorical label maps.
  This aids in inverting all of the label maps in a dataset."
  [dataset]
  (->> (vals dataset)
       (map (comp :categorical-map meta))
       (remove nil?)))


(defn invert-categorical-map
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
            (dissoc (meta column) :categorical-map)
            (ds-col/missing column)))))


(defn- safe-str
  [data]
  (if (keyword? data) (name data)
      (.replace (str data) " " "-")))


(defn fit-one-hot
  [dataset colname & [table-args res-dtype]]
  (let [{:keys [lookup-table label-datatype]}
        (fit-categorical-map dataset colname table-args res-dtype)
        column (ds-base/column dataset colname)
        src-meta (meta column)
        src-name (:name src-meta)
        name-fn (if (= (dtype/datatype src-name) :keyword)
                  (let [src-name (name src-name)]
                    #(keyword (str src-name "-" (safe-str %))))
                  #(str src-name "-" (safe-str %)))
        one-hot-map (->> lookup-table
                         (map (fn [[k v]]
                                [k (name-fn v)]))
                         (into {}))]
    {:one-hot-map one-hot-map
     :src-column src-name
     :label-datatype label-datatype}))


(defn transform-one-hot
  [dataset one-hot-fit-data]
  (let [{:keys [one-hot-map src-column label-datatype]}
        one-hot-fit-data
        column (ds-base/column dataset src-column)
        missing (ds-col/missing column)
        dataset (dissoc dataset src-column)]
    (->> one-hot-map
         (mapcat
          (fn [[k v]]
            [v (col-impl/new-column
                v
                (dtype/emap
                 #(if (= % k)
                    1
                    0)
                 label-datatype
                 column)
                (assoc (meta column)
                       :one-hot-map one-hot-fit-data)
                missing)]))
         (apply assoc dataset))))


(defn dataset->one-hot-maps
  "Given a dataset, return a map of source column names to one-hot maps.
  This aids in inverting all of the one hot maps in a dataset."
  [dataset]
  (->> (vals dataset)
       (map (comp :one-hot-map meta))
       (remove nil?)
       (distinct)))


(defn one-hot-invert
  [dataset one-hot-map res-colname]
  (let [cast-fn (get @casting/*cast-table* :boolean)
        invert-map (set/map-invert one-hot-map)
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
                        (map ds-col/missing (vals one-hot-ds)))
        res-dtype (reduce casting/widest-datatype (keys one-hot-map))]
    (assoc dataset
           res-colname
           (col-impl/new-column
            res-colname
            (apply dtype/emap (fn [& colvals]
                                (first (remove nil? colvals)))
                   res-dtype
                   rev-mapped-cols)
            (meta (first (vals one-hot-ds)))
            missing))))
