(ns tech.v3.dataset.column-filters
  "Queries to select column subsets that have various properites such as all numeric
  columns, all feature columns, or columns that have a specific datatype.

  All column filters return column names in the same order as the input dataset so it
  is safe to call 'select-columns' with the result and the order of columns will not change."
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset.base :as ds-base]
            [clojure.set :as set])
  (:import [clojure.lang IFn])
  (:refer-clojure :exclude [boolean update]))


(defn column-filter
  [dataset filter-fn]
  (->> (ds-base/columns dataset)
       (filter filter-fn)
       (map (comp :name meta))
       (ds-base/select-columns dataset)))


(defn metadata-filter
  [dataset filter-fn]
  (->> (map meta (ds-base/columns dataset))
       (filter filter-fn)
       (map :name)
       (ds-base/select-columns dataset)))


(defn categorical
  [dataset]
  (metadata-filter dataset :categorical?))


(defn numeric
  [dataset]
  (metadata-filter dataset (comp casting/numeric-type?
                                 packing/unpack-datatype
                                 :datatype)))


(defn datatype
  [dataset datatype]
  (metadata-filter dataset #(= datatype (:datatype %))))


(defn boolean
  [dataset]
  (datatype dataset :boolean))


(defn string
  [dataset]
  (datatype dataset :string))


(defn target
  [dataset]
  (metadata-filter dataset :inference-target?))


(defn feature
  [dataset]
  (metadata-filter dataset (complement :inference-target?)))


(defn datetime
  [dataset]
  (metadata-filter dataset (comp  dtype-dt/datetime-datatype? :datatype)))


(defn intersection
  [lhs-ds rhs-ds]
  (let [lhs-names (set (map (comp :name meta) (ds-base/columns lhs-ds)))]
    (metadata-filter rhs-ds (comp lhs-names :name))))


(defn union
  [lhs-ds rhs-ds]
  (let [lhs-names (set (map (comp :name meta) (ds-base/columns lhs-ds)))]
    (reduce (fn [lhs-ds rhs-col]
              (if (lhs-names (:name (meta rhs-col)))
                lhs-ds
                (ds-base/add-column lhs-ds rhs-col)))
            lhs-ds
            (ds-base/columns rhs-ds))))


(defn difference
  ([lhs-ds rhs-ds]
   (apply dissoc lhs-ds (map (comp :name meta) (ds-base/columns rhs-ds))))
  ([lhs-ds]
   lhs-ds))
