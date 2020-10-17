(ns tech.v3.dataset.column-filters
  "Queries to select column subsets that have various properites such as all numeric
  columns, all feature columns, or columns that have a specific datatype.

  Further a few set operations (union, intersection, difference) are provided
  to further manipulate subsets of columns.

  All functions are transformations from dataset to dataset."
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset.base :as ds-base])
  (:import [clojure.lang IFn])
  (:refer-clojure :exclude [boolean update]))


(defn column-filter
  "Return a dataset with only the columns for which the filter function returns a truthy
  value."
  [dataset filter-fn]
  (->> (ds-base/columns dataset)
       (filter filter-fn)
       (map (comp :name meta))
       (ds-base/select-columns dataset)))


(defn metadata-filter
    "Return a dataset with only the columns for which, given the column metadata,
  the filter function returns a truthy value."
  [dataset filter-fn]
  (->> (map meta (ds-base/columns dataset))
       (filter filter-fn)
       (map :name)
       (ds-base/select-columns dataset)))


(defn categorical
  "Return a dataset containing only the categorical columns."
  [dataset]
  (metadata-filter dataset :categorical?))


(defn numeric
  "Return a dataset containing only the numeric columns."
  [dataset]
  (metadata-filter dataset (comp casting/numeric-type?
                                 packing/unpack-datatype
                                 :datatype)))

(defn of-datatype
  "Return a dataset containing only the columns of a specific datatype."
  [dataset datatype]
  (metadata-filter dataset #(= datatype (:datatype %))))


(defn boolean
  "Return a dataset containing only the boolean columns."
  [dataset]
  (of-datatype dataset :boolean))


(defn string
  "Return a dataset containing only the string columns."
  [dataset]
  (of-datatype dataset :string))


(defn target
  "Return a dataset containing only the columns that have been marked as inference
  targets."
  [dataset]
  (metadata-filter dataset :inference-target?))


(defn probability-distribution
  "Return the columns of the dataset that comprise the probability distribution
  after classification."
  [dataset]
  (metadata-filter dataset #(= :probability-distribution (:column-type %))))


(defn prediction
  "Return the columns of the dataset marked as predictions."
  [dataset]
  (metadata-filter dataset #(= :prediction (:column-type %))))


(defn feature
  "Return a dataset container only the columns which have not been marked as inference
  columns."
  [dataset]
  (metadata-filter dataset (complement :inference-target?)))


(defn datetime
  "Return a dataset containing only the datetime columns."
  [dataset]
  (metadata-filter dataset (comp  dtype-dt/datetime-datatype? :datatype)))


(defn intersection
  "Return only columns for rhs for which an equivalently named column exists in lhs."
  [lhs-ds rhs-ds]
  (let [lhs-names (set (map (comp :name meta) (ds-base/columns lhs-ds)))]
    (metadata-filter rhs-ds (comp lhs-names :name))))


(defn union
  "Return all columns of lhs along with any columns in rhs which have names that
  do not exist in lhs."
  [lhs-ds rhs-ds]
  (let [lhs-names (set (map (comp :name meta) (ds-base/columns lhs-ds)))]
    (reduce (fn [lhs-ds rhs-col]
              (if (lhs-names (:name (meta rhs-col)))
                lhs-ds
                (ds-base/add-column lhs-ds rhs-col)))
            lhs-ds
            (ds-base/columns rhs-ds))))


(defn difference
  "Return the columns in lhs which do not have an equivalently named column in
  rhs."
  ([lhs-ds rhs-ds]
   (apply dissoc lhs-ds (map (comp :name meta) (ds-base/columns rhs-ds))))
  ([lhs-ds]
   lhs-ds))
