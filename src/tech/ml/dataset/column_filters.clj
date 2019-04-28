(ns tech.ml.dataset.column-filters
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.base :as ds]
            [tech.ml.dataset.column :as ds-col]
            [clojure.set :as c-set])
  (:refer-clojure :exclude [string? not and or boolean?]))


(defn select-columns
  [dataset column-name-seq]
  (cond
    (fn? column-name-seq)
    (->> (column-name-seq dataset)
         (ds/select-columns dataset)
         ds/columns)
    (clojure.core/or (= :all column-name-seq)
                     (sequential? column-name-seq)
                     (nil? column-name-seq))
    (->> (ds/select-columns dataset (clojure.core/or column-name-seq :all))
         ds/columns)
    (clojure.core/or (clojure.core/string? column-name-seq)
                     (keyword? column-name-seq))
    [(ds/column dataset column-name-seq)]
    :else
    (throw (ex-info (format "Unrecognized argument to select columns: %s" column-name-seq)
                    {}))))


(defn select-column-names
  [dataset column-name-seq]
  (->> (select-columns dataset column-name-seq)
       (map ds-col/column-name)))


(defn column-filter
  [dataset col-filter-fn & [column-name-seq]]
  (->> (select-columns dataset column-name-seq)
       (filter col-filter-fn)
       (map ds-col/column-name)))


(defn of-datatype?
  "Return column-names  of a given datatype."
  [dataset datatype & [column-name-seq]]
  (column-filter dataset #(do
                            (= datatype (dtype/get-datatype %)))
                 column-name-seq))


(defn string?
  [dataset & [column-name-seq]]
  (of-datatype? dataset :string column-name-seq))


(defn boolean?
  [dataset & [column-name-seq]]
  (of-datatype? dataset :string column-name-seq))


(defn numeric?
  [dataset & [column-name-seq]]
  (column-filter dataset #(casting/numeric-type? (dtype/get-datatype %))
                 column-name-seq))


(defn categorical?
  [dataset & [column-name-seq]]
  (column-filter dataset (comp :categorical? ds-col/metadata)
                 column-name-seq))


(defn missing?
  [dataset & [column-name-seq]]
  (column-filter dataset #(not= 0 (ds-col/missing %))
                 column-name-seq))


(defn not
  [dataset & [column-name-seq]]
  (if-not (nil? column-name-seq)
    (->> (c-set/difference
          (set (ds/column-names dataset))
          (set (select-column-names dataset column-name-seq)))
         (ds/order-column-names dataset))
    (ds/column-names dataset)))


(defn and
  [dataset lhs-name-seq rhs-name-seq]
  (->> (c-set/intersection (set (select-column-names dataset lhs-name-seq))
                           (set (select-column-names dataset rhs-name-seq)))
       (ds/order-column-names dataset)))


(defn or
  [dataset lhs-name-seq rhs-name-seq]
  (->> (c-set/union (set (select-column-names dataset lhs-name-seq))
                    (set (select-column-names dataset rhs-name-seq)))
       (ds/order-column-names dataset)))


(defn inference?
  [dataset & [column-name-seq]]
  (column-filter
   dataset
   (comp #(= % :inference) :column-type ds-col/metadata)
   column-name-seq))


(defn feature?
  [dataset & [column-name-seq]]
  (column-filter
   dataset
   (comp #(= % :feature) :column-type ds-col/metadata)
   column-name-seq))
