(ns tech.ml.dataset.column-filters
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col])
  (:refer-clojure :exclude [string?]))


(defn of-datatype?
  "Return column-names  of a given datatype."
  [dataset datatype]
  (->> (ds/columns dataset)
       (filter #(= datatype
                   (dtype/get-datatype %)))
       (map ds-col/column-name)))


(defn string?
  [dataset]
  (of-datatype? dataset :string))


(defn numeric?
  [dataset]
  (->> (ds/columns dataset)
       (filter #(casting/numeric-type?
                 (dtype/get-datatype %)))))
