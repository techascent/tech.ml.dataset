(ns tech.v3.dataset.impl.column-index-structure
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argops :refer [arggroup]]))


(defn index-structure-kind [data]
  (let [data-datatype (dtype/elemwise-datatype data)]
    (cond
      (casting/numeric-type? data-datatype)
      ::numeric

      :else
      data-datatype)))


(defmulti make-index-structure
  "Returns an index structure based on the type of data in the column."
  (fn [data _] (index-structure-kind data)))


(defmethod make-index-structure ::numeric
  [data missing]
  (let [idx-map (arggroup data)]
    (java.util.TreeMap. ^java.util.Map idx-map)))


(defmethod make-index-structure :object
  [data missing] nil)
