(ns tech.v3.dataset.impl.column-index-structure
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime.base :as datetime-base]))


(defn build-idx-to-rows-map [data]
  "Returns a mapping of data's values to its row positions(s).
   This mapping can be passed to a datastructure like TreeMap.

   Example:

     input:   [1 2 2]
     output:  {1 [0] 2 [1 2]}"
  (let [collect-row-nums (fn [memo [k v]]
                           (if-let [existing (get memo k)]
                             (assoc memo k (into existing v))
                             (assoc memo k v)))]
    (->> data
         (map-indexed (fn [row-number elem] [elem [row-number]]))
         (reduce collect-row-nums {}))))


(defn index-structure-kind [data]
  (let [data-datatype (dtype/elemwise-datatype data)]
    (cond
      (casting/numeric-type? data-datatype)
      ::numeric-type

      :else
      data-datatype)))


(defmulti make-index-structure
  "Returns an index structure based on the type of data in the column."
  (fn [data _] (index-structure-kind data)))


(defmethod make-index-structure :numeric
  [data missing]
  (let [idx-map (build-idx-to-row-map data)]
    (java.util.TreeMap. ^java.util.Map idx-map)))


(defmethod make-index-structure :object
  [data missing] nil)
