(ns tech.v3.dataset.impl.column-index-structure
  (:import [java.util TreeMap LinkedHashMap]
           [tech.v3.datatype ListPersistentVector])
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.impl.column-base :refer [column-datatype-categorical?]]
            [tech.v3.datatype :refer [elemwise-datatype clone ->buffer]]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.datatype.casting :refer [datatype->object-class]]
            [clojure.set :refer [difference]]))


(extend-type TreeMap
  col-proto/PIndexStructure
  (select-from-index [index-structure mode selection-spec]
    (case mode
      :pick
      (let [^TreeMap new-index-structure (TreeMap.)]
        (doseq [k selection-spec]
          (.put new-index-structure k (.get index-structure k)))
        new-index-structure)
      :slice
      (let [{from            :from
             from-inclusive? :from-inclusive?
             to              :to
             to-inclusive?   :to-inclusive?} selection-spec]
        (.subMap ^TreeMap index-structure
                 from
                 (if (nil? from-inclusive?) true from-inclusive?)
                 to
                 (if (nil? to-inclusive?) true to-inclusive?))))))


(extend-type LinkedHashMap
  col-proto/PIndexStructure
  (select-from-index [index-structure mode selection-spec]
    (case mode
      :pick
      (let [^LinkedHashMap new-index-structure (LinkedHashMap.)]
        (doseq [k selection-spec]
            (.put new-index-structure k (.get index-structure k)))
        new-index-structure))))


(defn build-value-to-index-position-map [column-data]
  (let [idx-map (arggroup column-data)
        vals->list-persistent-vector (fn [_ list-data]
                                       (-> (clone list-data) ->buffer ListPersistentVector.))]
    (.replaceAll idx-map (reify java.util.function.BiFunction
                           (apply [this k v]
                             (vals->list-persistent-vector k v))))
    idx-map))


(defmulti make-index-structure
  "Returns an index structure based on the type of data in the column."
  (fn [data metadata]
    (if (= (:datatype metadata) :object)
      (elemwise-datatype data)
      (:datatype metadata))))


(defmethod make-index-structure :default
  [data metadata]
  (let [^java.util.Map idx-map (build-value-to-index-position-map data)
        data-datatype (elemwise-datatype data)]
    (if (contains? metadata :categorical?)
      (if (:categorical? metadata)
        (LinkedHashMap. idx-map)
        (TreeMap. idx-map))
      (if (column-datatype-categorical? data-datatype)
        (LinkedHashMap. idx-map)
        (TreeMap. idx-map)))))
