(ns tech.v3.dataset.impl.column-index-structure
  (:import [java.util Map TreeMap LinkedHashMap]
           [tech.v3.datatype ListPersistentVector])
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.impl.column-base :refer [column-datatype-categorical?]]
            [tech.v3.datatype :refer [elemwise-datatype clone ->buffer]]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.datatype.casting :refer [datatype->object-class]]
            [clojure.set :refer [difference]]))


(defn- pick-from-index-structure [keys index-structure new-index-structure]
  (doseq [k keys]
    (.put ^Map new-index-structure k (.get ^Map index-structure k)))
  new-index-structure)


(extend-type TreeMap
  col-proto/PIndexStructure
  (select-from-index [index-structure mode selection-spec {:keys [as-index-structure]
                                                          :or {as-index-structure false}}]
   (case mode
     :pick
     (let [picked-map (pick-from-index-structure selection-spec
                                              index-structure
                                              ^TreeMap (TreeMap.))]
       (if as-index-structure
         picked-map
         (reduce into (ListPersistentVector. []) (.values picked-map))))
     :slice
     (let [{from            :from
            from-inclusive? :from-inclusive?
            to              :to
            to-inclusive?   :to-inclusive?} selection-spec
           submap (.subMap ^TreeMap index-structure
                    from
                    (if (nil? from-inclusive?) true from-inclusive?)
                    to
                    (if (nil? to-inclusive?) true to-inclusive?))]
       (if as-index-structure
         submap
         (reduce into (ListPersistentVector. []) (.values submap)))))))


(extend-type LinkedHashMap
  col-proto/PIndexStructure
  (select-from-index [index-structure mode selection-spec {:keys [as-index-structure]
                                                           :or {as-index-structure false}}]
    (case mode
      :pick
      (let [picked-map (pick-from-index-structure selection-spec
                                               index-structure
                                               ^LinkedHashMap (LinkedHashMap.))]
        (if as-index-structure
          picked-map
          (reduce into (ListPersistentVector. []) (.values picked-map)))))))


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
  (fn [data _] (elemwise-datatype data)))



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
