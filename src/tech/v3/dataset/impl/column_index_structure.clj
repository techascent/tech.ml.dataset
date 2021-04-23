(ns tech.v3.dataset.impl.column-index-structure
  (:import [java.util TreeMap LinkedHashMap]
           [tech.v3.datatype ListPersistentVector])
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.datatype :refer [elemwise-datatype clone ->buffer]]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.datatype.casting :refer [datatype->object-class]]
            [tech.v3.dataset.column-index :as col-index]
            [clojure.set :refer [difference]]))


(extend-type TreeMap
  col-proto/PIndexStructure
  (select-from-index [index-structure mode selection-spec]
    (case mode
      ::col-index/pick
      (let [^TreeMap new-index-structure (.clone ^TreeMap index-structure)
            s (difference (set (.keySet new-index-structure)) (set selection-spec))]
        (doseq [k s]
          (.remove new-index-structure k))
        new-index-structure)
      ::col-index/slice
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
      ::col-index/pick
      (let [^LinkedHashMap new-index-structure (.clone ^LinkedHashMap index-structure)
            s (difference (set (.keySet new-index-structure)) (set selection-spec))]
        (doseq [k s]
          (.remove new-index-structure k))
        new-index-structure))))


;; https://github.com/techascent/tech.ml.dataset/blob/master/src/tech/v3/dataset/impl/column.clj#L41
(defn categorical? [dtype]
  (boolean (#{:string :keyword :symbol} dtype)))


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
    (let [data-dtype (-> data elemwise-datatype)
          data-klass (-> data-dtype datatype->object-class)]
      (if (contains? metadata :categorical?)
        (if (:categorical? metadata)
          ::categorical
          data-klass)
        (if (categorical? data-dtype)
          ::categorical
          data-klass)))))


(defmethod make-index-structure ::categorical
  [data _]
  (let [idx-map (build-value-to-index-position-map data)]
    (LinkedHashMap. ^java.util.Map idx-map)))


(defmethod make-index-structure :default
  [data _]
  (let [idx-map (build-value-to-index-position-map data)]
    (TreeMap. ^java.util.Map idx-map)))
