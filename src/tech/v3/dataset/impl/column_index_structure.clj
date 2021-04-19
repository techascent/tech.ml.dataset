(ns tech.v3.dataset.impl.column-index-structure
  (:import [java.util TreeMap LinkedHashMap]
           [tech.v3.datatype ListPersistentVector])
  (:require [tech.v3.protocols.column :as col-proto]
            [tech.v3.datatype :refer [elemwise-datatype clone ->buffer]]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.datatype.casting :refer [datatype->object-class]]
            [clojure.set :refer [difference]]))


(defn select-from-index [index-structure mode selection-spec]
  (col-proto/select-from-index index-structure mode selection-spec))


(extend-type TreeMap
  col-proto/PIndexStructure
  (select-from-index [index-structure mode selection-spec]
    (case mode
      ::pick
      (let [^TreeMap new-index-structure (.clone ^TreeMap index-structure)
            s (difference (set (.keySet new-index-structure)) (set selection-spec))]
        (doseq [k s]
          (.remove new-index-structure k))
        new-index-structure)
      ::slice
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
      ::pick
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
  (fn [data meta]
    (let [meta-marked-categorical? (:categorical meta)
          data-dtype (-> data elemwise-datatype)
          data-klass (-> data-dtype datatype->object-class)]
      (if (nil? meta-marked-categorical?)
        (if (categorical? data-dtype)
          ::categorical
          data-klass)
        (if meta-marked-categorical?
          ::categorical
          data-klass)))))


(defmethod make-index-structure ::categorical
  [data _]
  (let [idx-map (build-value-to-index-position-map data)]
    (LinkedHashMap. ^java.util.Map idx-map)))

;; When tech.datatype does not know what something is it describes it
;; as an object (see tech.v3.datatype.casting/elemwise-datatype). This
;; dispatch method then serves as a default unless someone has extended
;; this multimethod to catch a more specific datatype.
(defmethod make-index-structure :default
  [data _]
  (let [idx-map (build-value-to-index-position-map data)]
    (TreeMap. ^java.util.Map idx-map)))
