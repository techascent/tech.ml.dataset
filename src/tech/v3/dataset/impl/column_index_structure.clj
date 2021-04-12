(ns tech.v3.dataset.impl.column-index-structure
  (:import [java.util TreeMap LinkedHashMap]
           [tech.v3.datatype ListPersistentVector])
  (:require [tech.v3.datatype :refer [elemwise-datatype clone ->buffer]]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.datatype.casting :refer [datatype->object-class]]
            [clojure.set :refer [difference]]
            ))



(defprotocol PIndexStructure
  (select-from-index
    [index-structure mode selection-spec]
    "Select a subset of the index. Supports a variety of modes."))


(extend-type TreeMap
  PIndexStructure
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
        (.subMap ^TreeMap index-structure from (or from-inclusive? true) to (or to-inclusive? true))))))


(extend-type LinkedHashMap
  PIndexStructure
  (select-from-index [index-structure mode selection-spec]
    (case mode
      ::pick
      (let [^LinkedHashMap new-index-structure (.clone ^LinkedHashMap index-structure)
            s (difference (set (.keySet new-index-structure)) (set selection-spec))]
        (doseq [k s]
          (.remove new-index-structure k))
        new-index-structure))))


(defn build-value-to-index-position-map [column-data]
  (let [idx-map (arggroup column-data)
        vals->list-persistent-vector (fn [_ list-data]
                                       (println (-> (clone list-data) ->buffer ListPersistentVector.))
                                       (-> (clone list-data) ->buffer ListPersistentVector.))]
    (.replaceAll idx-map (reify java.util.function.BiFunction
                           (apply [this k v]
                             (vals->list-persistent-vector k v))))
    idx-map))


(defmulti make-index-structure
  "Returns an index structure based on the type of data in the column."
  (fn [data]
    (-> data elemwise-datatype datatype->object-class)))


(defmethod make-index-structure ::categorical
  [data]
  (let [idx-map (build-value-to-index-position-map data)]
    (LinkedHashMap. ^java.util.Map idx-map)))

;; When tech.datatype does not know what something is it describes it
;; as an object (see tech.v3.datatype.casting/elemwise-datatype). This
;; dispatch method then serves as a default unless someone has extended
;; this multimethod to catch a more specific datatype.
(defmethod make-index-structure java.lang.Object
  [data]
  (let [idx-map (build-value-to-index-position-map data)]
    (TreeMap. ^java.util.Map idx-map)))


;; Build a custom hierarchy to identify categorical types. The types
;; identified here matche the set identified as categorical in
;; the check in tech.v3.dataset.column/new-column that sets the `categorical?`
;; metadata, see:
;; https://github.com/techascent/tech.ml.dataset/blob/master/src/tech/v3/dataset/impl/column.clj#L41
(derive java.lang.String ::categorical)
(derive clojure.lang.Keyword ::categorical)
(derive clojure.lang.Symbol ::categorical)
(prefer-method make-index-structure ::categorical java.lang.Object)

;; symbols not loaded into dtype-next's type system by default
(tech.v3.datatype.casting/add-object-datatype! :symbol clojure.lang.Symbol true)
