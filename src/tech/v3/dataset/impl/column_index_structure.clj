(ns tech.v3.dataset.impl.column-index-structure
  (:import [java.util TreeMap])
  (:require [tech.v3.datatype :refer [elemwise-datatype]]
            [tech.v3.datatype.argops :refer [arggroup]]
            [tech.v3.datatype.casting :refer [datatype->object-class]]))


(defprotocol PIndexStructure
  (slice-index
    [index-structure from to]
    [index-structure from from-inclusive? to to-inclusive?]
    "Slice by keys or range"))


(extend-type TreeMap
  PIndexStructure
  (slice-index
    ([index-structure from to]
     (slice-index index-structure from true to true))
    ([index-structure from from-inclusive? to to-inclusive?]
     (-> (.subMap ^TreeMap index-structure from from-inclusive? to to-inclusive?)))))


(defmulti make-index-structure
  "Returns an index structure based on the type of data in the column."
  (fn [data]
    (println {:datatype (elemwise-datatype data) :klass (-> data elemwise-datatype datatype->object-class)})
    (-> data elemwise-datatype datatype->object-class)))


(defmethod make-index-structure ::categorical
  [data]
  (let [idx-map (arggroup data)]
    (java.util.LinkedHashMap. ^java.util.Map idx-map)))

;; When tech.datatype does not know what something is it describes it
;; as an object (see tech.v3.datatype.casting/elemwise-datatype). This
;; dispatch method then serves as a default unless someone has extended
;; this multimethod to catch a more specific datatype.
(defmethod make-index-structure java.lang.Object
  [data]
  (let [idx-map (arggroup data)]
    (TreeMap. ^java.util.Map idx-map)))


;; Build a custom hierarchy to identify categorical types. The types
;; identified here matches the set identified as categorical in
;; the check in tech.v3.dataset.column/new-column that sets the `categorical?`
;; metadata, see:
;; https://github.com/techascent/tech.ml.dataset/blob/master/src/tech/v3/dataset/impl/column.clj#L41
(derive java.lang.String ::categorical)
(derive clojure.lang.Keyword ::categorical)
(derive clojure.lang.Symbol ::categorical)
(prefer-method make-index-structure ::categorical java.lang.Object)

;; symbols not loaded into dtype-next's type system by default
(tech.v3.datatype.casting/add-object-datatype! :symbol clojure.lang.Symbol true)
