(ns tech.v3.dataset.column-index-structure
  (:require [tech.v3.protocols.column :as col-proto]))

(defn select-from-index
  "Select a subset of the index as specified by the `mode` and
  `selection-spec`. Each index-structure type supports different
  modes. By default, tech.ml.dataset supports two index types:
  `java.util.TreeMap` for non-categorical data, and `java.util.LinkedHashMap`
  for categorical data.

  Mode support for these types is as follows:

  `java.util.TreeMap` - supports `::slice` and `::pick`.
  `java.util.LinkedHashMap - support `::pick`

  Usage by mode:

  :slice
  (select-from-index :slice {:from 2 :to 5})
  (select-from-index :slice {:from 2 :from-inclusive? true :to 5 :to-inclusive? false})

  :pick
  (select-from-index :pick [:a :c :e])

  Note: Other index structures defined by other libraries may define other modes, so
        consult documentation."
  ([index-structure mode selection-spec]
   (select-from-index index-structure mode selection-spec {:as-index-structure false}))
  ([index-structure mode selection-spec options]
   (col-proto/select-from-index index-structure mode selection-spec options)))


(comment 
  (require '[tech.v3.dataset :as ds])
  (require '[tech.v3.dataset.column :as col])
  (require '[criterium.core :as c])

  (def x (col/new-column :x (range 200000)))

  (def xidx (col/index-structure x))

  (c/quick-bench
  (def x-indices (select-from-index xidx :slice {:from 100000 :to 200000})))

  (def x-indices (select-from-index xidx :slice {:from 100000 :to 200000}))



  (c/quick-bench
  (-> xidx
      (.subMap 100000 true 200000 true)
      (.values)
      (->> (reduce into (tech.v3.datatype.ListPersistentVector. [])))))
  ;; Execution time mean : 23.044314 sec

  (c/quick-bench
  (-> xidx
      (.subMap 100000 true 200000 true)

      (.values)
      (->> (reduce concat []))
      (tech.v3.datatype.ListPersistentVector.)
      ))
  ;; Execution time mean : 2.879188 ms

  (c/quick-bench
  (let [lst (java.util.ArrayList.)
        values (-> xidx
                    (.subMap 100000 true 200000 true)
                    (.values))]
    (doall (map #(.addAll lst %) values))
    lst))
  ;; Execution time mean : 11.996928 ms

  (c/quick-bench
  (let [lst (tech.v3.datatype/make-list :int64)
        values (-> xidx
                    (.subMap 100000 true 200000 true)
                    (.values))]
    (doall (map #(.addAll lst %) values))))
  ;; Execution time mean : 76.205408 ms

  (c/quick-bench
  (let [lst (tech.v3.datatype/make-list :int64)
        values (-> xidx
                    (.subMap 100000 true 200000 true)
                    (.values))]
    (doall (map #(.addAll lst %) values))))
)
