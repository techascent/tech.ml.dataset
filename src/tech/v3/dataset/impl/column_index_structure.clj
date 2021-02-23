(ns tech.v3.dataset.impl.column-index-structure
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]))

;; TODO: Make this more efficient. Maybe can use an efficient group-by operation?
(defn build-idx-to-row-map [data]
  (let [collect-row-nums (fn [memo [k v]]
                           (if-let [existing (get memo k)]
                             (assoc memo k (into existing v))
                             (assoc memo k v)))]
    (->> data
         (map-indexed (fn [row-number elem]
                        [elem [row-number]]))
         (reduce collect-row-nums {}))))


;; Seems like this might better belong in tech.datatype.datetime
(defn datetime-datatype? [dtype]
  (boolean (tech.v3.datatype.datetime.base/datatypes dtype)))


(defn index-structure-kind [data]
  (let [data-datatype (dtype/elemwise-datatype data)]
    (cond
      (casting/numeric-type? data-datatype)
      :numeric

      (datetime-datatype? data-datatype)
      :datetime

      :else
      data-datatype)))


(defmulti make-index-structure
  (fn [data missing] ()index-structure-kind))


(defmethod make-index-structure :numeric
  [data missing]
  (let [idx-map (build-idx-to-row-map data)]
    (println {:dispatch-type :numeric :data data :idx-map idx-map})
    (java.util.TreeMap. ^java.util.Map idx-map)))


(defmethod make-index-structure :datetime
  [data missing]
  (let [idx-map (build-idx-to-row-map data)]
    (println {:dispatch-type :datetime :data data :idx-map idx-map})
    (java.util.TreeMap. ^java.util.Map idx-map)))


 (defmethod make-index-structure :object
  [data missing] nil)


(comment
  (def cx (new-column "x"
                      (dtype/make-reader :float32 9 (rand))
                      nil
                      nil))
  (index-structure cx)


  (def cy (new-column "y"
                      (dtype/make-reader :string 9 "hi!")
                      nil
                      nil))

  (class (index-structure cy))

  (require '[tech.v3.datatype.datetime :as dtdt])

  (def cz
    (new-column
     "z"
     (dtype/make-reader :local-date
                        10
                        (dtdt/plus-temporal-amount (dtdt/local-date) idx :days))
      ;; (dtype/make-reader :local-date 10 (dtdt/local-date))
      nil
      nil))

  (-> cz
      (dtype/elemwise-datatype)
      ;; (casting/un-alias-datatype)
      ;; (datetime-datatype?)
      ;; (casting/numeric-type?)
      )

  (index-structure cz)

  )
