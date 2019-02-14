(ns tech.ml.utils
  (:require [tech.parallel :as parallel])
  (:import [java.util Iterator NoSuchElementException]))


(defn nanos->millis
  ^long [^long nanos]
  (-> (/ nanos 1000000.0)
      (Math/round)
      long))


(defmacro time-section
  "Time a section, return
  {:retval retval
  :milliseconds ms}"
  [& body]
  `(let [start-time# (System/nanoTime)
         retval# (do ~@body)
         stop-time# (System/nanoTime)]
     {:retval retval#
      :milliseconds (-> (- stop-time# start-time#)
                        nanos->millis)}))


(defn prefix-merge
  [prefix src-map merge-map]
  (merge src-map
         (->> merge-map
              (map (fn [[item-k item-v]]
                     [(keyword (str prefix "-" (name item-k))) item-v]))
              (into {}))))



(defn sequence->iterator
  "Java ml interfaces sometimes use iterators where they really should
  use sequences (iterators have state).  In any case, we do what we can."
  ^Iterator [item-seq]
  (let [next-item-fn (parallel/create-next-item-fn item-seq)
        next-item-atom (atom (next-item-fn))]
    (proxy [Iterator] []
      (hasNext []
        (boolean @next-item-atom))
      (next []
        (locking this
          (if-let [entry @next-item-atom]
            (do
              (reset! next-item-atom (next-item-fn))
              entry)
            (throw (NoSuchElementException.))))))))
