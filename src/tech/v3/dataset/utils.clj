(ns ^:no-doc tech.ml.dataset.utils
  (:require [tech.parallel.require :as parallel-req]
            [tech.parallel.next-item-fn :as parallel-nfn]
            [tech.v2.datatype.casting :as casting])
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
  (let [next-item-fn (parallel-nfn/create-next-item-fn item-seq)
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


(defn set-slf4j-log-level
  "Set the slf4j log level.  Safe to call if slf4j is not in the
  classpath."
  [level]
  (try
    ((parallel-req/require-resolve
      'tech.ml.dataset.utils.slf4j-log-level/set-log-level) level)
    (catch Throwable e
      :exception)))


(defn column-safe-name
  ^String [item-name]
  (cond
    (and (or (keyword? item-name)
             (symbol? item-name)))
    (if (namespace item-name)
      (str (namespace item-name) "/" (name item-name))
      (str (name item-name)))
    (boolean? item-name)
    (if item-name "true" "false")
    :else
    (str item-name)))


(defn extend-column-name
  [src-name dst-name]
  (let [dst-name (if (or (keyword? dst-name)
                         (symbol? dst-name))
                   (name dst-name)
                   (str dst-name))
        new-name (str (column-safe-name src-name) "-" dst-name)]
    (cond
      (keyword? src-name) (keyword new-name)
      (symbol? src-name) (symbol new-name)
      :else src-name)))


(defn numeric-datatype?
  [dtype-enum]
  (boolean (casting/numeric-type? dtype-enum)))
