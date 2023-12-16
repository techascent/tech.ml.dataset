(ns ^:no-doc tech.v3.dataset.utils
  (:require [clojure.string :as str])
  (:import [java.util Iterator]))


(defn nanos->millis
  ^long [^long nanos]
  (-> (/ nanos 1000000.0)
      (Math/round)))


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
  (.iterator ^Iterable (seq item-seq)))


(defn set-slf4j-log-level
  "Set the slf4j log level.  Safe to call if slf4j is not in the
  classpath.  Upon success, returns a keyword.  Upon failure, returns
  a map with {:exception} pointing to the failure."
  [level]
  (locking #'sequence->iterator
    (try
      ((requiring-resolve
        'tech.v3.dataset.utils.slf4j-log-level/set-log-level) level)
      (catch Throwable e
        {:exception e}))))


(defn column-safe-name
  "Given a generic item (keyword, symbol) create a string that safe to be used
  to name columns."
  ^String [item-name]
  (cond
    (or (keyword? item-name)
        (symbol? item-name))
    (if (namespace item-name)
      (str (namespace item-name) "/" (name item-name))
      (str (name item-name)))
    (boolean? item-name)
    (if item-name "true" "false")
    :else
    (str item-name)))


(defn rand-str
  "Generates a random string of length `n` composed of alphanumeric characters."
  [n]
  (apply str (repeatedly n #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))

(defn remove-zero-width-spaces
  "Remove zero-width non-breaking spaces (ZWNBSP) from a string.
  These non-breaking spaces often occur in the beginning of CSV-files."
  [s]
  (str/replace s #"\uFEFF" ""))

(defn rand-str-column-name-postfix
  "Generates a random string postfix for a column name."
  [col-idx colname]
  (str (column-safe-name colname) "_" (rand-str 5)))
