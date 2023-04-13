(ns tech.v3.dataset.set
  "Extensions to datasets to do per-row bag-semantics set/union and intersection."
  (:require [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.datatype :as dtype]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiConsumer]
           [java.util Map HashSet]
           [tech.v3.datatype Buffer]
           [ham_fisted BitmapTrieCommon]))


(defn- concurrent-hashmap-frequencies
  [data]
  (hamf-rf/preduce
   (constantly (hamf/java-concurrent-hashmap))
   (fn [acc v]
     (.compute ^Map acc v BitmapTrieCommon/incBiFn)
     acc)
   (fn [l r] l)
   {:min-n 1000}
   data))


(defn- concurrent-hashmap-intersection
  [bifn ^ConcurrentHashMap l ^ConcurrentHashMap r]
  (let [[^ConcurrentHashMap minmap ^ConcurrentHashMap maxmap]
        (if (< (.size l) (.size r))
          [l r] [r l])
        retval (hamf/java-concurrent-hashmap)
        bifn (hamf-fn/->bi-function bifn)]
    (.forEach minmap 100 (reify java.util.function.BiConsumer
                           (accept [this k v]
                             (let [ov (.getOrDefault maxmap k ::not-found)]
                               (when-not (identical? ov ::not-found)
                                 (.put retval k (.apply bifn v ov)))))))
    retval))


(defn- concurrent-hashmap-union
  [bifn ^ConcurrentHashMap l ^ConcurrentHashMap r]
  (let [retval (hamf/java-concurrent-hashmap l)
        bifn (hamf-fn/->bi-function bifn)]
    (.forEach r 100 (reify BiConsumer
                      (accept [this k v]
                        (let [ov (.getOrDefault l k ::not-found)]
                          (if-not (identical? ov ::not-found)
                            (.put retval k (.apply bifn ov v))
                            (.put retval k v))))))
    retval))



(defn- expand-setop-result
  [options ds-map]
  (if-let [count-attname (get options :count)]
    (->> ds-map
         (lznc/map #(assoc (key %) count-attname (val %)))
         (ds-io/->>dataset))
    (->> ds-map
         (lznc/map #(hamf/repeat (val %) (key %)))
         (apply lznc/concat)
         (ds-io/->>dataset))))


(defn reduce-intersection
  "Given a sequence of datasets, union the rows such that tuples that exist in all datasets
  appear in the final dataset at their mininum repetition amount.  Can return either a
  dataset with duplicate tuples or a dataset with a :count column.

  Options:

  * `:count` - Name of count column, if nil then tuples are duplicated and count is implicit.

```clojure
user> (def ds-a (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 2 :b 3}]))
#'user/ds-a
user> (def ds-b (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 3 :b 3}]))
#'user/ds-b
user> (ds-set/reduce-intersection [ds-a ds-b])
_unnamed [2 2]:

| :a | :b |
|---:|---:|
|  1 |  2 |
|  1 |  2 |
user> (ds-set/reduce-intersection {:count :count} [ds-a ds-b])
_unnamed [1 3]:

| :a | :b | :count |
|---:|---:|-------:|
|  1 |  2 |      2 |
```"
  ([options datasets]
   (->> datasets
        (reduce (fn [acc ds]
                  (let [rows (ds-proto/rows ds {:copying? true})]
                    (if acc
                      (->> rows
                           (lznc/filter (hamf-fn/predicate
                                         v (.containsKey ^java.util.Map acc v)))
                           (concurrent-hashmap-frequencies)
                           (concurrent-hashmap-intersection
                            (hamf-fn/bi-function l r (min (long l) (long r)))
                            acc))
                      (concurrent-hashmap-frequencies rows))))
                nil)
        (expand-setop-result options)))
  ([datasets] (reduce-intersection nil datasets)))


(defn reduce-union
  "Given a sequence of datasets, union the rows such that all tuples appear in the final
  dataset at their maximum repetition amount.  Can return either a dataset with duplicate
  tuples or a dataset with a :count column.

  Options:

  * `:count` - Name of count column, if nil then tuples are duplicated and count is implicit.

```clojure
user> (def ds-a (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 2 :b 3}]))
#'user/ds-a
user> (def ds-b (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 3 :b 3}]))
#'user/ds-b
user> (ds-set/reduce-union [ds-a ds-b])
_unnamed [4 2]:

| :a | :b |
|---:|---:|
|  2 |  3 |
|  3 |  3 |
|  1 |  2 |
|  1 |  2 |
user> (ds-set/reduce-union {:count :count} [ds-a ds-b])
_unnamed [3 3]:

| :a | :b | :count |
|---:|---:|-------:|
|  2 |  3 |      1 |
|  3 |  3 |      1 |
|  1 |  2 |      2 |
```"
  ([options datasets]
   (->> datasets
        (reduce (fn [acc ds]
                  (let [rows (ds-proto/rows ds {:copying? true})]
                    (if acc
                      (->> rows
                           (concurrent-hashmap-frequencies)
                           (concurrent-hashmap-union
                            (hamf-fn/bi-function l r (min (long l) (long r)))
                            acc))
                      (concurrent-hashmap-frequencies rows))))
                nil)
        (expand-setop-result options)))
  ([datasets] (reduce-union nil datasets)))


(defn union
  "Union two datasets producing a new dataset with the union of tuples.  Repeated tuples will
  be repeated in final dataset at their maximum per-dataset repetition count."
  ([a] a)
  ([a b] (reduce-union [a b]))
  ([a b & args] (reduce-union (lznc/concat [a b] args))))


(defn intersection
  "Intersect two datasets producing a new dataset with the union of tuples.
  Tuples repeated across all datasets repeated in final dataset at their minimum
  per-dataset repetition count."
  ([a] a)
  ([a b] (reduce-intersection [a b]))
  ([a b & args] (reduce-intersection (lznc/concat [a b] args))))


(defn difference
  "Remove tuples from a that also appear in b."
  ([a] a)
  ([a b]
   (let [b-rows (ds-proto/rows b {:copying? true})
         ^HashSet s (->> (hamf/upgroups
                          (dtype/ecount b-rows)
                          (fn [^long sidx ^long eidx]
                            (doto (HashSet.)
                              (.addAll (.subBuffer ^Buffer b-rows sidx eidx)))))
                         (reduce #(do (.addAll ^HashSet %1 ^HashSet %2) %1)))]
     (ds-base/filter a (hamf-fn/predicate r (not (.contains s r)))))))
