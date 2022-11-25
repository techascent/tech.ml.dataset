(ns tech.v3.dataset.set
  "Extensions to datasets to do bag-semantics set/union and intersection."
  (:require [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.protocols :as ds-proto]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiConsumer]
           [java.util Map]
           [ham_fisted BitmapTrieCommon]))




(defn- concurrent-hashmap-frequencies
  [data]
  (hamf/preduce
   (constantly (hamf/java-concurrent-hashmap))
   (fn [acc v]
     (.compute ^Map acc v BitmapTrieCommon/incBiFn)
     acc)
   (fn [l r] l)
   {:min-n 1000}
   data))


(defn- concurrent-hashmap-intersection
  [bifn ^ConcurrentHashMap l ^ConcurrentHashMap r]
  (let [retval (hamf/java-concurrent-hashmap)
        bifn (hamf/->bi-function bifn)]
    (.forEach l 100 (reify java.util.function.BiConsumer
                      (accept [this k v]
                        (let [ov (.getOrDefault r k ::not-found)]
                          (when-not (identical? ov ::not-found)
                            (.put retval k (.apply bifn v ov)))))))
    retval))


(defn- concurrent-hashmap-union
  [bifn ^ConcurrentHashMap l ^ConcurrentHashMap r]
  (let [retval (hamf/java-concurrent-hashmap l)
        bifn (hamf/->bi-function bifn)]
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
  ([options datasets]
   (->> datasets
        (reduce (fn [acc ds]
                  (let [rows (ds-proto/rows ds {:copying? true})]
                    (if acc
                      (->> rows
                           (lznc/filter (hamf/predicate
                                         v (.containsKey ^java.util.Map acc v)))
                           (concurrent-hashmap-frequencies)
                           (concurrent-hashmap-intersection
                            (hamf/bi-function l r (min (long l) (long r)))
                            acc))
                      (concurrent-hashmap-frequencies rows))))
                nil)
        (expand-setop-result options)))
  ([datasets] (reduce-intersection nil datasets)))


(defn reduce-union
  ([options datasets]
   (->> datasets
        (reduce (fn [acc ds]
                  (let [rows (ds-proto/rows ds {:copying? true})]
                    (if acc
                      (->> rows
                           (concurrent-hashmap-frequencies)
                           (concurrent-hashmap-union
                            (hamf/bi-function l r (min (long l) (long r)))
                            acc))
                      (concurrent-hashmap-union rows))))
                nil)
        (expand-setop-result options)))
  ([datasets] (reduce-union nil datasets)))


(comment
  (defn intersection-clj
  [& datasets]
  (->> datasets
       (map (comp frequencies #(ds/rows % {:copying? true})))
       (reduce (fn [acc m]
                 (mapcat (fn [[el n]]
                           (let [new-n (min n (get m el 0))]
                             (when (pos? new-n)
                               [[el new-n]])))
                         acc)))
       (mapcat (fn [[el n]] (repeat n el)))
       ds/->dataset))


(defn intersection-value-space
  [& datasets]
  (->> datasets
       (reduce (fn [acc ds]
                 (let [rows (ds/rows ds {:copying? true})]
                   (if acc
                     (->> rows
                          (lznc/filter (hamf/predicate
                                        v (.containsKey ^java.util.Map acc v)))
                          (hamf/frequencies)
                          (hamf/map-intersection
                           (hamf/bi-function l r (min (long l) (long r)))
                           acc))
                     (hamf/frequencies rows))))
               nil)
       (lznc/map #(assoc (key %) :count (val %)))
       (ds/->>dataset)))


(defn- concurrent-hashmap-frequencies
  [data]
  (hamf/preduce
   (constantly (hamf/java-concurrent-hashmap))
   (fn [acc v]
     (.compute ^Map acc v BitmapTrieCommon/incBiFn)
     acc)
   (fn [l r] l)
   {:min-n 1000}
   data))


(defn- concurrent-hashmap-intersection
  [bifn ^ConcurrentHashMap l ^ConcurrentHashMap r]
  (let [retval (hamf/java-concurrent-hashmap)
        bifn (hamf/->bi-function bifn)]
    (.forEach l 100 (reify java.util.function.BiConsumer
                      (accept [this k v]
                        (let [ov (.getOrDefault r k ::not-found)]
                          (when-not (identical? ov ::not-found)
                            (.put retval k (.apply bifn v ov)))))))
    retval))


(defn intersection-value-space-concurrent-hashmap
  [& datasets]
  (->> datasets
       (reduce (fn [acc ds]
                 (let [rows (ds/rows ds {:copying? true})]
                   (if acc
                     (->> rows
                          (lznc/filter (hamf/predicate
                                        v (.containsKey ^java.util.Map acc v)))
                          (concurrent-hashmap-frequencies)
                          (concurrent-hashmap-intersection
                           (hamf/bi-function l r (min (long l) (long r)))
                           acc))
                     (concurrent-hashmap-frequencies rows))))
               nil)
       (lznc/map #(assoc (key %) :count (val %)))
       (ds/->>dataset)))


(defn intersection-index-space
  [& datasets]
  (->> datasets
       (reduce
        (fn [acc ds]
          (let [rows (ds/rows ds {:copying? true})
                n-rows (dtype/ecount rows)]
            (if acc
              (->> (hamf/range n-rows)
                   (lznc/filter (hamf/long-predicate
                                 v (.containsKey ^java.util.Map acc (rows v))))
                   (hamf/group-by-reduce rows
                                         #(dtype/make-list :int32)
                                         (hamf/long-accumulator
                                          acc v (do (.addLong ^IMutList acc v) acc))
                                         #(do (.addAll ^IMutList %1 ^IMutList %2) %1))
                   (hamf/map-intersection
                    (hamf/bi-function l r (hamf/subvec l 0 (min (count l) (count r))))
                    acc))
              (argops/arggroup rows))))
        nil)
       (hamf/vals)
       (apply lznc/concat)
       (hamf/int-array-list)
       (#(with-meta % {:min 0 :max (count %)}))
       (ds/select-rows (first datasets))))


(def ds-a (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 2 :b 3}]))
(def ds-b (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 3 :b 3}]))


(defonce big-ds-a (ds/->dataset {:a (lznc/repeatedly 20000 #(rand-int 337))
                                 :b (cycle [:a :b :c :d :e])
                                 :c (cycle [:d :e :f :g :h])}))



(defonce big-ds-b (ds/->dataset {:a (lznc/repeatedly 20000 #(rand-int 337))
                                 :b (cycle [:a :b :c :d :e])
                                 :c (cycle [:d :e :f :g :h])}))
  )
