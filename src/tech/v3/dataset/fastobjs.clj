(ns tech.v3.dataset.fastobjs
  (:require [tech.v3.datatype.imlist :refer [imlist] :as imlist]
            [tech.v3.datatype.immap :refer [immap] :as immap]
            [tech.v3.parallel.for :as pfor])
  (:import [clojure.lang MapEntry RT PersistentArrayMap PersistentHashMap]
           [tech.v3.datatype ArrayHelpers]
           [tech.v3.dataset FastStruct]
           [java.util Map ArrayList]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ^:no-doc generic-create-map
  [keyvec vals]
  (let [kc (count keyvec)
        vals (vec vals)
        _ (when-not (= (count vals) kc)
            (throw (Exception. "Key/val count mismatch")))
        objs (object-array (* 2 kc))]
    (loop [idx 0]
      (when (< idx kc)
        (do
          (ArrayHelpers/aset objs (* 2 idx) (keyvec idx))
          (ArrayHelpers/aset objs (+ 1 (* 2 idx)) (vals idx))
          (recur (unchecked-inc idx)))))
    (if (< kc 16)
      (PersistentArrayMap. objs)
      (PersistentHashMap/create nil objs))))


(defn map-factory
  "Return an IFn that efficiently creates a map with exactly these keys."
  [keys]
  (let [keys (vec keys)]
    (when (not= (count keys)
                (count (distinct keys)))
      (throw (Exception. (str "Duplicate keys detected: " (vec keys))))))
  ;;Since we know there are no duplicates we can use fastpaths to create the map
  (case (count keys)
    0 (constantly {})
    1 (let [k (first keys)]
        (fn [v] {k v}))
    2 (let [[k1 k2] keys]
        (fn [v1 v2]
          (let [data (object-array 4)]
            (ArrayHelpers/aset data 0 k1)
            (ArrayHelpers/aset data 1 v1)
            (ArrayHelpers/aset data 2 k2)
            (ArrayHelpers/aset data 3 v2)
            (PersistentArrayMap. data))))
    3 (let [[k1 k2 k3] keys]
        (fn [v1 v2 v3]
          (let [data (object-array 6)]
            (ArrayHelpers/aset data 0 k1)
            (ArrayHelpers/aset data 1 v1)
            (ArrayHelpers/aset data 2 k2)
            (ArrayHelpers/aset data 3 v2)
            (ArrayHelpers/aset data 4 k3)
            (ArrayHelpers/aset data 5 v3)
            (PersistentArrayMap. data))))
    4 (let [[k1 k2 k3 k4] keys]
        (fn [v1 v2 v3 v4]
          (let [data (object-array 8)]
            (ArrayHelpers/aset data 0 k1)
            (ArrayHelpers/aset data 1 v1)
            (ArrayHelpers/aset data 2 k2)
            (ArrayHelpers/aset data 3 v2)
            (ArrayHelpers/aset data 4 k3)
            (ArrayHelpers/aset data 5 v3)
            (ArrayHelpers/aset data 6 k4)
            (ArrayHelpers/aset data 7 v4)
            (PersistentArrayMap. data))))
    5 (let [[k1 k2 k3 k4 k5] keys]
        (fn [v1 v2 v3 v4 v5]
          (let [data (object-array 10)]
            (ArrayHelpers/aset data 0 k1)
            (ArrayHelpers/aset data 1 v1)
            (ArrayHelpers/aset data 2 k2)
            (ArrayHelpers/aset data 3 v2)
            (ArrayHelpers/aset data 4 k3)
            (ArrayHelpers/aset data 5 v3)
            (ArrayHelpers/aset data 6 k4)
            (ArrayHelpers/aset data 7 v4)
            (ArrayHelpers/aset data 8 k5)
            (ArrayHelpers/aset data 9 v5)
            (PersistentArrayMap. data))))
    6 (let [[k1 k2 k3 k4 k5 k6] keys]
        (fn [v1 v2 v3 v4 v5 v6]
          (let [data (object-array 12)]
            (ArrayHelpers/aset data 0 k1)
            (ArrayHelpers/aset data 1 v1)
            (ArrayHelpers/aset data 2 k2)
            (ArrayHelpers/aset data 3 v2)
            (ArrayHelpers/aset data 4 k3)
            (ArrayHelpers/aset data 5 v3)
            (ArrayHelpers/aset data 6 k4)
            (ArrayHelpers/aset data 7 v4)
            (ArrayHelpers/aset data 8 k5)
            (ArrayHelpers/aset data 9 v5)
            (ArrayHelpers/aset data 10 k6)
            (ArrayHelpers/aset data 11 v6)
            (PersistentArrayMap. data))))
    7 (let [[k1 k2 k3 k4 k5 k6 k7] keys]
        (fn [v1 v2 v3 v4 v5 v6 v7]
          (let [data (object-array 14)]
            (ArrayHelpers/aset data 0 k1)
            (ArrayHelpers/aset data 1 v1)
            (ArrayHelpers/aset data 2 k2)
            (ArrayHelpers/aset data 3 v2)
            (ArrayHelpers/aset data 4 k3)
            (ArrayHelpers/aset data 5 v3)
            (ArrayHelpers/aset data 6 k4)
            (ArrayHelpers/aset data 7 v4)
            (ArrayHelpers/aset data 8 k5)
            (ArrayHelpers/aset data 9 v5)
            (ArrayHelpers/aset data 10 k6)
            (ArrayHelpers/aset data 11 v6)
            (ArrayHelpers/aset data 10 k7)
            (ArrayHelpers/aset data 11 v7)
            (PersistentArrayMap. data))))
    8 (let [sfact (FastStruct/createFactory (vec keys))]
        (fn [v1 v2 v3 v4 v5 v6 v7 v8]
          (.apply sfact (imlist v1 v2 v3 v4 v5 v6 v7 v8))))
    (let [sfact (FastStruct/createFactory (vec keys))]
      (fn [& args]
        (let [al (ArrayList. (count args))]
          (.addAll al ^Collection args)
          (.apply sfact al))))))


(comment

  (defn profile
    [n-keys]
    (let [keys (subvec ["a" "b" "c" "d" "e" "f" "g" "h" "i"] 0 n-keys)
          cljfact (map-factory keys)
          fact-fn (case n-keys
                    0
                    (fn [fact]
                      (let [mdata (-> (fact)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    1
                    (fn [fact]
                      (let [mdata (-> (fact 1)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    2
                    (fn [fact]
                      (let [mdata (-> (fact 1 2)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)
                        ))
                    3
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    4
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3 4)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    5
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3 4 5)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    6
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3 4 5 6)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    7
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3 4 5 6 7)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    8
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3 4 5 6 7 8)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it)))
                    9
                    (fn [fact]
                      (let [mdata (-> (fact 1 2 3 4 5 6 7 8 9)
                                      (assoc "dd" 1))]
                        ;;Then to parse these at the dataset level this pathway
                        ;;also needs to be optimized
                        (pfor/doiter it (.entrySet ^Map mdata) it))))]
      (println "cljfact")
      (time (dotimes [iter 1000000] (fact-fn cljfact)))))

   (dotimes [idx 10]
     (println "profiling:" idx)
     (profile idx))

  (dotimes [iidx 1000]
    (dotimes [idx 9]
      #_(println "profiling:" idx)
      (profile idx)))


  (defn profile-list
    [n-args]
    (let [fact-fn
          (fn [fact]
            (case n-args
              0 (fact)
              1 (fact 1)
              2 (fact 1 2)
              3 (fact 1 2 3)
              4 (fact 1 2 3 4)
              5 (fact 1 2 3 4 5)
              6 (fact 1 2 3 4 5 6)
              7 (fact 1 2 3 4 5 6 7)
              8 (fact 1 2 3 4 5 6 7 8)
              9 (fact 1 2 3 4 5 6 7 8 9)
              10 (fact 1 2 3 4 5 6 7 8 9 10)
              11 (fact 1 2 3 4 5 6 7 8 9 10 11)
              12 (fact 1 2 3 4 5 6 7 8 9 10 11 12)))]
      (println "vector")
      (time (dotimes [iter 1000000] (fact-fn vector)))
      (println "imlist")
      (time (dotimes [iter 1000000] (fact-fn imlist/imlist)))))

  (dotimes [idx 13]
    (println "profiling:" idx)
    (profile-list idx))
  )
