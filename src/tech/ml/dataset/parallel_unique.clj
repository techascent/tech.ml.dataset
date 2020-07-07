(ns tech.ml.dataset.parallel-unique
  (:require [tech.parallel.for :as parallel-for]
            [tech.v2.datatype :as dtype])
  (:import [java.util HashSet Set]))


(defn parallel-unique
  "Scan the data in parallel and geneate a set of unique items.
  Input must be convertible to a reader"
  ^Set [data]
  (if-let [rdr (dtype/->reader data)]
    (parallel-for/indexed-map-reduce
     (dtype/ecount rdr)
     (fn [^long start-idx ^long len]
       (let [data (HashSet.)]
         (dotimes [iter len]
           (.add data (rdr (unchecked-add iter start-idx))))
         data))
     (partial reduce (fn [^Set lhs ^Set rhs]
                       (.addAll lhs rhs)
                       lhs)))
    (throw (Exception. "Data is not convertible to a reader"))))
