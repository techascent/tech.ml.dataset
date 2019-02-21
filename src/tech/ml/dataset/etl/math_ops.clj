(ns tech.ml.dataset.etl.math-ops
  (:require [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.etl.impl.math-ops
             :refer [def-math-op def-unary
                     def-col-stat-unary def-binary
                     def-col-stat-unary-binary]
             :as math-ops-impl]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset :as ds]
            [tech.datatype :as dtype])
  (:refer-clojure :exclude [+ - / * max min
                            bit-and bit-xor
                            > >= < <=
                            replace]))


(def-math-op col :varargs
  "Lookup a column in the dataset.  With no args, returns the active column.  Else
finds indicated column."
  (case (count args)
    0
    (ds/column dataset column-name)
    1
    (ds/column dataset (first args))))

(def-math-op replace :varargs
  "Replace column values using fixed lookup map."
  (when-not (= (count args) 2)
    (throw (ex-info "Replace takes exactly 2 arguments." {})))
  (when-not (math-ops-impl/is-tensor? (first args))
    (throw (ex-info "First argument to replace must be a tensor" {})))
  (when-not (map? (second args))
    (throw (ex-info "Second argument to replace must be lookup map." {})))
  (let [tens (first args)
        tens-dtype (dtype/get-datatype tens)
        lookup-map (->> (second args)
                        (map (fn [[k v]]
                               [(dtype/cast k tens-dtype)
                                (dtype/cast v tens-dtype)]))
                        (into {}))]
    (->> (ds-col/column-values tens)
         (map (fn [col-val]
                (if-let [retval (get lookup-map col-val)]
                  retval
                  col-val)))
         (ds-col/new-column tens tens-dtype))))


(def-unary log1p #(Math/log1p (double %)))
(def-unary ceil #(Math/ceil (double %)))
(def-unary floor #(Math/floor (double %)))
(def-unary sqrt #(Math/sqrt (double %)))
(def-unary abs #(Math/abs (double %)))
(def-unary sin #(Math/sin (double %)))
(def-unary cos #(Math/cos (double %)))
(def-unary tanh #(Math/tanh (double %)))
(def-unary - -)



(def ^:private potential-stats [:mean
                                :variance
                                :median
                                :skew
                                :kurtosis
                                :geometric-mean
                                :sum-of-squares
                                :sum-of-logs
                                :quadratic-mean
                                :standard-deviation
                                :population-variance
                                :sum
                                :product
                                :quartile-1
                                :quartile-3])


(defmacro ^:private define-col-stats
  []
  `(do
     ~@(for [colstat-kwd potential-stats]
         `(def-col-stat-unary ~(symbol (name colstat-kwd))))))


(define-col-stats)

(def-col-stat-unary-binary min clojure.core/min)
(def-col-stat-unary-binary max clojure.core/max)

(def-binary + clojure.core/+)
(def-binary - clojure.core/-)
(def-binary * clojure.core/*)
(def-binary / clojure.core//)
(def-binary ** #(Math/pow (double %1) (double %2)))
(def-binary bit-and clojure.core/bit-and)
(def-binary bit-xor clojure.core/bit-xor)
(def-binary eq clojure.core/=)
(def-binary > clojure.core/>)
(def-binary >= clojure.core/>=)
(def-binary < clojure.core/<)
(def-binary <= clojure.core/<=)
