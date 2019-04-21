(ns tech.ml.dataset.etl.math-ops
  (:require [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.etl.impl.math-ops :as ds-math-ops]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.window :as window]
            [tech.ml.dataset :as ds]
            [tech.datatype :as dtype]
            [tech.compute.tensor.functional])
  (:refer-clojure :exclude [+ - / *
                            <= < >= >
                            min max
                            bit-xor bit-and bit-and-not bit-not bit-set bit-test
                            bit-or bit-flip bit-clear
                            bit-shift-left bit-shift-right unsigned-bit-shift-right
                            quot rem cast not and or
                            replace])
  (:refer tech.compute.tensor.functional :exclude [min max mean]))


(math-ops/add-binary-op! :** (math-ops/get-binary-operand :pow))


(func-impl/make-math-fn nil :** #{:binary})

(defn col
  [{:keys [dataset column-name] :as env} & args]
  (case (count args)
    0
    (ds/column dataset column-name)
    1
    (ds/column dataset (first args))))


(ds-math-ops/register-symbol! 'col col)


(defn replace
  "Replace column values using fixed lookup map."
  [{:keys [dataset column-name] :as env} & args]
  (when-not (= (count args) 2)
    (throw (ex-info "Replace takes exactly 2 arguments." {})))
  (when-not (func-impl/tensor? (first args))
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


(ds-math-ops/register-symbol! 'replace replace)


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
         `(do
            (defn ~(symbol (name colstat-kwd))
              ~(format "Column stat function %s" (name colstat-kwd))
              [& args#]
              (ds-math-ops/col-stat-unary ~colstat-kwd args#))
            (ds-math-ops/register-symbol!
             (symbol ~(name colstat-kwd))
             (fn [_# & args#]
               (apply ~(symbol (name colstat-kwd)) args#)))))))


(define-col-stats)

;; The advantage of these min and max functions is that they work when the column
;; has missing values.

(defmacro ^:private def-col-stat-unary-binary
  [item-sym]
  `(do
     (defn ~item-sym
       [& args#]
       (ds-math-ops/col-stat-unary-binary
        ~(keyword (name item-sym)) args#))
     (ds-math-ops/register-symbol!
      (symbol ~(name item-sym))
      (fn [_# & args#]
        (apply ~(symbol (name item-sym)) args#)))))

(def-col-stat-unary-binary min)
(def-col-stat-unary-binary max)


(defn rolling
  "Perform a rolling window operation.  Possibilities are :mean, :max, :min, etc.
  Operation must be performed only when there are no NANs."
  [{:keys [dataset column-name]} window-size window-fn input-col]
  (let [window-fn (cond
                    (keyword? window-fn)
                    window-fn
                    (symbol? window-fn)
                    (keyword (name window-fn))
                    (string? window-fn)
                    (keyword window-fn)
                    :else
                    (throw (ex-info (format "Unrecognized window fn: %s" window-fn)
                                    {:window-fn window-fn})))]
    (window/specific-rolling-window
     input-col (long window-size) window-fn)))


(ds-math-ops/register-symbol! (symbol "rolling") rolling)
