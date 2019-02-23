(ns tech.ml.dataset.etl.impl.math-ops
  (:require [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset :as ds]
            [tech.compute.tensor :as ct]
            [tech.compute.tensor.functional.impl :as func-impl]))


(defonce ^:dynamic *etl-math-ops* (atom {}))


(defn col-stat-unary
  [op-kwd args]
  (when-not (= 1 (count args))
    (throw (ex-info "Unary function applied to multiple or zero arguments"
                    {:op-args args})))
  (when-not (func-impl/tensor? (first args))
    (throw (ex-info "Column stat function applied to something that is not a column."
                    {:argtype (type (first args))})))
  (-> (ds-col/stats (first args) [op-kwd])
      op-kwd))


(defn col-stat-unary-binary
  [op-kwd args]
  (if (and (= 1 (count args))
           (ds-col/is-column? (first args)))
    (-> (ds-col/stats (first args) [op-kwd])
        op-kwd)
    (func-impl/binary-op op-kwd args)))


(defn register-symbol!
  [sym-name sym-value]
  (func-impl/register-symbol! *etl-math-ops* sym-name sym-value))


(defn eval-expr
  "Tiny simple interpreter.  Adding dataset and column-name to the global
  environment."
  [{:keys [dataset column-name] :as env} math-expr]
  (func-impl/eval-expr (assoc env :symbol-map @*etl-math-ops*)
                       math-expr))
