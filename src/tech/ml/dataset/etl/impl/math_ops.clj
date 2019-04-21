(ns tech.ml.dataset.etl.impl.math-ops
  (:require [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset :as ds]
            [tech.v2.datatype.functional.interpreter :as func-inter]))


(defonce ^:dynamic *etl-math-ops* (atom {}))


(defn col-stat-unary
  [op-kwd args]
  (when-not (= 1 (count args))
    (throw (ex-info "Unary function applied to multiple or zero arguments"
                    {:op-args args})))
  (when-not (ds-col/is-column? (first args))
    (throw (ex-info "Column stat function applied to something that is not a column."
                    {:argtype (type (first args))})))
  (-> (ds-col/stats (first args) [op-kwd])
      op-kwd))


(defn register-symbol!
  [sym-name sym-value]
  (func-inter/register-symbol! *etl-math-ops* sym-name sym-value))
