(ns tech.ml.dataset.etl.impl.pipeline-operators
  (:require [tech.ml.dataset.etl.impl.column-filters :as column-filters]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset.options :as options]
            [tech.ml.dataset :as ds])

  (:import [tech.ml.protocols.etl
            PETLSingleColumnOperator
            PETLMultipleColumnOperator]))


(defonce ^:dynamic *etl-operator-registry* (atom {}))


(defn register-etl-operator!
  [op-kwd op]
  (-> (swap! *etl-operator-registry* assoc op-kwd op)
      keys))


(defn get-etl-operator
  [op-kwd]
  (if-let [retval (get @*etl-operator-registry* op-kwd)]
    retval
    (throw (ex-info (format "Failed to find etl operator %s" op-kwd)
                    {:op-keyword op-kwd}))))


(defn apply-pipeline-operator
  [{:keys [pipeline dataset options]} op]
  (let [inference? (:inference? options)
        recorded? (or (:recorded? options) inference?)
        [op context] (if-not recorded?
                       [op {}]
                       [(:operation op) (:context op)])
        op-type (keyword (name (first op)))
        col-selector (second op)
        op-args (drop 2 op)
        ;;The order of columns is extremely important at times.  It is unwise
        ;;to carelessly reorder column names unless the user explicitly asks
        col-seq (->> (column-filters/select-columns dataset col-selector)
                     (ds/order-column-names dataset)
                     vec)
        op-impl (get-etl-operator op-type)
        [context options] (if-not recorded?
                            (let [context
                                  (etl-proto/build-etl-context-columns
                                   op-impl dataset col-seq op-args)]
                              [context (options/merge-label-maps options context)])
                            [context options])
        dataset (etl-proto/perform-etl-columns
                 op-impl dataset col-seq op-args context)]
    {:dataset dataset
     :options options
     :pipeline (conj pipeline {:operation (->> (concat [(first op) col-seq]
                                                       (drop 2 op))
                                               vec)
                               :context context})}))


(defmacro def-single-column-etl-operator
  [op-symbol docstring op-context-code op-code]
  `(do (register-etl-operator!
        ~(keyword (name op-symbol))
        (reify PETLSingleColumnOperator
          (build-etl-context [~'op ~'dataset ~'column-name ~'op-args]
            ~op-context-code)
          (perform-etl [~'op ~'dataset ~'column-name ~'op-args ~'context]
            ~op-code)))
       (defn ~op-symbol
         ~docstring
         [~'dataset ~'col-filter & ~'args]
         (-> (apply-pipeline-operator
              {:pipeline []
               :options {}
               :dataset ~'dataset}
              (-> (concat '[~op-symbol]
                          [~'col-filter]
                          ~'args)
                  vec))
             :dataset))))


(defmacro def-multiple-column-etl-operator
  [op-symbol docstring op-context-code op-code]
  `(do (register-etl-operator!
        ~(keyword (name op-symbol))
        (reify PETLMultipleColumnOperator
          (build-etl-context-columns [~'op ~'dataset ~'column-name-seq ~'op-args]
            ~op-context-code)
          (perform-etl-columns [~'op ~'dataset ~'column-name-seq ~'op-args ~'context]
            ~op-code)))
       (defn ~op-symbol
         ~docstring
         [~'dataset ~'col-filter & ~'args]
         (-> (apply-pipeline-operator
              {:pipeline []
               :options {}
               :dataset ~'dataset}
              (-> (concat '[~op-symbol]
                          [~'col-filter]
                          ~'args)
                  vec))
             :dataset))))
