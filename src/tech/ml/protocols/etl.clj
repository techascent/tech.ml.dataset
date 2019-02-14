(ns tech.ml.protocols.etl
  (:require [tech.ml.protocols.column :as col-proto]))


(defprotocol PETLSingleColumnOperator
  "Define an operator for an ETL operation."
  (build-etl-context [op dataset column-names op-args])
  (perform-etl [op dataset column-names op-args context]))


(defprotocol PETLMultipleColumnOperator
  (build-etl-context-columns [op dataset column-name-seq op-args])
  (perform-etl-columns [op dataset column-name-seq op-args context]))


(defn default-etl-context-columns
  "Default implementation of build-etl-context-columns"
  [op dataset column-name-seq op-args]
  (->> column-name-seq
       (map (fn [col-name]
              (when-let [etl-ctx (build-etl-context op dataset col-name op-args)]
                [col-name etl-ctx])))
       (remove nil?)
       (into {})))


(defn default-perform-etl-columns
  [op dataset column-name-seq op-args context]
  (->> column-name-seq
       (reduce (fn [dataset col-name]
                 (perform-etl op dataset col-name op-args
                              (get context col-name)))
               dataset)))


(extend-protocol PETLMultipleColumnOperator
  Object
  (build-etl-context-columns [op dataset column-name-seq op-args]
    (default-etl-context-columns op dataset column-name-seq op-args))

  (perform-etl-columns [op dataset column-name-seq op-args context]
    (default-perform-etl-columns op dataset column-name-seq op-args context)))
