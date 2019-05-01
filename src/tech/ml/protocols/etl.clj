(ns tech.ml.protocols.etl
  (:require [tech.ml.protocols.column :as col-proto]))


(defprotocol PETLSingleColumnOperator
  "Define an operator for an ETL operation."
  (build-etl-context [op dataset column-name op-args])
  (perform-etl [op dataset column-name op-args context]))


(defprotocol PETLMultipleColumnOperator
  (build-etl-context-columns [op dataset column-name-seq op-args])
  (perform-etl-columns [op dataset column-name-seq op-args context]))
