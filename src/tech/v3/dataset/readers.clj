(ns ^:no-doc tech.v3.dataset.readers
  (:require [tech.v3.dataset.protocols :as ds-proto]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype Buffer]))


(defn value-reader
  "Return a reader that produces a reader of column values per index.
  Options:
  :copying? - Default to false - When true row values are copied on read."
  (^Buffer [dataset options]
   (ds-proto/rowvecs dataset options))
  (^Buffer [dataset]
   (value-reader dataset nil)))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value
  upon read."
  (^Buffer [dataset options]
   (ds-proto/rows dataset options))
  (^Buffer [dataset]
   (mapseq-reader dataset nil)))
