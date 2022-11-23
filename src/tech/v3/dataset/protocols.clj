(ns tech.v3.dataset.protocols
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype Buffer]))


(defprotocol PRowCount
  (^long row-count [this]))

(defprotocol PColumnCount
  (^long column-count [this]))

(defprotocol PMissing
  (missing [this]))

(defprotocol PSelectRows
  (select-rows [this rowidxs]))

(defprotocol PSelectColumns
  (select-columns [this colnames]))

(defprotocol PColumn
  (is-column? [col])
  (column-buffer [col]))


(defprotocol PDataset
  (is-dataset? [item])
  ;;error on failure
  (column [ds colname])
  ;;indexable object.
  (^Buffer rows [ds options])
  (^Buffer rowvecs [ds options]))


(defprotocol PDatasetTransform
  (transform [t dataset]))


(extend-type Object
  PDataset
  (is-dataset? [item] false)
  PColumn
  (is-column? [item] false)
  PRowCount
  (row-count [this] (dtype-proto/ecount this))
  PColumnCount
  (column-count [this] 0)
  PMissing
  (missing [this] (RoaringBitmap.)))


(defn column-name
  [col]
  (if (map? col)
    (or (get col :tech.v3.dataset/name)
        (get-in col [:tech.v3.dataset/metadata :name]))
    (:name (meta col))))


(defn dataset-name
  [ds]
  (:name (meta ds)))


(defn set-name
  [item nm]
  (vary-meta item assoc :name nm))
