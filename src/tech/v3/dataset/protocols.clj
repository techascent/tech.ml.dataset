(ns tech.v3.dataset.protocols
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [org.roaringbitmap RoaringBitmap]))


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
  (rows [ds])
  (rowvecs [ds])
  (row-at [ds idx])
  (rowvec-at [ds idx]))


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
