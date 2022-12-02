(ns tech.v3.dataset.protocols
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [ham-fisted.protocols :as hamf-proto])
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
    (get col :tech.v3.dataset/name
         (get-in col [:tech.v3.dataset/metadata :name]))
    (:name (meta col))))


(defn dataset-name
  [ds]
  (:name (meta ds)))


(defn set-name
  [item nm]
  (vary-meta item assoc :name nm))


;;For large reductions we may want to combine reducers on a single column when
;;possible.
(defprotocol PReducerCombiner
  "Some reduces such as ones based on apache can be specified separately
  in the output map but actually for efficiency need be represented by 1
  concrete reducer."
  (reducer-combiner-key [reducer])
  (combine-reducers [reducer combiner-key]
    "Return a new reducer that has configuration indicated by the above
     combiner-key.")
  (finalize-combined-reducer [this ctx]))


(extend-protocol PReducerCombiner
  Object
  (reducer-combiner-key [reducer] nil)
  (finalize-combined-reducer [this ctx]
    (hamf-proto/finalize this ctx)))
