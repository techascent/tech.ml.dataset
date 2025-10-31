(ns tech.v3.dataset.protocols
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [ham-fisted.protocols :as hamf-proto]
            [tech.v3.datatype :as dtype])
  (:import [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype Buffer]
           [clojure.lang IDeref])
  (:refer-clojure :exclude [merge]))


(defprotocol PRowCount
  (^long row-count [this]))

(defprotocol PColumnCount
  (^long column-count [this]))

(defprotocol PMissing
  (missing [this])
  (num-missing [this])
  (any-missing? [this]))

(defn missing-count
  ^long [this] (num-missing this))

(defprotocol PValidRows
  (valid-rows [this]))

(defprotocol PSelectRows
  (select-rows [this rowidxs]))

(defprotocol PRowFilter
  (row-filter [this pred?]))

(defprotocol PSelectColumns
  (select-columns [this colnames]))

(defprotocol PColumn
  (is-column? [col])
  (column-buffer [col]
    "Return either the backing dataset for dense columns or tuple of [indexes,data] for sparse columns")
  (empty-column? [col])
  (column-data [col]
    "Return the backing data store.  Note for sparse columns this may be much shorter than the column")
  (with-column-data [col new-data]
    "new data must be same length as old data"))

(defprotocol PDataset
  (is-dataset? [item])
  ;;error on failure
  (column [ds colname])
  ;;indexable object.
  (^Buffer rows [ds options])
  (^Buffer rowvecs [ds options]))


(defprotocol PDatasetParser
  "Protocols for the dataset parser created via (dataset-parser)."
  (add-row [p row]
    "row needs to reduce to a sequence of objects implementing -key and -val")
  (add-rows [p rows]
    "rows need only be reducible"))

(defprotocol PClearable
  (ds-clear [p]
    "Reset to initial state.  Avoid conflict with collection/clear"))


(defprotocol PDatasetTransform
  (transform [t dataset]))


(extend-type Object
  PDataset
  (is-dataset? [item] false)
  PColumn
  (is-column? [item] false)
  PRowCount
  (row-count [this] (dtype/ecount this))
  PColumnCount
  (column-count [this] 0)
  PMissing
  (missing [this] (RoaringBitmap.)))


(extend-type nil
  PDataset
  (is-dataset? [item] false)
  PColumn
  (is-column? [item] false)
  PRowCount
  (row-count [this] 0)
  PColumnCount
  (column-count [this] 0)
  PMissing
  (missing [this] (RoaringBitmap.)))


(defprotocol PColumnName
  (column-name [col]))


(extend-protocol PColumnName
  nil
  (column-name [col] nil)
  Object
  (column-name [col]
    (if (map? col)
      (get col :tech.v3.dataset/name
           (get-in col [:tech.v3.dataset/metadata :name]))
      (:name (meta col)))))


(defn dataset-name
  [ds]
  (:name (meta ds)))


(defn set-name
  [item nm]
  (vary-meta item assoc :name nm))


(defprotocol PDatasetReducer
  "An object that produces a per-dataset reducer and can merge contexts.
  It is also expected that this implements hamf-proto/Finalize"
  (ds->reducer [this ds])
  (merge [this lctx rctx]))


(extend-type Object
  PDatasetReducer
  (ds->reducer [this ds] this)
  (finalize-ds-reduced [this ctx]
    (if (instance? IDeref ctx)
      (.deref ^IDeref ctx)
      ctx)))

;;For large reductions we may want to combine reducers on a single column when
;;possible.
(defprotocol PReducerCombiner
  "Some reduces such as ones based on apache can be specified separately
  in the output map but actually for efficiency need be represented by 1
  concrete reducer."
  (reducer-combiner-key [reducer]))


(extend-protocol PReducerCombiner
  Object
  (reducer-combiner-key [reducer] nil))
