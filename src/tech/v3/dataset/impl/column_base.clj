(ns tech.v3.dataset.impl.column-base
  (:require [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.datatype :as dtype])
  (:import [java.util Map List]
           [tech.v3.datatype PrimitiveList]
           [tech.v3.dataset Text]))


(def ^Map dtype->missing-val-map
  {:boolean false
   :int8 Byte/MIN_VALUE
   :int16 Short/MIN_VALUE
   :int32 Integer/MIN_VALUE
   :int64 Long/MIN_VALUE
   :float32 Float/NaN
   :float64 Double/NaN
   :packed-instant (packing/pack (dtype-dt/milliseconds-since-epoch->instant 0))
   :packed-local-date (packing/pack (dtype-dt/milliseconds-since-epoch->local-date 0))
   :packed-duration 0
   :instant nil
   :zoned-date-time nil
   :local-date-time nil
   :local-date nil
   :local-time nil
   :duration nil
   :string ""
   :text nil
   :keyword nil
   :symbol nil})


(casting/add-object-datatype! :text Text)


(defn datatype->missing-value
  [dtype]
  (let [dtype (if (packing/packed-datatype? dtype)
                dtype
                (casting/un-alias-datatype dtype))]
    (get dtype->missing-val-map dtype
         (when (casting/numeric-type? dtype)
           (casting/cast 0 dtype)))))


(defn make-container
  (^PrimitiveList [dtype n-elems]
   (case dtype
     :string (str-table/make-string-table n-elems "")
     :text (let [^List list-data (dtype/make-container :list :text 0)]
             (dotimes [iter n-elems]
               (.add list-data nil))
             list-data)
     (dtype/make-container :list dtype n-elems)))
  (^PrimitiveList [dtype]
   (make-container dtype 0)))
