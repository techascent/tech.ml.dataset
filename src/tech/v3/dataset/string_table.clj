(ns tech.v3.dataset.string-table
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.dataset.dynamic-int-list :as int-list]
            [tech.v3.dataset.parallel-unique :refer [parallel-unique]]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.api :as hamf])
  (:import [java.util List HashMap Map ArrayList]
           [java.util.function Function]
           [tech.v3.datatype ObjectBuffer Buffer]
           [ham_fisted IMutList ChunkedList]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PStrTable
  (get-str-table [item]))


(declare make-string-table)


(deftype StringTable
    [^List int->str
     ^Map str->int
     ^IMutList data]
  dtype-proto/PClone
  (clone [this]
    ;;We do not need to dedup any more; a java array is a more efficient
    ;;storage mechanism
    (dtype/make-container :jvm-heap :string this))
  PStrTable
  (get-str-table [_this] {:int->str int->str
                         :str->int str->int})
  ObjectBuffer
  (elemwiseDatatype [_this] :string)
  (lsize [_this] (.size data))
  (subBuffer [this sidx eidx]
    (ChunkedList/sublistCheck sidx eidx (.lsize this))
    (if (and (== sidx 0)
             (== eidx (.lsize this)))
      this)
    (let [^List int->str int->str
          ^IMutList data (.subList data sidx eidx)]
      (reify ObjectBuffer
        (elemwiseDatatype [rdr] :string)
        (lsize [rdr] (- eidx sidx))
        (subBuffer [rdr ssidx seidx]
          (ChunkedList/sublistCheck ssidx seidx (.lsize this))
          (.subBuffer this (+ sidx ssidx) (+ sidx seidx)))
        (readObject [rdr idx] (.get int->str (.getLong data idx)))
        (reduce [this rfn acc]
          (.reduce data (hamf/long-accumulator
                         acc v
                         (rfn acc (.get int->str v)))
                   acc)))))
  (add [this value]
    (errors/when-not-errorf
     (instance? String value)
     "Value added to string table is not a string: %s" value)
    (let [item-idx (int (.computeIfAbsent
                         str->int
                         value
                         (reify Function
                           (apply [this keyval]
                             (let [retval (.size int->str)]
                               (.add int->str keyval)
                               retval)))))]
      (.addLong data item-idx))
    true)
  (readObject [_this idx]
    (.get int->str (.getLong data idx)))
  (writeObject [this idx value]
    (locking this
      (let [item-idx (int (.computeIfAbsent
                         str->int
                         value
                         (reify Function
                           (apply [this keyval]
                             (let [retval (.size int->str)]
                               (.add int->str keyval)
                               retval)))))]
        (.setLong data idx item-idx))))
  (reduce [this rfn acc]
    (.reduce data (hamf/long-accumulator
                   acc v
                   (rfn acc (.get int->str v)))
             acc)))


(defn make-string-table
  (^Buffer [n-elems missing-val ^List int->str ^HashMap str->int]
   (let [^Buffer data (int-list/dynamic-int-list n-elems)
         missing-val (str missing-val)]
     (.add int->str missing-val)
     (.put str->int missing-val (dec (.size int->str)))
     (StringTable. int->str str->int data)))
  (^Buffer [n-elems missing-val]
   (make-string-table n-elems missing-val (hamf/object-array-list n-elems) (HashMap.)))
  (^Buffer [n-elems]
   (make-string-table n-elems "" (hamf/object-array-list n-elems) (HashMap.)))
  (^Buffer []
   (make-string-table 0 "" (hamf/object-array-list) (HashMap.))))


(defn string-table-from-strings
  [str-data]
  (let [n-elems (long (or (hamf/constant-count str-data) 0))]
    (doto (make-string-table n-elems)
      (.addAllReducible str-data))))


(defn ->string-table
  ^StringTable [str-t]
  (errors/when-not-errorf (instance? StringTable str-t)
    "string table is wrong type: %s" str-t)
  str-t)


(defn indices
  "Get the string table backing index data."
  [^StringTable str-t]
  (-> (.data (->string-table str-t))
      (dtype-base/as-buffer)))


(defn int->string
  "Returns an unmodifiable list of strings from a string table.  This serves as the
  index->string lookup table.  Returned value also implements 'nth' efficiently."
  ^List [^StringTable str-t]
  (-> (->string-table str-t)
      (.int->str)))
