(ns tech.ml.dataset.string-table
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [java.util List HashMap Map RandomAccess Iterator]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]))


(defprotocol PStrTable
  (get-str-table [item]))


(deftype StringTable
    [^Map int->str
     ^Map str->int
     ^IntList data]
  dtype-proto/PDatatype
  (get-datatype [this] :string)
  tech.v2.datatype.Countable
  (lsize [this] (long (.size data)))
  PStrTable
  (get-str-table [this] {:int->str int->str
                         :str->int str->int})
  List
  (size [this] (.size data))
  (add [this str-val]
    (.add this (.size data) str-val)
    true)
  (add [this idx str-val]
    (when-not (instance? String str-val)
      (throw (Exception. "Can only use strings")))
    (let [item-idx (int (if-let [idx-val (.get str->int str-val)]
                          idx-val
                          (let [idx-val (.size str->int)]
                            (.put str->int str-val idx-val)
                            (.put int->str idx-val str-val)
                            idx-val)))]
      (.add data idx item-idx)
      true))
  (get [this idx] (.get int->str (.get data idx)))
  (set [this idx str-val]
    (when-not (instance? String str-val)
      (throw (Exception. "Can only use strings")))
    (let [item-idx (int (if-let [idx-val (.get str->int str-val)]
                          idx-val
                          (let [idx-val (.size str->int)]
                            (.put str->int str-val idx-val)
                            (.put int->str idx-val str-val)
                            idx-val)))]
      (.set data idx item-idx)))
  (subList [this start-offset end-offset]
    (StringTable. int->str str->int (.subList data start-offset end-offset)))
  RandomAccess
  Iterable
  (iterator [this]
    (let [^IntIterator src-iter (.iterator data)]
      (reify Iterator
        (hasNext [iter] (.hasNext src-iter))
        (next [iter] (.get int->str (.nextInt src-iter)))))))


(defn make-string-table
  (^List [n-elems ^HashMap int->str ^HashMap str->int]
   (let [^IntList data (dtype/make-container :list :int32 (long n-elems))]
     (.put int->str (int 0) "")
     (.put str->int "" (int 0))
     (.size data (int n-elems))
     (StringTable. int->str str->int data)))
  (^List [n-elems]
   (make-string-table n-elems (HashMap.) (HashMap.)))
  (^List []
   (make-string-table 0 (HashMap.) (HashMap.))))
