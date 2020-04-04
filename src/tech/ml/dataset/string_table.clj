(ns tech.ml.dataset.string-table
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.ml.dataset.dynamic-int-list :as int-list])
  (:import [java.util List HashMap Map RandomAccess Iterator]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]))


(defprotocol PStrTable
  (get-str-table [item]))


(declare make-string-table)


(deftype StringTable
    [^Map int->str
     ^Map str->int
     ^IntList data]
  dtype-proto/PDatatype
  (get-datatype [this] :string)
  dtype-proto/PClone
  (clone [this]
    (StringTable. int->str str->int (dtype/clone data)))
  dtype-proto/PPrototype
  (from-prototype [this new-datatype new-shape]
    (let [n-elems (long (apply * new-shape))]
      (if-not (= new-datatype :string)
        (dtype/make-container :list new-datatype n-elems)
        (make-string-table n-elems (.get int->str 0) int->str str->int))))
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
    ;;dtype/copy! calls set in parallel but will never call add
    ;;in parallel.  This is unsafe really but add is called during parsing
    ;;a lot and it has a huge effect for some files.
    (locking str->int
      (when-not (instance? String str-val)
        (throw (Exception. "Can only use strings")))
      (let [item-idx (int (if-let [idx-val (.get str->int str-val)]
                            idx-val
                            (let [idx-val (.size str->int)]
                              (.put str->int str-val idx-val)
                              (.put int->str idx-val str-val)
                              idx-val)))
            old-value (int (.set data idx item-idx))]
        (.get int->str old-value))))
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
  (^List [n-elems missing-val ^HashMap int->str ^HashMap str->int]
   (let [^IntList data (int-list/dynamic-int-list (long n-elems))
         missing-val (str missing-val)]
     (.put int->str (int 0) missing-val)
     (.put str->int missing-val (int 0))
     (.size data (int n-elems))
     (StringTable. int->str str->int data)))
  (^List [n-elems missing-val]
   (make-string-table n-elems missing-val (HashMap.) (HashMap.)))
  (^List [n-elems]
   (make-string-table n-elems "" (HashMap.) (HashMap.)))
  (^List []
   (make-string-table 0 "" (HashMap.) (HashMap.))))
