(ns tech.ml.dataset.string-table
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.ml.dataset.dynamic-int-list :as int-list]
            [tech.ml.dataset.parallel-unique :refer [parallel-unique]]
            [tech.parallel.for :as parallel-for])
  (:import [java.util List HashMap Map RandomAccess Iterator ArrayList
            Collections]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]))


(defprotocol PStrTable
  (get-str-table [item]))


(declare make-string-table)


(deftype StringTable
    [^List int->str
     ^Map str->int
     ^IntList data]
  dtype-proto/PDatatype
  (get-datatype [this] :string)
  dtype-proto/PClone
  (clone [this]
    ;;We do not need to dedup any more; a java array is a more efficient
    ;;storage mechanism
    (dtype/make-container :java-array :string this))
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
                            (.add int->str idx-val str-val)
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
                              (.add int->str idx-val str-val)
                              idx-val)))
            old-value (int (.set data idx item-idx))]
        (.get int->str old-value))))
  (subList [this start-offset end-offset]
    (StringTable. int->str str->int (.subList data start-offset end-offset)))
  (toArray [this]
    (object-array this))
  RandomAccess
  Iterable
  (iterator [this]
    (let [^IntIterator src-iter (.iterator data)]
      (reify Iterator
        (hasNext [iter] (.hasNext src-iter))
        (next [iter] (.get int->str (.nextInt src-iter)))))))


(defn make-string-table
  (^List [n-elems missing-val ^List int->str ^HashMap str->int]
   (let [^IntList data (int-list/dynamic-int-list (long n-elems))
         missing-val (str missing-val)]
     (.add int->str (int 0) missing-val)
     (.put str->int missing-val (int 0))
     (.size data (int n-elems))
     (StringTable. int->str str->int data)))
  (^List [n-elems missing-val]
   (make-string-table n-elems missing-val (ArrayList.) (HashMap.)))
  (^List [n-elems]
   (make-string-table n-elems "" (ArrayList.) (HashMap.)))
  (^List []
   (make-string-table 0 "" (ArrayList.) (HashMap.))))


(defn string-table-from-strings
  [str-data]
  (if-let [str-reader (dtype/->reader str-data)]
    (let [unique-set (parallel-unique str-reader)
          _ (.remove unique-set "")
          set-iter (.iterator unique-set)
          n-unique-elems (inc (.size unique-set))
          n-elems (dtype/ecount str-reader)
          str->int (HashMap. n-unique-elems)
          int->str (ArrayList. n-unique-elems)]
      (.put str->int "" (unchecked-int 0))
      (.add int->str (unchecked-int 0) "")
      (loop [continue? (.hasNext set-iter)
             idx (int 0)]
        (when continue?
          (let [str-entry (.next set-iter)
                idx (unchecked-int idx)]
            (.put str->int str-entry (unchecked-int idx))
            (.add int->str (unchecked-int idx) str-entry))
          (recur (.hasNext set-iter) (unchecked-inc idx))))
      (cond
        (< n-unique-elems Byte/MAX_VALUE)
        (let [data (byte-array n-elems)]
          (parallel-for/parallel-for
           idx n-elems
           (aset data idx (unchecked-byte (.get str->int (str-reader idx)))))
          (StringTable. int->str str->int (int-list/make-from-container data)))
        (< n-unique-elems Short/MAX_VALUE)
        (let [data (short-array n-elems)]
          (parallel-for/parallel-for
           idx n-elems
           (aset data idx (unchecked-short (.get str->int (str-reader idx)))))
          (StringTable. int->str str->int (int-list/make-from-container data)))
        :else
        (let [data (int-array n-elems)]
          (parallel-for/parallel-for
           idx n-elems
           (aset data idx (unchecked-int (.get str->int (str-reader idx)))))
          (StringTable. int->str str->int (int-list/make-from-container data)))))
    (let [str-table (make-string-table 0)]
      (doseq [data str-data]
        (.add str-table data))
      str-table)))

(defn ->string-table
  ^StringTable [str-t]
  (when-not (instance? StringTable str-t)
    (throw (Exception. (format "string table is wrong type: %s"
                               str-t))))
  str-t)

(defn indices
  "Get the string table backing index data."
  [^StringTable str-t]
  (-> (->string-table str-t)
      (.data str-t)
      (int-list/int-list->data)))

(defn int->string
  "Returns an unmodifiable list of strings from a string table.  This serves as the
  index->string lookup table.  Returned value also implements 'nth' efficiently."
  ^List [^StringTable str-t]
  (-> (->string-table str-t)
      (.int->str)
      (dtype/->reader)))
