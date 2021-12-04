(ns tech.v3.dataset.string-table
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.dataset.dynamic-int-list :as int-list]
            [tech.v3.dataset.parallel-unique :refer [parallel-unique]]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.errors :as errors])
  (:import [java.util List HashMap Map ArrayList]
           [java.util.function Function]
           [tech.v3.datatype PrimitiveList ObjectBuffer]))


(defprotocol PStrTable
  (get-str-table [item]))


(declare make-string-table)


(deftype StringTable
    [^List int->str
     ^Map str->int
     ^PrimitiveList data]
  dtype-proto/PClone
  (clone [this]
    ;;We do not need to dedup any more; a java array is a more efficient
    ;;storage mechanism
    (dtype/make-container :jvm-heap :string this))
  PStrTable
  (get-str-table [_this] {:int->str int->str
                         :str->int str->int})
  PrimitiveList
  (elemwiseDatatype [_this] :string)
  (lsize [_this] (.lsize data))
  (ensureCapacity [_this new-size]
    (.ensureCapacity data new-size))
  (addBoolean [_this value]
    (errors/throwf "Invalid value in string table: %s" value))
  (addLong [_this value]
    (errors/throwf "Invalid value in string table: %s" value))
  (addDouble [_this value]
    (errors/throwf "Invalid value in string table: %s" value))
  (addObject [this value]
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
      (.addLong data item-idx)))
  ObjectBuffer
  (readObject [_this idx]
    (.get int->str (.readLong data idx)))
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
      (.writeLong data idx item-idx)))))


(defn make-string-table
  (^PrimitiveList [n-elems missing-val ^List int->str ^HashMap str->int]
   (let [^PrimitiveList data (int-list/dynamic-int-list (long n-elems))
         missing-val (str missing-val)]
     (.add int->str (int 0) missing-val)
     (.put str->int missing-val (int 0))
     (.ensureCapacity data (int n-elems))
     (StringTable. int->str str->int data)))
  (^PrimitiveList [n-elems missing-val]
   (make-string-table n-elems missing-val (ArrayList.) (HashMap.)))
  (^PrimitiveList [n-elems]
   (make-string-table n-elems "" (ArrayList.) (HashMap.)))
  (^PrimitiveList []
   (make-string-table 0 "" (ArrayList.) (HashMap.))))


(defn string-table-from-strings
  [str-data]
  (if-let [str-reader (dtype/as-reader str-data)]
    (let [unique-set (parallel-unique str-reader)
          _ (.remove unique-set "")
          set-iter (.iterator unique-set)
          n-unique-elems (inc (.size unique-set))
          str->int (HashMap. n-unique-elems)
          int->str (ArrayList. n-unique-elems)
          container-dtype (cond
                            (< n-unique-elems Byte/MAX_VALUE) :int8
                            (< n-unique-elems Short/MAX_VALUE) :int16
                            :else :int32)]
      (.put str->int "" (unchecked-int 0))
      (.add int->str "")
      ;;This loop as to be single threaded
      (loop [continue? (.hasNext set-iter)
             idx 1]
        (when continue?
          (let [str-entry (.next set-iter)]
            (.put str->int str-entry (unchecked-int idx))
            (.add int->str str-entry))
          (recur (.hasNext set-iter) (unchecked-inc idx))))
      ;;The rest can be parallelized
      (let [data (dtype/make-container
                  :jvm-heap container-dtype
                  (dtype/emap #(.get str->int %)
                              :int32
                              str-reader))]
        (StringTable. int->str str->int (int-list/make-from-container data))))
    ;;Else the data isn't convertible to a reader
    (let [str-table (make-string-table 0)]
      (parallel-for/consume! #(.add str-table %) str-data)
      str-table)))


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
      (.int->str)
      (dtype/->reader)))
