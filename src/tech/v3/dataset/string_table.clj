(ns tech.v3.dataset.string-table
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.dataset.dynamic-int-list :as int-list]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.lazy-noncaching :as lznc]
            [clojure.tools.logging :as log])
  (:import [java.util List HashMap Map ArrayList]
           [java.util.function Function]
           [tech.v3.datatype ObjectBuffer Buffer]
           [ham_fisted IMutList ChunkedList Casts ArrayHelpers ArrayLists]))


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
    (let [sz (.size this)
          ^objects rv (make-array String sz)
          local-int->str int->str
          local-data data]
      (StringTable. (ArrayLists/toList (hamf/object-array int->str)) nil (dtype-proto/clone data))
      #_(dorun (hamf/pgroups sz (fn string-table-clone [^long sidx ^long eidx]
                                (loop [sidx sidx]
                                  (when (< sidx eidx)
                                    (ArrayHelpers/aset rv sidx (.get int->str (.getLong local-data sidx)))
                                    (recur (inc sidx)))))))
      #_(ArrayLists/toList rv)))
  PStrTable
  (get-str-table [_this] {:int->str int->str
                         :str->int str->int})
  ObjectBuffer
  (elemwiseDatatype [_this] :string)
  (lsize [_this] (.size data))
  (clear [this]
    (.clear int->str)
    (.clear str->int)
    (.clear data))
  (size [_this] (.size data))
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
          (.reduce data (hamf-rf/long-accumulator
                         acc v
                         (rfn acc (.get int->str v)))
                   acc)))))
  (add [this value]
    (errors/when-not-errorf
     (or (nil? value) (instance? String value))
     "Value added to string table is not a string: %s" (type value))
    (let [value (or value "")
          item-idx (int (.computeIfAbsent
                         str->int
                         value
                         (reify Function
                           (apply [this keyval]
                             (let [retval (.size int->str)]
                               (.add int->str keyval)
                               retval)))))]
      (.addLong data item-idx))
    true)
  (add [this idx ct value]
    (let [value (or value "")
          item-idx (int (.computeIfAbsent
                         str->int
                         value
                         (reify Function
                           (apply [this keyval]
                             (let [retval (.size int->str)]
                               (.add int->str keyval)
                               retval)))))]
      (.add data (.size data) ct item-idx)))
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
    (.reduce data (hamf-rf/long-accumulator
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

(defn compress-indexes
  ^IMutList [^IMutList indexes ^long max-idx]
  (cond
    (<= max-idx Byte/MAX_VALUE) (ArrayLists/toList (hamf/byte-array indexes))
    (<= max-idx Short/MAX_VALUE) (ArrayLists/toList (hamf/short-array indexes))
    (<= max-idx Integer/MAX_VALUE) (ArrayLists/toList (hamf/int-array indexes))
    :else (.toLongArray indexes)))

(definterface IDof
  (idOf ^long [s]))

(defn fast-str
  ^String [s]
  (cond
    (nil? s) ""
    (instance? String s) s
    :else (.toString ^Object s)))

(deftype FastStringContainer [^IMutList indexes ^List int->str ^HashMap str->int]
  IDof
  (idOf [this s]
    (let [s (fast-str s)
          sz (long (.size str->int))
          lookup (.putIfAbsent str->int s sz)]
      (if lookup
        lookup
        (do
          (.add int->str s)
          sz))))
  java.util.function.Consumer
  (accept [this v] (.add this v))
  IMutList
  (add [this v]
    (.addLong indexes (.idOf this v))
    true)
  (add [this idx ct v]
    (.add indexes (.size indexes) ct (.idOf this v)))
  (get [this idx]
    (let [rv (.get int->str (.getLong indexes idx))]
      (when (nil? rv)
        (throw (RuntimeException. (str "Index out of range: " idx))))
      rv))
  (size [this] (.size indexes))
  (clear [this]
    (.clear indexes)
    (.clear int->str)
    (.clear str->int))
  tech.v3.datatype.protocols/PElemwiseDatatype
  (elemwise-datatype [this] :string)
  clojure.lang.IDeref
  (deref [this]
    (StringTable. (hamf/vec int->str) (.clone str->int)
                  (compress-indexes indexes (long (.size str->int))))))

(defn fast-string-container
  ([str->int int->str]
   (when-not (instance? java.util.HashMap str->int)
     (throw (RuntimeException. "Invalid creation of fast string container")))
   (FastStringContainer. (hamf/long-array-list)
                         int->str str->int))
  ([]
   (let [str->int (hamf/java-hashmap)
         int->str (ArrayList.)]
     (.put str->int "" 0)
     (.add int->str "")
     (fast-string-container str->int int->str))))

(defn string-table-from-strings
  ([str-data] (string-table-from-strings (fast-string-container) str-data))
  ([fast-string-container str-data]
   (hamf-rf/reduce-reducer (hamf-rf/consumer-reducer (constantly fast-string-container)) str-data)))


(defn ->string-table
  ^StringTable [str-t]
  (if (instance? StringTable str-t)
    str-t
    (string-table-from-strings str-t)))


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

(defn merge-string-tables
  "Return new string table with strings from lhs and rhs.  Neither input is changed."
  [^StringTable lhs ^StringTable rhs]
  (string-table-from-strings (lznc/concat (.-int->str lhs)
                                          (.-int->str rhs))))



