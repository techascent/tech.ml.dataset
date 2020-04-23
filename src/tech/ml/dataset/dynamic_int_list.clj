(ns tech.ml.dataset.dynamic-int-list
  "An int-list implementation that resizes its backing store as it is required to hold
  wider data."
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [primitive-math :as pmath]
            [tech.v2.datatype.list]
            [tech.parallel.for :as parallel-for])
  (:import [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator
            IntListIterator]
           [it.unimi.dsi.fastutil.shorts ShortArrayList ShortList ShortIterator]
           [it.unimi.dsi.fastutil.bytes ByteArrayList ByteList ByteIterator]
           [java.util List Iterator]
           [tech.ml.dataset SimpleIntList]
           [tech.v2.datatype IntReader IntWriter]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro byte-range?
  [number]
  `(and (<= ~number Byte/MAX_VALUE)
        (>= ~number Byte/MIN_VALUE)))


(defmacro ^:private short-range?
  [number]
  `(and (<= ~number Short/MAX_VALUE)
        (>= ~number Short/MIN_VALUE)))


(defmacro promote-short!
  []
  `(when (instance? ByteList ~'backing-store)
     (set! ~'backing-store (let [new-list# (ShortArrayList. )]
                             (dotimes [iter# (.size ^ByteList ~'backing-store)]
                               (.add new-list#
                                     (pmath/short
                                      (.get ^ByteList ~'backing-store iter#))))
                             new-list#))
     (set! ~'reader nil)))


(defmacro promote-int!
  []
  `(do
     (when (instance? ByteList ~'backing-store)
       (set! ~'backing-store (let [new-list# (IntArrayList. )]
                               (dotimes [iter# (.size ~'backing-store)]
                                 (.add new-list#
                                       (pmath/int
                                        (.get ^ByteList ~'backing-store iter#))))
                               new-list#))
       (set! ~'reader nil))
     (when (instance? ShortList ~'backing-store)
       (set! ~'backing-store (let [new-list# (IntArrayList. )]
                               (dotimes [iter# (.size ~'backing-store)]
                                 (.add new-list#
                                       (pmath/int
                                        (.get ^ShortList ~'backing-store iter#))))
                               new-list#))
       (set! ~'reader nil))))



(deftype DynamicIntList [^:unsynchronized-mutable ^List backing-store
                         ^:unsynchronized-mutable ^IntReader reader]
  dtype-proto/PDatatype
  (get-datatype [item] :int32)
  dtype-proto/PCountable
  (ecount [item] (.size ^List backing-store))
  dtype-proto/PClone
  (clone [item] (DynamicIntList. (dtype-proto/clone backing-store)
                                 nil))
  dtype-proto/PToList
  (convertible-to-fastutil-list? [item] true)
  (->list-backing-store [item] backing-store)
  SimpleIntList
  (lsize [this] (long (.size backing-store)))
  (size [this] (.size backing-store))
  (size [this new-len]
    (cond
      (instance? ByteList backing-store)
      (.size ^ByteList backing-store new-len)
      (instance? ShortList backing-store)
      (.size ^ShortList backing-store new-len)
      (instance? IntList backing-store)
      (.size ^IntList backing-store new-len)))
  (addInt [this idx value]
    (boolean
     (cond
       (byte-range? value)
       (cond
         (instance? ByteList backing-store)
         (.add ^ByteList backing-store idx (pmath/byte value))
         (instance? ShortList backing-store)
         (.add ^ShortList backing-store idx (pmath/short value))
         (instance? IntList backing-store)
         (.add ^IntList backing-store idx (pmath/int value))
         :else (throw (Exception. "Programmer error")))
       (short-range? value)
       (do
         (promote-short!)
         (cond
           (instance? ShortList backing-store)
           (.add ^ShortList backing-store idx (pmath/short value))
           (instance? IntList backing-store)
           (.add ^IntList backing-store idx (pmath/int value))
           :else (throw (Exception. "Programmer error"))))
       :else
       (do
         (promote-int!)
         (.add ^IntList backing-store idx value)))))
  (getInt [this idx]
    (when-not reader
      (let [new-reader
            (cond
              (instance? ByteList backing-store)
              (let [^ByteList backing-store backing-store
                    n-elems (long (.size backing-store))]
                (if-let [{:keys [java-array offset length]}
                         (dtype-proto/->sub-array backing-store)]
                  (let [offset (pmath/int offset)
                        ^bytes java-array java-array]
                    (reify IntReader
                      (lsize [rdr] n-elems)
                      (read [rdr idx]
                        (pmath/int (aget java-array (pmath/+ (pmath/int idx)
                                                             offset))))))
                  (reify IntReader
                    (lsize [rdr] n-elems)
                    (read [rdr idx]
                      (pmath/int (.getByte backing-store (pmath/int idx)))))))
              (instance? ShortList backing-store)
              (let [^ShortList backing-store backing-store
                    n-elems (long (.size backing-store))]
                (reify IntReader
                  (lsize [rdr] n-elems)
                  (read [rdr idx]
                    (pmath/int (.getShort backing-store (pmath/int idx))))))
              (instance? IntList backing-store)
              (typecast/datatype->reader :int32 backing-store)
              :else (throw (Exception. "Programmer error")))]
        (set! reader new-reader)))
    (.read reader idx))
  (setInt [this idx value]
    (locking this
      (cond
        (byte-range? value)
        (cond
          (instance? ByteList backing-store)
          (.set ^ByteList backing-store idx (pmath/byte value))
          (instance? ShortList backing-store)
          (.set ^ShortList backing-store idx (pmath/short value))
          (instance? IntList backing-store)
          (.set ^IntList backing-store idx (pmath/int value))
          :else (throw (Exception. "Programmer error")))
        (short-range? value)
        (do
          (promote-short!)
          (cond
            (instance? ShortList backing-store)
            (.set ^ShortList backing-store idx (pmath/short value))
            (instance? IntList backing-store)
            (.set ^IntList backing-store idx (pmath/int value))
            :else (throw (Exception. "Programmer error"))))
        :else
        (do
          (promote-int!)
          (.set ^IntList backing-store idx value)))))
  (subList [this start-off end-off]
    (DynamicIntList. (.subList backing-store start-off end-off)
                     nil))
  IntReader
  (read [item idx] (.getInt item (int idx)))
  IntWriter
  (write [item idx value] (.setInt item (int idx) value))
  Iterable
  (iterator [this]
    (case (dtype-proto/get-datatype backing-store)
      :int8
      (let [^ByteIterator src-iter (.iterator backing-store)]
        (reify IntListIterator
          (hasNext [iter] (.hasNext src-iter))
          (nextInt [iter]
            (pmath/int (.nextByte src-iter)))))
      :int16
      (let [^ShortIterator src-iter (.iterator backing-store)]
        (reify IntListIterator
          (hasNext [iter] (.hasNext src-iter))
          (nextInt [iter]
            (pmath/int (.nextShort src-iter)))))
      :int32
      (.iterator backing-store))))


(defn dynamic-int-list
  [num-or-item-seq]
  (if (number? num-or-item-seq)
    (DynamicIntList. (ByteArrayList/wrap (byte-array (long num-or-item-seq)))
                     nil)
    (let [retval (DynamicIntList. (ByteArrayList.) nil)]
      (parallel-for/doiter
       next-val
       num-or-item-seq
       (.add retval (pmath/int next-val)))
      retval)))
