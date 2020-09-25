(ns tech.v3.dataset.dynamic-int-list
  "An int-list implementation that resizes its backing store as it is required to hold
  wider data."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath])
  (:import [java.util List Iterator]
           [tech.v3.datatype PrimitiveList LongIO BooleanConversions]
           [tech.v3.datatype.list ListImpl]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro ^:private byte-range?
  [number]
  `(and (<= ~number Byte/MAX_VALUE)
        (>= ~number Byte/MIN_VALUE)))


(defmacro ^:private short-range?
  [number]
  `(and (<= ~number Short/MAX_VALUE)
        (>= ~number Short/MIN_VALUE)))


(deftype DynamicIntList [^:unsynchronized-mutable ^ListImpl backing-store
                         ^:unsynchronized-mutable ^long int-width]
  dtype-proto/PClone
  (clone [item] (DynamicIntList. (dtype-proto/clone backing-store)
                                 int-width))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item]
    (dtype-proto/convertible-to-array-buffer? backing-store))
  (->array-buffer [item]
    (dtype-proto/->array-buffer backing-store))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [item]
    (dtype-proto/convertible-to-native-buffer? backing-store))
  (->native-buffer [item]
    (dtype-proto/->native-buffer backing-store))
  PrimitiveList
  (elemwiseDatatype [this] :int32)
  (lsize [this] (.lsize backing-store))
  (ensureCapacity [item new-size]
    (.ensureCapacity backing-store new-size))
  (addBoolean [this value]
    (.addLong this (if value 1 0)))
  (addDouble [this value]
    (.addLong this (long value)))
  (addObject [this value]
    (.addLong this (long value)))
  (addLong [this value]
    ;;perform container conversion
    (cond
      (byte-range? value)
      nil
      (short-range? value)
      (when (pmath/< 16 int-width)
        (set! backing-store (dtype/make-container :list :int16 backing-store))
        (set! int-width 16))

      (pmath/< 32 int-width)
      (do
        (set! backing-store (dtype/make-container :list :int32 backing-store))
        (set! int-width 32)))
    (.addLong backing-store value))
  LongIO
  (readLong [this idx]
    (.readLong backing-store idx))
  ;;Writing is serialized.
  (writeLong [this idx value]
    (locking this
      (cond
        (byte-range? value)
        nil
        (short-range? value)
        (when (pmath/< 16 int-width)
          (set! backing-store (dtype/make-container :list :int16 backing-store))
          (set! int-width 16))

        (< 32 int-width)
        (do
          (set! backing-store (dtype/make-container :list :int32 backing-store))
          (set! int-width 32)))
      (.writeLong backing-store idx value))))


(defn dynamic-int-list
  "Create a dynamic int list from a sequence of numbers or from a
  single integer n-elems argument."
  ^PrimitiveList [num-or-item-seq]
  (if (number? num-or-item-seq)
    (DynamicIntList. (dtype/make-container :list :int8 (long num-or-item-seq))
                     :int8)
    (let [retval (DynamicIntList. (dtype/make-container :list :int8 0)
                                  8)]
      (parallel-for/consume! #(.addLong retval (long %)) num-or-item-seq)
      retval)))


(defn make-from-container
  "Make a dynamic int list from something convertible to a byte, short,
  or integer list.  Shares backing data."
  ^PrimitiveList [container]
  (let [container-datatype (dtype/elemwise-datatype container)]
    (errors/when-not-errorf
     (and (casting/integer-type? container-datatype)
          (<= (casting/int-width container-datatype) 32))
     "Container datatype must be integer and 32 bits or less: %s"
     container-datatype)
    (errors/when-not-errorf
     (dtype-base/as-buffer container)
     "Container must be convertible to either an array buffer or native buffer: %s"
     (type container))
    (let [list-data (dtype-base/as-buffer container)]
      (DynamicIntList. (dtype-list/wrap-container list-data)
                       (casting/int-width container-datatype)))))
