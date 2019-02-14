(ns tech.libs.tablesaw.datatype.fastutil
  (:require [tech.datatype.base :as base]
            [tech.datatype.java-primitive :as primitive]
            [clojure.core.matrix.protocols :as mp])
  (:import [it.unimi.dsi.fastutil.bytes ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]
           [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare make-fastutil-list)


(defn byte-list-cast ^ByteArrayList [item] item)
(defn short-list-cast ^ShortArrayList [item] item)
(defn int-list-cast ^IntArrayList [item] item)
(defn long-list-cast ^LongArrayList [item] item)
(defn float-list-cast ^FloatArrayList [item] item)
(defn double-list-cast ^DoubleArrayList [item] item)


(defmacro datatype->list-cast-fn
  [datatype item]
  (case datatype
    :int8 `(byte-list-cast ~item)
    :int16 `(short-list-cast ~item)
    :int32 `(int-list-cast ~item)
    :int64 `(long-list-cast ~item)
    :float32 `(float-list-cast ~item)
    :float64 `(double-list-cast ~item)))


(defmacro datatype->buffer-creation-length
  [datatype src-ary len]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary 0 ~len)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary 0 ~len)
    :int32 `(IntBuffer/wrap ^ints ~src-ary 0 ~len)
    :int64 `(LongBuffer/wrap ^longs ~src-ary 0 ~len)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary 0 ~len)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary 0 ~len)))


(defmacro extend-fastutil-type
  [typename datatype]
  `(clojure.core/extend
       ~typename
     base/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     base/PContainerType
     {:container-type (fn [_#] :typed-buffer)}
     base/PAccess
     {:get-value (fn [item# ^long idx#]
                   (base/get-value (primitive/->buffer-backing-store item#) idx#))
      :set-value! (fn [item# ^long offset# value#]
                    (base/set-value! (primitive/->buffer-backing-store item#) offset# value#))
      :set-constant! (fn [item# ^long offset# value# ^long elem-count#]
                       (base/set-constant! (primitive/->buffer-backing-store item#)
                                           offset# value# elem-count#))}

     base/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/copy-raw->item! raw-data# (primitive/->buffer-backing-store ary-target#)
                                               target-offset# options#))}
     base/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (when-not (= 1 (base/shape->ecount shape#))
                          (throw (ex-info "Base containers cannot have complex shapes"
                                          {:shape shape#})))
                        (make-fastutil-list datatype# (base/shape->ecount shape#)))}

     primitive/PToBuffer
     {:->buffer-backing-store (fn [item#]
                                (let [item# (datatype->list-cast-fn ~datatype item#)]
                                  (datatype->buffer-creation-length ~datatype (.elements item#) (.size item#))))}

     primitive/POffsetable
     {:offset-item (fn [src-buf# offset#]
                     (primitive/offset-item (primitive/->buffer-backing-store src-buf#)
                                            offset#))}

     primitive/PToArray
     {:->array (fn [item#]
                 (let [item# (datatype->list-cast-fn ~datatype item#)
                       backing-store# (.elements item#)]
                   (when (= (.size item#) (alength backing-store#))
                     backing-store#)))
      :->array-copy (fn [item#]
                      (let [dst-ary# (primitive/make-array-of-type ~datatype
                                                                   (mp/element-count item#))]
                        (base/copy! item# dst-ary#)))}

     mp/PElementCount
     {:element-count (fn [item#]
                       (-> (datatype->list-cast-fn ~datatype item#)
                           (.size)))}))


(extend-fastutil-type ByteArrayList :int8)
(extend-fastutil-type ShortArrayList :int16)
(extend-fastutil-type IntArrayList :int32)
(extend-fastutil-type LongArrayList :int64)
(extend-fastutil-type FloatArrayList :float32)
(extend-fastutil-type DoubleArrayList :float64)


(defn make-fastutil-list
  ([datatype elem-count-or-seq options]
   (let [src-data (primitive/make-array-of-type datatype elem-count-or-seq options)]
     (case datatype
       :int8 (ByteArrayList/wrap ^bytes src-data)
       :int16 (ShortArrayList/wrap ^shorts src-data)
       :int32 (IntArrayList/wrap ^ints src-data)
       :int64 (LongArrayList/wrap ^longs src-data)
       :float32 (FloatArrayList/wrap ^floats src-data)
       :float64 (DoubleArrayList/wrap ^doubles src-data))))
  ([datatype elem-count-or-seq]
   (make-fastutil-list datatype elem-count-or-seq {})))
