(ns tech.v3.dataset.reductions.impl
  "Helper namespace to help make reductions.clj more understandable."
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.base :as ds-base]
            [ham-fisted.api :as hamf]
            [ham-fisted.protocols :as hamf-proto])
  (:import [clojure.lang IFn$OLO IFn$ODO]
           [ham_fisted Transformables]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn reducer->column-reducer
  ([reducer cname]
   (reducer->column-reducer reducer nil cname))
  ([reducer op-space cname]
   (let [merge-fn (hamf-proto/->merge-fn reducer)]
     (reify
       ds-proto/PDatasetReducer
       (ds->reducer [this ds]
         (let [rfn (hamf-proto/->rfn reducer)
               op-space (or op-space
                            (cond
                              (instance? IFn$OLO rfn) :int64
                              (instance? IFn$ODO rfn) :float64
                              :else
                              :object))
               col (dtype/->reader (ds-base/column ds cname) op-space)
               init-fn (hamf-proto/->init-val-fn reducer)]
           (case (casting/simple-operation-space op-space)
             :int64
             (let [rfn (Transformables/toLongReductionFn rfn)]
               (reify
                 hamf-proto/Reducer
                 (->init-val-fn [r] init-fn)
                 (->rfn [r] (hamf/long-accumulator
                             acc v (.invokePrim rfn acc (.readLong col v))))
                 hamf-proto/ParallelReducer
                 (->merge-fn [r] merge-fn)))
             :float32
             (let [rfn (Transformables/toDoubleReductionFn rfn)]
               (reify
                 hamf-proto/Reducer
                 (->init-val-fn [r] init-fn)
                 (->rfn [r] (hamf/long-accumulator
                             acc v (.invokePrim rfn acc (.readDouble col v))))
                 hamf-proto/ParallelReducer
                 (->merge-fn [r] merge-fn)))
             (reify
               hamf-proto/Reducer
               (->init-val-fn [r] init-fn)
               (->rfn [r] (hamf/long-accumulator
                           acc v (rfn acc (.readObject col v))))
               hamf-proto/ParallelReducer
               (->merge-fn [r] merge-fn)))))
       (merge [this lhs rhs] (merge-fn lhs rhs))
       hamf-proto/Finalize
       (finalize [this v] (hamf-proto/finalize reducer v))))))
