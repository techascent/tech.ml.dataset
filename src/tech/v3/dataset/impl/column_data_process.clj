(ns tech.v3.dataset.impl.column-data-process
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.dataset.impl.column-base :as column-base])
  (:import [tech.v3.datatype PrimitiveList]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def object-primitive-array-types
  {(Class/forName "[Ljava.lang.Boolean;") :boolean
   (Class/forName "[Ljava.lang.Byte;") :int8
   (Class/forName "[Ljava.lang.Short;") :int16
   (Class/forName "[Ljava.lang.Character;") :char
   (Class/forName "[Ljava.lang.Integer;") :int32
   (Class/forName "[Ljava.lang.Long;") :int64
   (Class/forName "[Ljava.lang.Float;") :float32
   (Class/forName "[Ljava.lang.Double;") :float64})


(defn scan-data-for-missing
  "Scan container for missing values.  Returns a map of
  {:data :missing}"
  [obj-data]
  (let [obj-data-datatype (dtype/elemwise-datatype obj-data)]
    (if (and (dtype/reader? obj-data)
             (not= :object (casting/flatten-datatype obj-data-datatype)))
      {:data obj-data
       :missing (bitmap/->bitmap)}
      (let [dst-container-type (get object-primitive-array-types
                                    (type obj-data)
                                    (dtype/elemwise-datatype obj-data))
            sparse-val (get column-base/dtype->missing-val-map dst-container-type)
            sparse-indexes (bitmap/->bitmap)]
        (if-let [obj-data (dtype/as-reader obj-data)]
          (let [n-items (dtype/ecount obj-data)
                dst-data (dtype/make-container :jvm-heap dst-container-type n-items)
                dst-io (dtype/->primitive-io dst-data)]
            (parallel-for/parallel-for
             idx
             n-items
             (let [obj-data (.readObject obj-data idx)]
               (if (not (nil? obj-data))
                 (.writeObject dst-io idx obj-data)
                 (locking sparse-indexes
                   (.add sparse-indexes idx)
                   (.writeObject dst-io idx sparse-val)))))
            {:data dst-data
             :missing sparse-indexes})
          (let [^PrimitiveList dst-data (dtype/make-container :list dst-container-type 0)]
            (parallel-for/consume!
             #(if-not (nil? %)
                (.addObject dst-data %)
                (do
                  (let [idx (.size dst-data)]
                    (.add sparse-indexes idx)
                    (.addObject dst-data sparse-val))))
             obj-data)
            {:data dst-data
             :missing sparse-indexes}))))))
