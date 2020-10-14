(ns tech.v3.dataset.reductions
  (:require [tech.v3.dataset.base :as ds-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype IndexReduction Buffer]
           [java.util Map Map$Entry HashMap List Set HashSet]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer Function DoubleConsumer]
           [java.util.stream Stream]
           [tech.v3.datatype DoubleReader DoubleConsumers$Sum]
           [it.unimi.dsi.fastutil.ints Int2ObjectMap
            Int2ObjectOpenHashMap]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn aggregate-group-by-column-reduce
  "Returns a java stream of results."
  (^Stream [column-name ^IndexReduction reducer ds-seq options]
   (let [reduction (ConcurrentHashMap.)
         _ (doseq [dataset ds-seq]
             (let [batch-data (.prepareBatch reducer dataset)]
               (dtype-reductions/unordered-group-by-reduce
                reducer batch-data (dataset column-name) reduction)))]
     (-> (.entrySet ^Map reduction)
         (.parallelStream)
         (.map (reify Function
                 (apply [this v]
                   (let [v ^Map$Entry v]
                     [(.getKey v)
                      (.finalize reducer (.getValue v))])))))))
  (^Stream [column-name reducer ds-seq]
   (aggregate-group-by-column-reduce column-name reducer ds-seq {})))


(defn dsum-reducer
  "Return the summation of a column in double space as the result of a reduction.
  Reduces to a map of {:n-elems :sums {colname summation}}"
  ^IndexReduction [colname-seq]
  (reify IndexReduction
    (prepareBatch [this ds]
      (mapv #(dtype/->reader (ds %)) colname-seq))
    (reduceIndex [this readers ctx idx]
      (let [^List readers readers
            n-readers (.size readers)
            ^objects ctx (if ctx
                           ctx
                           (object-array (repeatedly n-readers #(DoubleConsumers$Sum.))))]
        (dotimes [ary-idx n-readers]
          (.accept ^DoubleConsumer (aget ctx ary-idx)
                   (.readDouble ^Buffer (.get readers ary-idx)
                                idx)))
        ctx))
    (reduceReductions [this lhs rhs]
      (let [^objects lhs lhs
            ^objects rhs rhs]
        (dotimes [idx (alength lhs)]
          (let [^DoubleConsumers$Sum lhs-cons (aget lhs idx)
                ^DoubleConsumers$Sum rhs-cons (aget rhs idx)]
            (set! (.-value  lhs-cons) (pmath/+ (.-value lhs-cons) (.-value rhs-cons)))
            (set! (.-nElems lhs-cons) (pmath/+ (.-nElems lhs-cons) (.-nElems rhs-cons)))))
        lhs))
    (finalize [this lhs]
      (let [^objects lhs lhs]
        (let [^DoubleConsumers$Sum first-cons (aget lhs 0)]
          {:n-elems (.-nElems first-cons)
           :sums (->> (map vector
                           colname-seq
                           (map (fn [^DoubleConsumers$Sum data]
                                  (.-value data))
                                lhs))
                      (into {}))})))))


(defn unique-reducer
  "Reduces to a java.util.HashSet of unique elements.
  Returns a map of {colname unique-set}"
  ^IndexReduction [colname-seq]
  (reify IndexReduction
    (prepareBatch [this ds]
      (mapv #(dtype/->reader (ds %) :object) colname-seq))
    (reduceIndex [this readers ctx idx]
      (let [^List readers readers
            n-readers (.size readers)
            ctx (if ctx
                  ctx
                  (let [obj-ary
                        (object-array n-readers)]
                    (dotimes [idx n-readers]
                      (aset obj-ary idx (HashSet.)))
                    obj-ary))
            ^objects data-ary ctx]
        (dotimes [ary-idx n-readers]
          (.add ^Set (aget data-ary ary-idx) ((.get readers ary-idx) idx)))
        ctx))
    (reduceReductions [this lhs rhs]
      (let [^objects lhs lhs
            ^objects rhs rhs]
        (dotimes [idx (alength lhs)]
          (.addAll ^Set (aget lhs idx) ^Set (aget rhs idx))))
      lhs)
    (finalize [this lhs]
      (->> (map vector colname-seq lhs)
           (into {})))))


(defn aggregate-reducer
  ^IndexReduction [reducer-map]
  (let [reducer-names (keys reducer-map)
        reducer-seq (object-array (vals reducer-map))
        n-reducers (alength reducer-seq)]
    (reify IndexReduction
      (prepareBatch [this dataset]
        (object-array (map #(.prepareBatch ^IndexReduction % dataset) reducer-seq)))
      (reduceIndex [this ds-ctx obj-ctx idx]
        (let [^objects ds-ctx ds-ctx
              ^objects obj-ctx (if obj-ctx
                                 obj-ctx
                                 (object-array n-reducers))]
          (dotimes [r-idx n-reducers]
            (aset obj-ctx r-idx
                  (.reduceIndex ^IndexReduction (aget reducer-seq r-idx)
                                (aget ds-ctx r-idx)
                                (aget obj-ctx r-idx)
                                idx)))
          obj-ctx))
      (reduceReductions [this lhs-ctx rhs-ctx]
        (let [^objects lhs-ctx lhs-ctx
              ^objects rhs-ctx rhs-ctx]
          (dotimes [r-idx n-reducers]
            (aset lhs-ctx r-idx
                  (.reduceReductions ^IndexReduction (aget reducer-seq r-idx)
                                     (aget lhs-ctx r-idx)
                                     (aget rhs-ctx r-idx))))
          lhs-ctx))
      (finalize [this ctx]
        (let [^objects ctx ctx]
          (->> (map (fn [reducer-name reducer ctx]
                      [reducer-name
                       (.finalize ^IndexReduction reducer ctx)])
                    reducer-names
                    reducer-seq
                    ctx)
               (into {})))))))


(comment
  (def stocks (ds-base/->dataset "test/data/stocks.csv"))
  (-> (aggregate-group-by-column-reduce
       "symbol"
       (reify IndexReduction
         (reduceIndex [this ds ctx idx]
           (let [{:keys [price sum n-elems] :as ctx}
                 (if ctx
                   ctx
                   {:price (dtype/->reader (ds "price"))
                    :sum 0.0
                    :n-elems 0})]
             (-> ctx
                 (update :sum (fn [^double sum]
                                (+ sum (price idx))))
                 (update :n-elems inc))))
         (reduceReductions [this lhs-ctx rhs-ctx]
           (let [sum (+ (double (:sum lhs-ctx))
                        (double (:sum rhs-ctx)))
                 n-elems (+ (:n-elems lhs-ctx)
                            (:n-elems rhs-ctx))]
             {:sum sum
              :n-elems n-elems}))
         (finalize [this ctx] (select-keys ctx [:sum :n-elems])))
       [stocks]))


  (aggregate-group-by-column-reduce
   "symbol"
   (dsum-reducer ["price"])
   [stocks stocks stocks])


  (aggregate-group-by-column-reduce
   "date"
   (unique-reducer ["symbol"])
   [stocks])


  (aggregate-group-by-column-reduce
   "symbol"
   (aggregate-reducer {:summations (dsum-reducer ["price"])
                       :sets (unique-reducer ["date"])})
   [stocks stocks stocks])


  )
