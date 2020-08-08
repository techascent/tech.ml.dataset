(ns tech.ml.dataset.reductions
  (:require [tech.ml.dataset.base :as ds-base]
            [tech.parallel.for :as parallel-for]
            [tech.parallel :as parallel]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [primitive-math :as pmath])
  (:import [tech.ml.dataset IndexReduction IndexReduction$IndexedBiFunction]
           [java.util Map Map$Entry HashMap List Set HashSet]
           [java.util.function BiFunction BiConsumer Function]
           [java.util.stream Stream]
           [tech.v2.datatype DoubleReader]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn aggregate-group-by-column-reduce
  "Returns a java stream of results."
  (^Stream [column-name ^IndexReduction reducer ds-seq options]
   (let [reduce-bi-fn (reify
                        BiFunction
                        (apply [this lhs rhs]
                          (.reduceReductions reducer lhs rhs)))
         reduce-maps-fn (fn
                          ([^Map lhs ^Map rhs]
                           (.forEach
                            rhs
                            (reify BiConsumer
                              (accept [this k v]
                                (.merge lhs k v reduce-bi-fn))))
                           lhs)
                          ([lhs] lhs))
         reduction
         (->> ds-seq
              (parallel/queued-pmap
               3
               (fn [dataset]
                 (let [coldata (dtype/->reader (dataset column-name) :object)
                  n-elems (dtype/ecount coldata)
                  dataset-context (.datasetContext reducer dataset)]
                   (parallel-for/indexed-map-reduce
                    n-elems
                    (fn [^long start-idx ^long n-groups]
                      (let [groups (HashMap.)
                            end-idx (long (+ start-idx n-groups))
                            compute-fn (IndexReduction$IndexedBiFunction.
                                        dataset-context reducer)]
                        (loop [idx start-idx]
                          (if (< idx end-idx)
                            (do
                              (.setIndex compute-fn idx)
                              (.compute groups (coldata idx) compute-fn)
                              (recur (unchecked-inc idx)))
                            groups))))
                    (partial reduce reduce-maps-fn)))))
              (reduce reduce-maps-fn))]
     (-> (.entrySet ^Map reduction)
         (.parallelStream)
         (.map (reify Function
                 (apply [this v]
                   (let [v ^Map$Entry v]
                     [(.getKey v)
                      (.finalize reducer (.getValue v))])))))))
  (^Stream [column-name reducer ds-seq]
   (aggregate-group-by-column-reduce column-name reducer ds-seq {})))


(defrecord DSUMRecord [^doubles data-ary ^longs n-elem-ary])


(defn dsum-reducer
  "Return the summation of a column in double space as the result of a reduction.
  Reduces to a map of {:n-elems :sums {colname summation}}"
  ^IndexReduction [colname-seq]
  (reify IndexReduction
    (datasetContext [this ds]
      (mapv #(typecast/datatype->reader :float64 (ds %)) colname-seq))
    (reduceIndex [this readers ctx idx]
      (let [^List readers readers
            n-readers (.size readers)
            first? (not ctx)
            ^DSUMRecord ctx (if ctx
                              ctx
                              (->DSUMRecord (double-array n-readers) (long-array 1)))
            ^doubles data-ary (.data-ary ctx)
            ^longs n-elem-ary (.n-elem-ary ctx)]
        (if first?
          (dotimes [ary-idx n-readers]
            (aset data-ary ary-idx (.read ^DoubleReader (.get readers ary-idx)
                                          idx)))
          (dotimes [ary-idx n-readers]
            (aset data-ary ary-idx (pmath/+
                                    (aget data-ary ary-idx)
                                    (.read ^DoubleReader (.get readers ary-idx)
                                           idx)))))
        (aset n-elem-ary 0 (unchecked-inc (aget n-elem-ary 0)))
        ctx))
    (reduceReductions [this lhs rhs]
      (let [^DSUMRecord lhs lhs
            ^DSUMRecord rhs rhs
            ^doubles lhs-data (.data-ary lhs)
            ^longs lhs-nelems (.n-elem-ary lhs)
            ^doubles rhs-data (.data-ary rhs)
            ^longs rhs-nelems (.n-elem-ary rhs)]
        (aset lhs-nelems 0 (pmath/+ (aget rhs-nelems 0)
                                    (aget lhs-nelems 0)))
        (dotimes [idx (alength lhs-data)]
          (aset lhs-data idx (pmath/+ (aget lhs-data idx)
                                      (aget rhs-data idx))))
        lhs))
    (finalize [this lhs]
      (let [^DSUMRecord lhs lhs]
        {:n-elems (aget ^longs (.n-elem-ary lhs) 0)
         :sums (->> (map vector colname-seq (.data-ary lhs))
                    (into {}))}))))


(defn unique-reducer
  "Reduces to a java.util.HashSet of unique elements.
  Returns a map of {colname unique-set}"
  ^IndexReduction [colname-seq]
  (reify IndexReduction
    (datasetContext [this ds]
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
      (datasetContext [this dataset]
        (object-array (map #(.datasetContext ^IndexReduction % dataset) reducer-seq)))
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
  (aggregate-group-by-column-reduce
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
          :avg (/ sum n-elems)
          :n-elems n-elems}))
     (finalize [this ctx] ctx))
   [stocks])


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
