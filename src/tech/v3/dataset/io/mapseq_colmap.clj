(ns ^:no-doc tech.v3.dataset.io.mapseq-colmap
  "Sequences of maps are maybe the most basic pure datastructure for data.
  Converting them into a more structured form (and back) is a key component of
  dealing with datasets"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.protocols :as hamf-proto])
  (:import [java.util HashMap Map$Entry Map Map$Entry LinkedHashMap]
           [java.util.function Function Consumer]
           [clojure.lang IDeref]
           [ham_fisted Reductions$IndexedAccum Reducible]))


(defrecord ^:private MapseqReducer [options
                                    parse-context
                                    ^Map parsers
                                    key-fn
                                    colname->parser
                                    row-idx*]
  Consumer
  (accept [this row]
    (let [row-idx (long @row-idx*)]
      (vswap! row-idx* unchecked-inc)
      (hamf/consume!
       (hamf/consumer
        e
        (let [^Map$Entry e e
              parser (colname->parser (.getKey e))]
          (column-parsers/add-value! parser row-idx (.getValue e))))
       row)))
  IDeref
  (deref [this]
    (parse-context/parsers->dataset (assoc options :key-fn nil) parsers @row-idx*)))


(defn mapseq-reducer
  "Create a non-parallelized mapseq hamf reducer.  If you want a single IFn that follows the
  convention of clojure transducers call hamf/reducer->rfn."
  [options]
  (reify
    hamf-proto/Reducer
    (->init-val-fn [r]
      #(let [parse-context (parse-context/options->parser-fn options :object)
             parsers (LinkedHashMap.)
             key-fn (:key-fn options identity)
             colparser-compute-fn (hamf/function
                                   colname
                                   (let [col-idx (.size parsers)]
                                     {:column-idx col-idx
                                      :column-name (key-fn colname)
                                      :column-parser (parse-context colname)}))
             colname->parser (fn [colname]
                               (:column-parser
                                (.computeIfAbsent parsers colname
                                                  colparser-compute-fn)))
             row-idx* (volatile! 0)]
         (MapseqReducer. options parse-context parsers key-fn colname->parser row-idx*)))
    (->rfn [r] hamf/consumer-accumulator)
    hamf-proto/Finalize
    (finalize [r v] @v)))


(defn mapseq->dataset
  ([options mapseq]
   (hamf/reduce-reducer (mapseq-reducer options) mapseq))
  ([mapseq]
   (mapseq->dataset {} mapseq)))


(defn column-map->dataset
  ([options column-map]
   (let [parse-context (parse-context/options->parser-fn options :object)
         coltypes (->> (vals column-map)
                       (group-by argtypes/arg-type))
         n-rows (cond
                  (:reader coltypes)
                  (apply max (map dtype/ecount (coltypes :reader)))
                  (:iterable coltypes)
                  (count (apply map (constantly nil) (coltypes :iterable)))
                  :else
                  1)]
     (->> column-map
          (pfor/pmap
           (fn [^Map$Entry mapentry]
             (let [colname (.getKey mapentry)
                   coldata (.getValue mapentry)
                   argtype (argtypes/arg-type coldata)
                   coldata (if (= argtype :iterable)
                             (take n-rows coldata)
                             coldata)]
               (if (= :scalar argtype)
                 #:tech.v3.dataset{:name colname
                                   :data (dtype/const-reader coldata n-rows)}
                 (if (and (= :reader argtype)
                          (not= :object (dtype/elemwise-datatype coldata)))
                   #:tech.v3.dataset{:name colname
                                     :data coldata}
                   ;;Actually attempt to parse the data
                   (let [parser (parse-context colname)
                         retval
                         (do
                           (pfor/consume!
                            #(column-parsers/add-value! parser (first %) (second %))
                            (map-indexed vector coldata))
                           (assoc (column-parsers/finalize! parser (dtype/ecount parser))
                                  :tech.v3.dataset/name colname))]
                     retval))))))
          (ds-impl/new-dataset options))))
  ([column-map]
   (column-map->dataset nil column-map)))
