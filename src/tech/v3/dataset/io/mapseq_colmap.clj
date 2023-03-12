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
           [tech.v3.dataset.protocols PDatasetParser]
           [clojure.lang IDeref Counted Indexed]
           [ham_fisted Reductions$IndexedAccum Reducible Consumers$IncConsumer
            ITypedReduce]))


(defrecord ParseRecord [^long col-idx column-name column-parser])


(deftype ^:private MapseqReducer [options parsers consumer ^Consumers$IncConsumer row-idx]
  Consumer
  (accept [this row]
    (hamf/consume! consumer row)
    (.inc row-idx))
  Counted
  (count [this] (.value row-idx))
  PDatasetParser
  (add-row [this row] (.accept this row))
  (add-rows [this rows] (hamf/consume! this rows))
  Indexed
  (nth [this idx] (nth this idx nil))
  (nth [this idx dv]
    (let [ec (.value row-idx)
          idx (if (< idx 0)
                (+ idx ec)
                idx)]
      (if (or (< idx 0)
              (>= idx ec))
        dv
        (into {} (map (fn [^ParseRecord pr]
                        [(.-column-name pr) (nth (.-column-parser pr)
                                                 (unchecked-int idx))]))
              (.values ^Map parsers)))))
  ITypedReduce
  (reduce [this rfn init]
    (let [parser-vals (.values ^Map parsers)]
      (reduce (fn [acc ^long idx]
                (rfn acc (into {} (map (fn [^ParseRecord pr]
                                         [(.-column-name pr) (nth (.-column-parser pr)
                                                                  (unchecked-int idx))]))
                               parser-vals)))
              init (hamf/range (.value row-idx)))))
  IDeref
  (deref [this]
    (parse-context/parsers->dataset (assoc options :key-fn nil) parsers (.value row-idx))))


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
                                   (ParseRecord. (.size parsers) (key-fn colname) (parse-context colname)))
             colname->parser (fn [colname]
                               (.-column-parser ^ParseRecord
                                                (.computeIfAbsent parsers colname
                                                                  colparser-compute-fn)))
             row-idx (hamf/inc-consumer)
             consumer (hamf/consumer
                       e
                       (let [^Map$Entry e e
                             parser (colname->parser (.getKey e))]
                         (column-parsers/add-value! parser (.value row-idx) (.getValue e))))]
         (MapseqReducer. options parsers consumer row-idx)))
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
