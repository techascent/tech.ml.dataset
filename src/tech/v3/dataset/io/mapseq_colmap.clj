(ns ^:no-doc tech.v3.dataset.io.mapseq-colmap
  "Sequences of maps are maybe the most basic pure datastructure for data.
  Converting them into a more structured form (and back) is a key component of
  dealing with datasets"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.protocols :as hamf-proto])
  (:import [java.util HashMap Map$Entry Map Map$Entry LinkedHashMap Iterator]
           [java.util.function Function Consumer]
           [tech.v3.dataset.protocols PDatasetParser PClearable]
           [clojure.lang IDeref Counted Indexed]
           [ham_fisted Reductions$IndexedAccum Reducible Consumers$IncConsumer
            ITypedReduce]))


(defrecord ParseRecord [^long col-idx column-name column-parser]
  ds-proto/PClearable
  (ds-clear [this] (ds-proto/ds-clear column-parser)))


(deftype ^:private MapseqReducer [options parsers consumer ^Consumers$IncConsumer row-idx]
  Consumer
  (accept [this row]
    (hamf-rf/consume! consumer row)
    (.inc row-idx))
  Counted
  (count [this] (.value row-idx))
  PDatasetParser
  (add-row [this row] (.accept this row))
  (add-rows [this rows] (hamf-rf/consume! this rows))
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
    (if (.isEmpty ^Map parsers)
      {:tech.v3.dataset/row-count (.value row-idx)
       :tech.v3.dataset/missing :all}
      (parse-context/parsers->dataset (assoc options :key-fn nil) parsers (.value row-idx))))
  PClearable
  (ds-clear [this]
    (reduce (fn [_ p] (ds-proto/ds-clear p)) nil (.values ^Map parsers))
    (.setValue row-idx 0)
    this))


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
             colparser-compute-fn (hamf-fn/function
                                   colname
                                   (ParseRecord. (.size parsers) (key-fn colname) (parse-context colname)))
             colname->parser (fn [colname]
                               (.-column-parser ^ParseRecord
                                                (.computeIfAbsent parsers colname
                                                                  colparser-compute-fn)))
             row-idx (hamf/inc-consumer)
             consumer (hamf-fn/consumer
                       e
                       (let [^Map$Entry e e
                             parser (colname->parser (.getKey e))]
                         (column-parsers/add-value! parser (.value row-idx) (.getValue e))))]
         (MapseqReducer. options parsers consumer row-idx)))
    (->rfn [r] hamf-rf/consumer-accumulator)
    hamf-proto/Finalize
    (finalize [r v] @v)))


(defn mapseq->dataset
  ([options mapseq]
   (hamf-rf/reduce-reducer (mapseq-reducer options) mapseq))
  ([mapseq]
   (mapseq->dataset {} mapseq)))


(defn column-map->dataset
  ([options column-map]
   (let [parse-context (parse-context/options->parser-fn options :object)
         coltypes (->> column-map
                       (group-by (fn [e] (argtypes/arg-type (val e)))))
         parse-scalar (fn [e ^long n-rows]
                        #:tech.v3.dataset{:name (key e)
                                          :data (dtype/const-reader (val e) n-rows)})]
     ;;new rule - anything iterable can only be iterated once.  Not all iterables can be re-iterated
     ;;without kicking off a potentially expensive process again
     ;;This means that if we haven't decided on n-rows yet the iteration of all
     ;;the iterables must both create columns *and* take the min length to be the new n-rows.

     ;;If we have readers those dictate the dataset length
     (if-let [rdr-cols (:reader coltypes)]
       (let [n-rows (long (apply max (map (comp dtype/ecount val) rdr-cols)))]
         (->> column-map
              (pfor/pmap
               (fn [e]
                 (let [coldata (val e)
                       colname (key e)]
                   (case (argtypes/arg-type coldata)
                     :scalar (parse-scalar e n-rows)
                     :reader (if (identical? :object (dtype/elemwise-datatype coldata))
                               (let [parser (parse-context colname)]
                                 (reduce (hamf-rf/indexed-accum
                                          acc idx v
                                          (column-parsers/add-value! parser idx v))
                                         nil
                                         coldata)
                                 (assoc (column-parsers/finalize! parser n-rows)
                                        :tech.v3.dataset/name colname))
                               #:tech.v3.dataset{:name colname
                                                 :data coldata})
                     :iterable (let [parser (parse-context colname)]
                                 (reduce (hamf-rf/indexed-accum
                                          acc idx v
                                          (if (< idx n-rows)
                                            (column-parsers/add-value! parser idx v)
                                            (reduced true)))
                                         nil coldata)
                                 (assoc (column-parsers/finalize! parser n-rows)
                                        :tech.v3.dataset/name colname))))))
              (ds-impl/new-dataset options)))
       ;;If we only have iterables the shortest one dictates dataset length
       (if-let [iter-cols (:iterable coltypes)]
         (let [[n-rows iterable-cols]
               (let [iterable-cols iter-cols
                     parse-contexts (mapv (fn [e]
                                            (let [cname (key e)]
                                              [cname
                                               (parse-context cname)
                                               (.iterator ^Iterable (ham-fisted.protocols/->iterable (val e)))]))
                                          iterable-cols)
                     val-data (object-array (count parse-contexts))
                     next-valid?
                     (fn [^long row-idx]
                       (let [all-valid?
                             (reduce (hamf-rf/indexed-accum
                                      acc idx pctx
                                      (let [^Iterator iter (nth pctx 2)]
                                        (if (.hasNext iter)
                                          (do
                                            (aset val-data idx (.next iter))
                                            true)
                                          (reduced false))))
                                     true
                                     parse-contexts)]
                         (when all-valid?
                           (reduce (hamf-rf/indexed-accum
                                    acc idx pctx
                                    (column-parsers/add-value! (nth pctx 1) row-idx (aget val-data idx)))
                                   nil
                                   parse-contexts)
                           true)))
                     n-rows
                     (loop [row-idx 0]
                       (if (next-valid? row-idx)
                         (recur (unchecked-inc row-idx))
                         row-idx))]
                 [n-rows
                  (into {} (map (fn [val]
                                  [(nth val 0)
                                   (assoc (column-parsers/finalize! (nth val 1) n-rows)
                                          :tech.v3.dataset/name (nth val 0))]))
                        parse-contexts)])]
           (->> column-map
                (map
                 (fn [e]
                   (let [coldata (val e)
                         colname (key e)]
                     (case (argtypes/arg-type coldata)
                       :scalar (parse-scalar e n-rows)
                       :iterable (get iterable-cols colname)))))
                (ds-impl/new-dataset options)))
         ;;Finally constants have a dataset length of 1
         (->> column-map
              (map #(parse-scalar % 1))
              (ds-impl/new-dataset options))))))
  ([column-map]
   (column-map->dataset nil column-map)))
