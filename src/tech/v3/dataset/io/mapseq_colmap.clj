(ns ^:no-doc tech.v3.dataset.io.mapseq-colmap
  "Sequences of maps are maybe the most basic pure datastructure for data.
  Converting them into a more structured form (and back) is a key component of
  dealing with datasets"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.dataset.impl.dataset :as ds-impl])
  (:import [java.util HashMap Map$Entry Map]
           [java.util.function Function]))


(defn mapseq->dataset
  ([options mapseq]
   (let [rows mapseq
         parse-context (parse-context/options->parser-fn options :object)
         parsers (HashMap.)
         key-fn (:key-fn options identity)
         colparser-compute-fn (reify Function
                                (apply [this colname]
                                  (let [col-idx (.size parsers)]
                                    {:column-idx col-idx
                                     :column-name (key-fn colname)
                                     :column-parser (parse-context colname)})))
         colname->parser (fn [colname]
                           (:column-parser
                            (.computeIfAbsent parsers colname
                                              colparser-compute-fn)))
         iter (pfor/->iterator rows)
         n-rows (loop [continue? (.hasNext iter)
                       row-idx 0]
                  (if continue?
                    (do
                      (when-let [^Map row (.next iter)]
                        (pfor/doiter
                         cell (.entrySet row)
                         (let [^Map$Entry cell cell
                               k (.getKey cell)
                               v (.getValue cell)
                               parser (colname->parser k)]
                           (column-parsers/add-value! parser row-idx v))))
                      (recur (.hasNext iter)
                             (unchecked-inc row-idx)))
                    row-idx))]
     ;;key-fn has already been applied
     (parse-context/parsers->dataset (assoc options :key-fn nil) parsers n-rows)
     ))
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
