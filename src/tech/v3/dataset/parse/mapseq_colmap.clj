(ns ^:no-doc tech.v3.dataset.parse.mapseq-colmap
  "Sequences of maps are maybe the most basic pure datastructure for data.
  Converting them into a more structured form (and back) is a key component of
  dealing with datasets"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.parse.column-parsers :as column-parsers]
            [tech.v3.dataset.parse.context :as parse-context]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.dataset.impl.dataset :as ds-impl])
  (:import [java.util HashMap]
           [java.util.function Function]))


(defn mapseq->dataset
  ([options mapseq]
   (let [rows (map-indexed vector mapseq)
         parse-context (column-parsers/options->parse-context options :object)
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
                                              colparser-compute-fn)))]
     (pfor/consume!
      (fn [[row-idx rowmap]]
        (doseq [[k v] rowmap]
          (let [parser (colname->parser k)]
            (column-parsers/add-value! parser row-idx v))))
      rows)
     (parse-context/parsers->dataset parsers)))
  ([mapseq]
   (mapseq->dataset {} mapseq)))


(defn column-map->dataset
  ([options column-map]
   (let [parse-context (column-parsers/options->parse-context options :object)]
     (->> column-map
          (map (fn [[colname coldata]]
                 ;;Fastpath for non-object already-niceified data
                 (if (and (dtype/reader? coldata)
                          (not= :object (dtype/elemwise-datatype coldata)))
                   {:name colname
                    :data coldata}
                   (let [parser (parse-context colname)]
                     (pfor/consume!
                      #(column-parsers/add-value! parser (first %) (second %))
                      (map-indexed vector coldata))
                     (column-parsers/finalize! parser (dtype/ecount parser))))))
          (ds-impl/new-dataset options))))
  ([column-map]
   (column-map->dataset nil column-map)))
