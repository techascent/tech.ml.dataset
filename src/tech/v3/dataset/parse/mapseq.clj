(ns ^:no-doc tech.v3.dataset.parse.mapseq
  "Sequences of maps are maybe the most basic pure datastructure for data.
  Converting them into a more structured form (and back) is a key component of
  dealing with datatets"
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.parse.column-parsers :as column-parsers]
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
         colname->parse-context (fn [colname]
                                  (:column-parser
                                   (.computeIfAbsent parsers colname
                                                     colparser-compute-fn)))]
     (pfor/consume!
      (fn [[row-idx rowmap]]
        (doseq [[k v] rowmap]
          (let [parser (colname->parse-context k)]
            (column-parsers/add-value! parser row-idx v))))
      rows)
     (let [row-count (apply max 0 (map (comp dtype/ecount :column-parser) (vals parsers)))]
       (->> parsers
            (sort-by :column-idx)
            (mapv (fn [{:keys [column-name column-parser]}]
                    (assoc (column-parsers/finalize! column-parser row-count)
                           :name column-name)))
            (ds-impl/new-dataset options)))))
  ([mapseq]
   (mapseq->dataset {} mapseq)))
