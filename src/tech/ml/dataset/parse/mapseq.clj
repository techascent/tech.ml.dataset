(ns tech.ml.dataset.parse.mapseq
  "Sequences of maps are maybe the most basic pure datastructure for data.
  Converting them into a more structured form (and back) is a key component of
  dealing with datatets"
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.ml.dataset.parse.spreadsheet :as parse-spreadsheet]
            [clojure.set :as set])
  (:import [java.util HashMap List Iterator Map]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]
           [tech.libs Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]))


(defn map->row
  ^Spreadsheet$Row
  [row-number map-data colname->idx]
  (let [vals (into [] map-data)
        row-number (int row-number)]
    (reify Spreadsheet$Row
      (getRowNum [this] row-number)
      (iterator [this]
        (let [^Iterator src-iter (.iterator ^List vals)]
          (reify Iterator
            (hasNext [this] (.hasNext src-iter))
            (next [this]
              (let [[k v] (.next src-iter)
                    col-index (colname->idx k)]
                (reify
                  dtype-proto/PDatatype
                  (get-datatype [this]
                    (if (= :scalar (argtypes/arg->arg-type v))
                      (dtype-proto/get-datatype v)
                      :object))
                  Spreadsheet$Cell
                  (missing [this] (nil? v))
                  (getColumnNum [this] (int col-index))
                  (value [this] v)
                  (doubleValue [this] (double v))
                  (boolValue [this] (boolean v)))))))))))


(defn mapseq->dataset
  ([mapseq {:keys [parser-scan-len parser-fn]
            :or {parser-scan-len 100}
            :as options}]
   (let [cell-name-hash (ConcurrentHashMap.)
         colname-compute-fn (reify Function
                              (apply [this k]
                                (long (.size cell-name-hash))))
         colname->idx (fn [colname]
                        (.computeIfAbsent cell-name-hash colname
                                          colname-compute-fn))
         sheet-name (or (:table-name options) "_unnamed")
         rows (->> mapseq
                   (map-indexed (fn [idx data]
                                  (map->row idx data colname->idx))))

         scan-rows (when parser-fn
                     (parse-spreadsheet/scan-initial-rows rows parser-scan-len))
         initial-idx->colname (set/map-invert cell-name-hash)
         col-parser-gen (reify
                          Function
                          (apply [this column-number]
                            (if parser-fn
                              (let [colname (get initial-idx->colname
                                                 (long column-number))]
                                (parse-spreadsheet/make-parser
                                 parser-fn colname (scan-rows column-number)))
                              (parse-spreadsheet/default-column-parser))))
         col-idx->colname-data (atom nil)]
     (parse-spreadsheet/process-spreadsheet-rows
      rows false col-parser-gen
      #(let [mapdata (swap! col-idx->colname-data
                            (fn [existing]
                              (if existing
                                existing
                                (set/map-invert cell-name-hash))))]
         (get mapdata % %))
      sheet-name)))
  ([mapseq]
   (mapseq->dataset mapseq {})))
