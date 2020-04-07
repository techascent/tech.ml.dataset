(ns tech.ml.dataset.print
  (:require [clojure.pprint :as pp]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.pprint :as dtype-pp])
  (:import [tech.v2.datatype ObjectReader]
           [java.util List]))


(def ^:dynamic *default-float-format* "%.3f")
(def ^:dynamic *default-print-length* 25)


(defn print-table
  ([ks data]
     (->> data
          (map (fn [item-map]
                 (->> item-map
                      (map (fn [[k v]]
                             [k (if (or (float? v)
                                        (double? v))
                                  (format *default-float-format* v)
                                  v)]))
                      (into {}))))
          (pp/print-table ks)))
  ([data]
   (print-table (sort (keys (first data))) data)))


(defn value-reader
  "Return a reader that produces a vector of column values per index."
  ^ObjectReader [dataset]
  (let [n-elems (long (second (dtype/shape dataset)))
        readers (->> (ds-proto/columns dataset)
                     (map (comp dtype-pp/reader-converter dtype/->reader)))]
    (reify ObjectReader
      (lsize [rdr] n-elems)
      (read [rdr idx] (vec (map #(.get ^List % idx) readers))))))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value"
  [dataset]
  (let [colnames (ds-proto/column-names dataset)]
    (->> (value-reader dataset)
         (unary-op/unary-reader
          :object
          (zipmap colnames x)))))



(defn print-dataset
  [dataset & {:keys [column-names index-range]
              :or {column-names :all}}]
  (let [index-range (or index-range
                        (range
                         (min (second (dtype/shape dataset))
                              *default-print-length*)))
        print-ds (ds-proto/select dataset column-names index-range)
        column-names (ds-proto/column-names print-ds)]
    (with-out-str
      (print-table column-names (mapseq-reader print-ds)))))
