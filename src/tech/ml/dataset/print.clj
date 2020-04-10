(ns tech.ml.dataset.print
  (:require [clojure.pprint :as pp]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.datetime :as dtype-dt])
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


(def datetime-epoch-types
  #{:epoch-milliseconds
    :epoch-seconds})


(defn value-reader
  "Return a reader that produces a vector of column values per index."
  (^ObjectReader [dataset {:keys [all-printable-columns?]
                           :as options}]
   (let [n-elems (long (second (dtype/shape dataset)))
         readers
         (->> (ds-proto/columns dataset)
              (map (fn [coldata]
                     (let [col-reader (dtype/->reader coldata)]
                       (if (or all-printable-columns?
                               (not (datetime-epoch-types
                                     (dtype/get-datatype col-reader))))
                         (dtype-pp/reader-converter col-reader)
                         col-reader)))))]
     (reify ObjectReader
       (lsize [rdr] n-elems)
       (read [rdr idx] (vec (map #(.get ^List % idx) readers))))))
  (^ObjectReader [dataset]
   (value-reader dataset {})))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value"
  ([dataset options]
   (let [colnames (ds-proto/column-names dataset)]
     (->> (value-reader dataset )
          (unary-op/unary-reader
           :object
           (zipmap colnames x)))))
  ([dataset]
   (mapseq-reader dataset {})))



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
      (print-table column-names (mapseq-reader print-ds
                                               {:all-printable-columns? true})))))
