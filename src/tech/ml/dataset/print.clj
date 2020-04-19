(ns tech.ml.dataset.print
  (:require [clojure.pprint :as pp]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.datetime :as dtype-dt])
  (:import [tech.v2.datatype ObjectReader]
           [java.util List]
           [tech.ml.dataset FastStruct]
           [clojure.lang PersistentStructMap$Def
            PersistentVector]))

(set! *warn-on-reflection* true)

(def ^:dynamic *default-table-row-print-length* 25)


(defn print-table
  ([ks data]
     (->> data
          (map (fn [item-map]
                 (->> item-map
                      (map (fn [[k v]]
                             [k (dtype-pp/format-object v)]))
                      (into {}))))
          (pp/print-table ks)))
  ([data]
   (print-table (sort (keys (first data))) data)))


(def datetime-epoch-types
  #{:epoch-milliseconds
    :epoch-seconds})


(defn dataset->readers
  ^List [dataset {:keys [all-printable-columns?]
                  :as _options}]
  (->> (ds-proto/columns dataset)
       (mapv (fn [coldata]
               (let [col-reader (dtype/->reader coldata)]
                 (if (or all-printable-columns?
                         (dtype-dt/packed-datatype? (dtype/get-datatype col-reader)))
                   (dtype-pp/reader-converter col-reader)
                   col-reader))))))


(defn value-reader
  "Return a reader that produces a vector of column values per index."
  (^ObjectReader [dataset options]
   (let [readers (dataset->readers dataset options)
         n-rows (long (second (dtype/shape dataset)))
         n-cols (long (first (dtype/shape dataset)))]
     (reify ObjectReader
       (lsize [rdr] n-rows)
       (read [rdr idx]
         (reify ObjectReader
           (lsize [inner-rdr] n-cols)
           (read [inner-rd inner-idx]
             ;;confusing because there is an implied transpose
             (.get ^List (.get readers inner-idx)
                   idx)))))))
  (^ObjectReader [dataset]
   (value-reader dataset {})))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value"
  ([dataset options]
   (let [colnamemap (->> (ds-proto/column-names dataset)
                         (map-indexed #(vector %2 %1))
                         (into {}))
         readers (value-reader dataset options)]
     (reify ObjectReader
       (lsize [rdr] (.lsize readers))
       (read [rdr idx]
         (FastStruct. colnamemap (.read readers idx))))))
  ([dataset]
   (mapseq-reader dataset {})))



(defn print-dataset
  [dataset & {:keys [column-names index-range]
              :or {column-names :all}}]
  (let [index-range (or index-range
                        (range
                         (min (second (dtype/shape dataset))
                              *default-table-row-print-length*)))
        print-ds (ds-proto/select dataset column-names index-range)
        column-names (ds-proto/column-names print-ds)]
    (with-out-str
      (print-table column-names (mapseq-reader print-ds
                                               {:all-printable-columns? true})))))
