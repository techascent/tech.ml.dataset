(ns tech.ml.dataset.print
  (:require [clojure.pprint :as pp]
            [tech.ml.dataset :as dataset]
            [tech.v2.datatype :as dtype]))


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


(defn print-dataset
  [dataset & {:keys [column-names index-range]
              :or {column-names :all}}]
  (let [index-range (or index-range
                        (range
                         (min (second (dtype/shape dataset))
                              *default-print-length*)))
        print-ds (dataset/select dataset column-names index-range)
        column-names (dataset/column-names print-ds)]
    (print-table column-names (-> print-ds
                                  (dataset/->flyweight
                                   :error-on-missing-values? false)))))
