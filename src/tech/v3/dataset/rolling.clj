(ns tech.v3.dataset.rolling
  "Implement a generalized rolling window including support for time-based variable
  width windows."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.rolling :as dt-rolling]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.io.column-parsers :as col-parsers])
  (:import [java.util.function BiFunction]
           [clojure.lang IDeref]
           [tech.v3.datatype DoubleConsumers$Sum])
  (:refer-clojure :exclude [min max]))


(defn mean
  [column-name]
  {:column-name column-name
   :reducer stats/mean})


(defn min
  [column-name]
  {:column-name column-name
   :datatype dtype/elemwise-datatype
   :reducer stats/min})


(defn max
  [column-name]
  {:column-name column-name
   :datatype dtype/elemwise-datatype
   :reducer stats/max})


(defn variance
  [column-name]
  {:column-name column-name
   :reducer stats/variance})


(defn standard-deviation
  [column-name]
  {:column-name column-name
   :reducer stats/standard-deviation})


(defn rolling
  "{:window :relative-position}"
  ([ds window reducer-map options]
   (let [n-rows (ds-base/row-count ds)
         reducers
         (->> reducer-map
              (map-indexed
               (fn [idx [k red]]
                 ;;windowed data consumer
                 (let [parser (col-parsers/promotional-object-parser
                               (:column-name red) nil)
                       red-fn (:reducer red)
                       cname (:column-name red)]
                   {:column-name cname
                    :window-fn (fn [idx rdr]
                                 (col-parsers/add-value! parser idx (red-fn rdr)))
                    :finalize-fn #(-> (col-parsers/finalize! parser n-rows)
                                      (assoc :tech.v3.dataset/name cname))})))
              (group-by :column-name)
              (mapv (fn [[colname reducers]]
                      {:column (ds colname)
                       :reducers (vec reducers)})))
         window-data (if (integer? window)
                       {:window-size window
                        :relative-position :center}
                       window)
         n-rows (ds-base/row-count ds)]
     (if (:window-size window-data)
       (let [window-size (long (:window-size window-data))
             n-pad (long (case (:relative-position window-data :center)
                           :center (quot (long window-size) 2)
                           :left (dec window-size)
                           :right 0))]
         (->> reducers
              (mapv
               (fn [{:keys [column reducers]}]
                 (let [buf (dtype/->buffer column)]
                   (dotimes [idx n-rows]
                     (let [rdr (dt-rolling/windowed-data-reader
                                window-size (- idx n-pad) buf)]
                       (dotimes [red-idx (count reducers)]
                         (let [reducer (reducers red-idx)
                               reducer-fn (get reducer :window-fn)]
                           (reducer-fn idx rdr))))))
                 (mapv (fn [reducer]
                         ((:finalize-fn reducer)))
                       reducers)))
              (apply concat)
              (reduce #(ds-base/add-column %1 %2)
                      ds))))))
  ([ds window reducer-map]
   (rolling ds window reducer-map nil)))


(comment
  (require '[tech.v3.dataset :as ds])
  (def test-ds (ds/->dataset {:a (map #(Math/sin (double %))
                                      (range 0 200 0.1))}))
  )
