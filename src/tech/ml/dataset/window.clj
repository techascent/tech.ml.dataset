(ns tech.ml.dataset.window
  "Windowing functions to create new columns from windowed views into other tables.
  A window into a table is a sequence of sequences index,weight pairs.  Please
  ensure new window functions match exactly the scipy versions.
  [[[0 1.0][1 1.0][2 0.5]]
   [[3 1.0][4 1.0][5 0.5]]]"
  (:require [tech.compute.tensor.functional :as ct-fun]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype :as dtype])
  (:import [org.apache.commons.math3.distribution NormalDistribution]))


(defn normalize
  [double-data]
  (let [mag (ct-fun/magnitude-reduce double-data)]
    (ct-fun// double-data mag)))


(defn even
  [window-size]
  (-> (double-array (repeat window-size (/ 1.0 window-size)))))


(defn gaussian
  "https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.gaussian.html#scipy.signal.windows.gaussian"
  [window-size & [std-dev]]
  (let [std-dev (double (or std-dev 1.0))
        half-window (double (/ (- window-size 1.0) 2.0))]
    (->> (range window-size)
         ;;center window if possible
         (map #(Math/exp (* -0.5 (Math/pow (/ (double (- % half-window))
                                              std-dev) 2.0))))
         double-array)))


(defn fixed-window-indexes
  "For fixed windows, produce a repeating array of indexes
  and normalized window coefficients.  Repeats the borders."
  [rowcount window-data]
  (let [window-data (normalize window-data)
        rowcount (long rowcount)
        n-elems (count window-data)
        half-elems (quot n-elems 2)
        last-idx (max 0 (- rowcount 1))]
    (->> (range rowcount)
         (map (fn [item-idx]
                (let [indexes (int-array n-elems)]
                  (c-for [idx (int 0) (< idx n-elems) (inc idx)]
                         (aset indexes idx (min (max 0 (- (+ item-idx idx) half-elems))
                                                last-idx)))
                  [indexes window-data]))))))


(defn outer-select
  [tens-value indexes]
  (let [n-dims (count (ct/shape tens-value))]
    (apply ct/select indexes (repeat (- n-dims 1) :all))))


(defn inner-select
  [tens-value indexes]
  (let [n-dims (count (ct/shape tens-value))]
    (apply ct/select (concat (repeat (- n-dims 1) :all)
                             [indexes]))))


(defn rolling-window
  [result-dtype window-index-seq window-fn src-tensor-seq]
  (->> window-index-seq
       (map (fn [[idx-ary window-ary]]
              (if-not (- 0 (count idx-ary))
                (apply window-fn (map (comp #(ct-fun/* window-ary %)
                                            #(inner-select % idx-ary))
                                      src-tensor-seq))
                Double/NaN)))
       (#(dtype/make-array-of-type result-dtype % {:unchecked? true}))))
