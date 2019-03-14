(ns tech.ml.dataset.window
  "Windowing functions to create new columns from windowed views into other tables.
  A window into a table is a sequence of sequences index,weight pairs.  Please
  ensure new window functions match exactly the scipy versions.
  [[[0 1.0][1 1.0][2 0.5]]
   [[3 1.0][4 1.0][5 0.5]]]"
  (:require [tech.compute.tensor.functional :as ct-fun]
            [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype :as dtype]
            [tech.compute.tensor :as ct]
            [tech.parallel :as parallel])
  (:import [org.apache.commons.math3.distribution NormalDistribution]))


(defn normalize
  [double-data]
  (let [mag (ct-fun/magnitude-reduce double-data)]
    (ct-fun// double-data mag)))


(defn ones
  [window-size]
  (double-array (repeat window-size 1.0)))


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

(defn- create-index-array
  ^ints [^long item-idx ^long window-size ^long last-idx]
  (let [indexes (int-array window-size)
        half-elems (quot window-size 2)]
    (c-for [idx (int 0) (< idx window-size) (inc idx)]
           (aset indexes idx (min (max 0 (- (+ item-idx idx) half-elems))
                                  last-idx)))
    indexes))


(defn fixed-window-indexes
  "For fixed windows, produce a repeating array of indexes
  and normalized window coefficients.  Repeats the borders."
  [rowcount window-data]
  (let [rowcount (long rowcount)
        n-elems (count window-data)
        half-elems (quot n-elems 2)
        last-idx (max 0 (- rowcount 1))
        window-data (if (every? #(= (first window-data) %) (rest window-data))
                      (first window-data)
                      window-data)]
    (->> (range rowcount)
         (map (fn [item-idx]
                [(create-index-array item-idx n-elems last-idx)
                 window-data])))))


(defn outer-select
  [tens-value indexes]
  (let [n-dims (count (ct/shape tens-value))]
    (apply ct/select tens-value (repeat (- n-dims 1) :all))))


(defn inner-select
  [tens-value indexes]
  (let [n-dims (count (ct/shape tens-value))]
    (apply ct/select tens-value (concat (repeat (- n-dims 1) :all)
                                        [indexes]))))


(defn generalized-rolling-window
  "Map a function across a rolling window of y tensors"
  [window-index-seq window-fn src-tensor-seq]
  (let [first-tens (first src-tensor-seq)
        result-dtype (dtype/get-datatype first-tens)
        src-tensor-seq (map ct/ensure-tensor src-tensor-seq)]
    (->> window-index-seq
         (map (fn [[idx-ary window-ary]]
                (if-not (= 0 (count idx-ary))
                  (let [src-data (map (comp #(ct-fun/* window-ary %)
                                            #(inner-select % idx-ary))
                                      src-tensor-seq)]
                    (apply window-fn src-data))
                  Double/NaN)))
         (#(dtype/make-array-of-type result-dtype % {:unchecked? true})))))


(defn specific-rolling-window
  [src-tensor window-size window-fn-keywd]
  (let [dest (ct/clone src-tensor)
        dest-tensor (ct/ensure-tensor dest)
        src-tensor (ct/ensure-tensor src-tensor)
        n-elems (dtype/ecount src-tensor)
        window-size (long window-size)
        last-idx (- n-elems 1)]
    ;;This type of reduction really should be supported deep in the cpu driver layer for
    ;;tensors--it should be a new tensor-level operation which would increase performance
    ;;to about maximum but for now this will get you maybe 10% of the way there.
    (parallel/parallel-for
     idx n-elems
     (let [indexes (create-index-array idx window-size last-idx)]
       (ct/unary-reduce! (ct/select dest-tensor [idx]) 1.0
                         (ct/select src-tensor indexes)
                         window-fn-keywd)))
    dest))
