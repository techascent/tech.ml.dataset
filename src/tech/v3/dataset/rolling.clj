(ns tech.v3.dataset.rolling
  "Implement a generalized rolling window including support for time-based variable
  width windows."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.rolling :as dt-rolling]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.dataset.base :as ds-base])
  (:import [tech.v3.datatype Buffer])
  (:refer-clojure :exclude [min max nth first last]))


(defn mean
  [column-name]
  {:column-name column-name
   :reducer stats/mean
   :datatype :float64})


(defn sum
  [column-name]
  {:column-name column-name
   :reducer stats/sum
   :datatype :float64})


(defn min
  [column-name]
  {:column-name column-name
   :reducer stats/min})


(defn max
  [column-name]
  {:column-name column-name
   :reducer stats/max})


(defn variance
  [column-name]
  {:column-name column-name
   :reducer stats/variance
   :datatype :float64})


(defn standard-deviation
  [column-name]
  {:column-name column-name
   :reducer stats/standard-deviation
   :datatype :float64})


(defn nth
  "Get the nth window value"
  [column-name nth-val]
  {:column-name column-name
   :reducer (fn [rdr] (rdr nth-val))})


(defn first
  [column-name]
  {:column-name column-name
   :reducer (fn [rdr] (rdr 0))})


(defn last
  [column-name]
  {:column-name column-name
   :reducer (fn [rdr] (rdr -1))})


(defn ^:no-doc apply-window-ranges
  [ds windows reducer-map edge-mode]
  (->> reducer-map
       (map-indexed
        (fn [_idx [k red]]
          (assoc red :dest-column-name k)))
       (group-by :column-name)
       (mapv (fn [[colname reducers]]
               (let [colname (if (= :scalar (argtypes/arg-type colname))
                               [colname]
                               (vec colname))]
                 {:columns (mapv (partial ds-base/column ds) colname)
                  :reducers (vec reducers)})))
       (mapv
        (fn [{:keys [columns reducers]}]
          ;;common case is 1 column
          (if (== 1 (count columns))
            (let [win-data (dt-rolling/window-ranges->window-reader
                            (columns 0) windows edge-mode)]
              (mapv (fn [reducer]
                      {:tech.v3.dataset/name (:dest-column-name reducer)
                       :tech.v3.dataset/data
                       (-> (dtype/emap (:reducer reducer) (:datatype reducer :object)
                                       win-data)
                           (dtype/clone))})
                    reducers))
            (let [win-data (mapv #(dt-rolling/window-ranges->window-reader
                                   % windows edge-mode)
                                 columns)]
              (mapv (fn [reducer]
                      {:tech.v3.dataset/name (:dest-column-name reducer)
                       :tech.v3.dataset/data
                       (-> (apply dtype/emap (:reducer reducer) (:datatype reducer :object)
                                  win-data)
                           (dtype/clone))})
                    reducers)))))
       (apply concat)
       (reduce #(ds-base/add-column %1 %2) ds)))


(defn rolling
  "Perform a rolling window operation appending columns to the original dataset.

  * ds - src dataset.
  * window - either an integer for fixed window sizes or a map describing the window
    operation containing keys:
    - `:window-type` - either `:fixed` or `:variable`.  For variable window operations
      `:column-name` must be a monotonically increasing column.
    - `:window-size` - for fixed window operation must be a positive integer.  For
       variable window operations must be a double value which is produced via a
       comparison function.
    - `:relative-window-position` - describes where the window is
       positioned.  Operations are `:left`, `:center`, `:right` and defaults to
       `:center` for fixed and `:right` for relative window types.
    - `:edge-mode` - for fixed windows describes what values to fill in at the edges
       of the source column.  Options are `:zero` which is 0 for numeric types and `nil`
       for object types and `:clamp` which fills in the first,last values of the column
       respectively.  Defaults to `:clamp`.
    - `:comp-fn` - if provided must return a double which is the result of comparing
      the last value of the range to the first which means `clojure.core/-`
      is a reasonable default.
    - `:units` - for datetime types, describes the units of `:window-size` and will
      dictate the numeric space if `:comp-fn` is not provided.
  * reducer-map - A map of result column name to reducer map.  The reducer map is a
    map which must contain at least `{:column-name :reducer}` where reducer is an ifn
    that is passed each window.  The result column is scanned to ascertain datatype and
    missing value status.  Multi-column reducers are supported if column-name is a vector
    of column names.  In that case each column's window is passed to the reducer.  The
    reducer can also specify the final datatype if `:datatype` is a key in the map.  Beware,
    however, that this disables missing value detection for integer datatypes.


**Fixed Window Examples:**


```clojure
user> (def test-ds (ds/->dataset {:a (map #(Math/sin (double %))
                                          (range 0 200 0.1))}))
#'user/test-ds
user> (ds/head (ds-roll/rolling test-ds 10 {:mean (ds-roll/mean :a)
                                            :min (ds-roll/min :a)
                                            :max (ds-roll/max :a)}))
_unnamed [5 4]:

|         :a |      :mean | :min |       :max |
|-----------:|-----------:|-----:|-----------:|
| 0.00000000 | 0.09834413 |  0.0 | 0.38941834 |
| 0.09983342 | 0.14628668 |  0.0 | 0.47942554 |
| 0.19866933 | 0.20275093 |  0.0 | 0.56464247 |
| 0.29552021 | 0.26717270 |  0.0 | 0.64421769 |
| 0.38941834 | 0.33890831 |  0.0 | 0.71735609 |
user> (ds/head (ds-roll/rolling test-ds
                                {:window-type :fixed
                                 :window-size 10
                                 :relative-window-position :left}
                                {:mean (ds-roll/mean :a)
                                 :min (ds-roll/min :a)
                                 :max (ds-roll/max :a)}))
_unnamed [5 4]:

|         :a |      :mean | :min |       :max |
|-----------:|-----------:|-----:|-----------:|
| 0.00000000 | 0.00000000 |  0.0 | 0.00000000 |
| 0.09983342 | 0.00998334 |  0.0 | 0.09983342 |
| 0.19866933 | 0.02985027 |  0.0 | 0.19866933 |
| 0.29552021 | 0.05940230 |  0.0 | 0.29552021 |
| 0.38941834 | 0.09834413 |  0.0 | 0.38941834 |
user> (ds/head (ds-roll/rolling test-ds
                                {:window-type :fixed
                                 :window-size 10
                                 :relative-window-position :right}
                                {:mean (ds-roll/mean :a)
                                 :min (ds-roll/min :a)
                                 :max (ds-roll/max :a)}))
_unnamed [5 4]:

|         :a |      :mean |       :min |       :max |
|-----------:|-----------:|-----------:|-----------:|
| 0.00000000 | 0.41724100 | 0.00000000 | 0.78332691 |
| 0.09983342 | 0.50138810 | 0.09983342 | 0.84147098 |
| 0.19866933 | 0.58052549 | 0.19866933 | 0.89120736 |
| 0.29552021 | 0.65386247 | 0.29552021 | 0.93203909 |
 | 0.38941834 | 0.72066627 | 0.38941834 | 0.96355819 |

user> ;;Multi column reducer
user> (ds/head (ds-roll/rolling test-ds 10
                                {:c {:column-name [:a :a]
                                     :reducer (fn [a b]
                                                (Math/round
                                                 (+ (dfn/sum a) (dfn/sum b))))
                                     :datatype :int16}}))
_unnamed [5 2]:

|         :a | :c |
|-----------:|---:|
| 0.00000000 |  2 |
| 0.09983342 |  3 |
| 0.19866933 |  4 |
| 0.29552021 |  5 |
| 0.38941834 |  7 |
```

**Variable Window Examples:**


```clojure
user> (def stocks (ds/->dataset \"test/data/stocks.csv\" {:key-fn keyword}))
#'user/stocks
user> ;;variable window column must be monotonically increasing
user> (def stocks (ds/sort-by-column stocks :date))
#'user/stocks
user> (ds/head stocks)
test/data/stocks.csv [5 3]:

| :symbol |      :date | :price |
|---------|------------|-------:|
|    AAPL | 2000-01-01 |  25.94 |
|     IBM | 2000-01-01 | 100.52 |
|    MSFT | 2000-01-01 |  39.81 |
|    AMZN | 2000-01-01 |  64.56 |
|    AAPL | 2000-02-01 |  28.66 |

user> (ds/head (ds-roll/rolling stocks
                                {:window-type :variable
                                 :column-name :date
                                 :units :days
                                 :window-size 3}
                                {:price-mean-3d (ds-roll/mean :price)
                                 :price-max-3d (ds-roll/max :price)
                                 :price-min-3d (ds-roll/min :price)}))
test/data/stocks.csv [5 6]:

| :symbol |      :date | :price | :price-mean-3d | :price-max-3d | :price-min-3d |
|---------|------------|-------:|---------------:|--------------:|--------------:|
|    AAPL | 2000-01-01 |  25.94 |    57.70750000 |        100.52 |         25.94 |
|     IBM | 2000-01-01 | 100.52 |    68.29666667 |        100.52 |         39.81 |
|    MSFT | 2000-01-01 |  39.81 |    52.18500000 |         64.56 |         39.81 |
|    AMZN | 2000-01-01 |  64.56 |    64.56000000 |         64.56 |         64.56 |
|    AAPL | 2000-02-01 |  28.66 |    56.49750000 |         92.11 |         28.66 |
user> (ds/head (ds-roll/rolling stocks
                                {:window-type :variable
                                 :column-name :date
                                 :units :months
                                 :window-size 3}
                                {:price-mean-3d (ds-roll/mean :price)
                                 :price-max-3d (ds-roll/max :price)
                                 :price-min-3d (ds-roll/min :price)}))
test/data/stocks.csv [5 6]:

| :symbol |      :date | :price | :price-mean-3d | :price-max-3d | :price-min-3d |
|---------|------------|-------:|---------------:|--------------:|--------------:|
|    AAPL | 2000-01-01 |  25.94 |    58.92500000 |        106.11 |         25.94 |
|     IBM | 2000-01-01 | 100.52 |    61.92363636 |        106.11 |         28.66 |
|    MSFT | 2000-01-01 |  39.81 |    58.06400000 |        106.11 |         28.66 |
|    AMZN | 2000-01-01 |  64.56 |    60.09222222 |        106.11 |         28.66 |
|    AAPL | 2000-02-01 |  28.66 |    57.56583333 |        106.11 |         28.37 |
```"
  ([ds window reducer-map _options]
   (let [n-rows (ds-base/row-count ds)
         window-data (if (integer? window)
                       {:window-size window
                        :relative-position :center
                        :window-type :fixed}
                       window)
         windows
         (case (:window-type window-data :fixed)
           :fixed
           (dt-rolling/fixed-rolling-window-ranges
            n-rows (:window-size window-data)
            (:relative-window-position window-data :center))
           :variable
           (let [_ (when-not (:column-name window-data)
                     (throw (Exception. (format "Variable rolling windows must have :column-name in the window data"))))
                 src-col (ds-base/column ds (:column-name window-data))
                 col-dt (dtype/elemwise-datatype src-col)]
             (vec (dt-rolling/variable-rolling-window-ranges
                   src-col (:window-size window-data)
                   {:comp-fn
                    (if-let [comp-fn (:comp-fn window-data)]
                      comp-fn
                      (when (dtype-dt/datetime-datatype? (packing/unpack-datatype col-dt))
                        (dtype-dt-ops/between-op
                         (dtype/elemwise-datatype src-col)
                         (:units window-data :milliseconds)
                         true)))
                    :relative-window-position (get window-data :relative-window-position)}))))]
     (apply-window-ranges ds windows reducer-map (:edge-mode window-data :clamp))))
  ([ds window reducer-map]
   (rolling ds window reducer-map nil)))


(defn expanding
  "Run a set of reducers across a dataset with an expanding set of windows.  These
  will produce a cumsum-type operation."
  [ds reducer-map]
  (apply-window-ranges ds (dt-rolling/expanding-window-ranges
                           (ds-base/row-count ds))
                       reducer-map
                       :clamp))

(comment
  (require '[tech.v3.dataset :as ds])
  (def test-ds (ds/->dataset {:a (map #(Math/sin (double %))
                                      (range 0 200 0.1))}))
  (rolling test-ds 10 {:mean (mean :a)
                       :min (min :a)
                       :max (max :a)})
  )
