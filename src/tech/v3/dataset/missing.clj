(ns ^:no-doc tech.v3.dataset.missing
  "Functions for dealing with missing data.  Taken (with permission) nearly verbatim
  from https://github.com/scicloj/tablecloth"
  (:require [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.column :as col]
            [tech.v3.datatype.update-reader :as update-rdr]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.tools.logging :as log])
  (:import [org.roaringbitmap RoaringBitmap]
           [clojure.lang IFn]))


(defn- iterable-sequence?
  "Check if object is sequential, is column or maybe a reader (iterable)?"
  [xs]
  (or (sequential? xs)
      (and (not (map? xs))
           (instance? Iterable xs))))


(defn select-missing
  "Select only rows with missing values"
  [ds]
  (ds-base/select-rows ds (ds-base/missing ds)))


(defn- remove-from-rbitmap
  ^RoaringBitmap [^RoaringBitmap rb ks]
  (let [rb (.clone rb)]
    (reduce (fn [^RoaringBitmap rb ^long k]
              (.remove rb k)
              rb) rb ks)))

(defn- replace-missing-with-value
  [col missing value]
  (col/new-column (col/column-name col)
                  (update-rdr/update-reader
                   col (cond
                         (map? value) value
                         (iterable-sequence? value) (zipmap missing (cycle value))
                         :else (bitmap/bitmap-value->bitmap-map
                                missing
                                (casting/cast value
                                              (dtype/elemwise-datatype col)))))
                  {} (if (map? value)
                       (remove-from-rbitmap missing (keys value))
                       (RoaringBitmap.))))

(defn- missing-direction-prev
  ^long [^RoaringBitmap rb ^long idx]
  (.previousAbsentValue rb idx))

(defn- missing-direction-next
  ^long [^RoaringBitmap rb ^long idx]
  (.nextAbsentValue rb idx))

(defn- replace-missing-with-direction
  [[primary-f secondary-f] col missing value]
  (let [cnt (dtype/ecount col)
        missing-map-pre-fn (fn [f m v]
                             (let [vv (f missing v)]
                               (if (< -1 vv cnt)
                                 (assoc m v (dtype/get-value col vv))
                                 m)))
        missing-map-fn #(reduce (partial missing-map-pre-fn %1) {} %2)
        step1 (replace-missing-with-value col missing
                                          (missing-map-fn primary-f missing))
        missing2 (col/missing step1)]
    (if (empty? missing2)
      step1
      (if (nil? value)
        (replace-missing-with-value step1 missing2
                                    (missing-map-fn secondary-f missing2))
        (replace-missing-with-value step1 missing2 value)))))

;; mid and range

(defn- find-missing-ranges
  [^RoaringBitmap missing]
  (when-not (empty? missing)
    (loop [current (.first missing)
           buff []]
      (if (neg? current)
        buff
        (let [p (.previousAbsentValue missing current)
              n (.nextAbsentValue missing current)]
          (recur (.nextValue missing n) (conj buff [p n])))))))


(defn- find-lerp-values
  [cnt col lerp-fn curr [start end]]
  (let [no-left? (neg? start)
        no-right? (>= end cnt)]
    (when-not (and no-left? no-right?) ;; all values are missing?
      (let [s (dtype/get-value col (if no-left? end start))
            e (dtype/get-value col (if no-right? start end))
            start (inc start)
            end (min cnt end)
            size (- end start)]
        (merge curr (zipmap (range start end) (lerp-fn s e size)))))))


(defn- midpoint-lerp-double
  [^double start ^double end ^long n-steps]
  (let [mid (+ start (/ (- (double end) (double start)) 2.0))]
    (dtype/const-reader mid n-steps)))

(defn- midpoint-lerp-object
  [start end ^long n-steps]
  (let [mid (+ start (/ (- end start) 2.0))]
    (dtype/const-reader mid n-steps)))

(defn- lerp-double
  [^double start ^double end ^long n-steps]
  (let [steps+ (inc n-steps)
        data-range (/ (- end start) (double steps+))]
    (dtype/make-reader :float64 n-steps
                       (+ start (* data-range (inc idx))))))

(defn- lerp-object
  [start end ^long n-steps]
  (let [steps+ (inc n-steps)
        data-range (/ (- end start) steps+)]
    (dtype/make-reader :object n-steps
                       (+ start (* data-range (inc idx))))))



(defn- replace-missing-with-lerp
  ([lerp-type col missing]
   (let [ranges (find-missing-ranges missing)
         cnt (dtype/ecount col)
         orig-col-dtype (dtype/elemwise-datatype col)
         [col-dtype col-rdr]
         (cond
           (dtype-dt/datetime-datatype? orig-col-dtype)
           [:float64 (-> (dtype-dt/datetime->milliseconds col)
                         (dtype-proto/elemwise-cast :float64))]
           (casting/numeric-type? orig-col-dtype)
           [:float64 (dtype-proto/elemwise-cast col :float64)]
           :else
           [:object (dtype-proto/->reader col)])
         lerp-fn (condp = [col-dtype lerp-type]
                   [:float64 :mid] midpoint-lerp-double
                   [:object :mid] midpoint-lerp-object
                   [:float64 :lerp] lerp-double
                   [:object :lerp] lerp-object)
         missing-replace (reduce (partial find-lerp-values cnt col-rdr lerp-fn)
                                 {} ranges)
         temp-result-col (replace-missing-with-value col-rdr missing missing-replace)]
     (println (vec (map long temp-result-col)))
     (if (dtype-dt/datetime-datatype? orig-col-dtype)
       (col/new-column (:name (meta col))
                       (dtype-dt/milliseconds->datetime orig-col-dtype
                                                        temp-result-col))
       (vary-meta temp-result-col assoc :name (:name (meta col)))))))


(defn replace-missing-with-strategy
  [col missing strategy value]
  (let [value (if (fn? value)
                (value col)
                value)
        col-dtype (dtype/elemwise-datatype col)
        ;;non-numeric columns default to down from lerp or mid
        strategy (if (and (#{:lerp :mid} strategy)
                          (not (or (casting/numeric-type? col-dtype)
                                   (dtype-dt/datetime-datatype? col-dtype))))
                   :down
                   strategy)]
    (condp = strategy
      :down (replace-missing-with-direction
             [missing-direction-prev missing-direction-next] col missing value)
      :up (replace-missing-with-direction
           [missing-direction-next missing-direction-prev] col missing value)
      :lerp (replace-missing-with-lerp :lerp col missing)
      :mid (replace-missing-with-lerp :mid col missing)
      (replace-missing-with-value col missing value))))



(defn replace-missing
  "Replace missing values in some columns with a given strategy.
  The columns selector may be:

  - seq of any legal column names
  - or a column filter function, such as `numeric` and `categorical`

  Strategies may be:

  - `:down` - take value from previous non-missing row if possible else use next
    non-missing row.
  - `:up` - take value from next non-missing row if possible else use previous
     non-missing row.
  - `:mid` - Use midpoint of averaged values between previous and next nonmissing
     rows.
  - `:lerp` - Linearly interpolate values between previous and next nonmissing rows.
  - `:value` - Value will be provided - see below.

      value may be provided which will then be used.  Value may be a function in which
      case it will be called on the column with missing values elided and the return will
      be used to as the filler."
  ([ds] (replace-missing ds :mid))
  ([ds strategy] (replace-missing ds :all strategy))
  ([ds columns-selector strategy]
   (replace-missing ds columns-selector strategy nil))
  ([ds columns-selector strategy value]
   (let [strategy (or strategy :mid)
         row-cnt (ds-base/row-count ds)
         selected (if (fn? columns-selector)
                    (columns-selector ds)
                    (ds-base/select-columns ds columns-selector))]
     (->> selected
          (ds-base/columns)
          (reduce (fn [ds col]
                    (let [^RoaringBitmap missing (col/missing col)]
                      (cond
                        (.isEmpty missing)
                        ds
                        (== row-cnt (.getCardinality missing))
                        (if (and (= :value strategy)
                                 (not (instance? IFn value)))
                          (ds-base/add-or-update-column
                           ds (replace-missing-with-strategy col missing strategy value))
                          (do
                            (log/warnf "Column has no values and strategy (%s) is dependent upon existing values"
                                       strategy)
                            ds))
                        :else
                        (ds-base/add-or-update-column
                         ds (replace-missing-with-strategy col missing strategy value)))))
                  ds)))))
