(ns ^:no-doc tech.v3.dataset.missing
  "Functions for dealing with missing data.  Taken (with permission) nearly verbatim
  from https://github.com/scicloj/tablecloth"
  (:require [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.column :as col]
            [tech.v3.datatype.update-reader :as update-rdr]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.tools.logging :as log])
  (:import [org.roaringbitmap RoaringBitmap]
           [clojure.lang IFn]
           [tech.v3.datatype ObjectReader]
           [tech.v3.dataset.impl.column Column]))


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

(defn- maybe-column-buffer
  [col]
  (if (instance? Column col)
    (.buffer ^Column col)
    col))

(defn- replace-missing-with-value
  [col missing value]
  (let [unpack-data (packing/unpack (maybe-column-buffer col))]
    (col/new-column (col/column-name col)
                    (update-rdr/update-reader
                     unpack-data
                     (cond
                       (map? value) value
                       (iterable-sequence? value) (zipmap missing (cycle value))
                       :else (bitmap/bitmap-value->bitmap-map
                              missing
                              (let [col-dt (dtype/elemwise-datatype unpack-data)]
                                (casting/cast value col-dt)))))
                    (meta col)
                    (if (map? value)
                      (remove-from-rbitmap missing (keys value))
                      (RoaringBitmap.)))))


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
        col (replace-missing-with-value col missing (missing-map-fn primary-f missing))
        missing (col/missing col)]
    (cond
      (empty? missing)
      col
      (not (nil? value))
      (replace-missing-with-value col missing value)
      secondary-f
      (replace-missing-with-value col missing (missing-map-fn secondary-f missing))
      :else
      col)))


(defn- replace-missing-with-abb
  [col ^RoaringBitmap missing]
  (let [^RoaringBitmap non-missing (doto (RoaringBitmap.)
                                     (.add 0 (dtype/ecount col))
                                     (.andNot missing)) ;; prepare non-missing indices
        non-missing-cnt (.getCardinality non-missing) ;; how many non-missing we have
        ;; bootstrap `non-missing-count` values from a column
        samples1 (col/select col (repeatedly non-missing-cnt #(.select non-missing (rand-int non-missing-cnt))))
        ;; bootstrap `missing-count` values for imputation from first bootstrap round
        samples2 (col/select samples1 (repeatedly (.getCardinality missing) #(rand-int (dtype/ecount samples1))))]
    (replace-missing-with-value col missing samples2)))

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
                   [:float64 :midpoint] midpoint-lerp-double
                   [:object :midpoint] midpoint-lerp-object
                   [:float64 :lerp] lerp-double
                   [:object :lerp] lerp-object)
         missing-replace (reduce (partial find-lerp-values cnt col-rdr lerp-fn)
                                 {} ranges)
         temp-result-col (replace-missing-with-value col-rdr missing missing-replace)]
     (if (dtype-dt/datetime-datatype? orig-col-dtype)
       (col/new-column (:name (meta col))
                       (dtype-dt/milliseconds->datetime orig-col-dtype
                                                        temp-result-col))
       (vary-meta temp-result-col assoc :name (:name (meta col)))))))


(defn- replace-missing-with-nearest
  [col missing]
  (let [ranges (find-missing-ranges missing)
        cnt (dtype/ecount col)
        col-dt (dtype/elemwise-datatype col)
        lerp-fn (fn [start end ^long n-steps]
                  (let [hs (quot (inc n-steps) 2)]
                    (reify ObjectReader
                      (elemwiseDatatype [rdr] col-dt)
                      (lsize [rdr] n-steps)
                      (readObject [rdr idx]
                        (if (< idx hs) start end)))))
        missing-replace (reduce (partial find-lerp-values cnt col lerp-fn)
                                {} ranges)]
    (replace-missing-with-value col missing missing-replace)))


(defn replace-missing-with-strategy
  [col missing strategy value]
  (let [value (if (fn? value)
                (value col)
                value)
        col-dtype (dtype/elemwise-datatype col)
        ;;non-numeric columns default to down from lerp or mid
        strategy (cond
                   (and (#{:lerp :midpoint} strategy)
                        (not (or (casting/numeric-type? col-dtype)
                                 (dtype-dt/datetime-datatype? col-dtype))))
                   :down
                   (= strategy :mid)
                   :nearest
                   :else
                   strategy)]
    (condp = strategy
      :down (replace-missing-with-direction
             [missing-direction-prev nil] col missing value)
      :up (replace-missing-with-direction
           [missing-direction-next nil] col missing value)
      :downup (replace-missing-with-direction
               [missing-direction-prev missing-direction-next] col missing value)
      :updown (replace-missing-with-direction
               [missing-direction-next missing-direction-prev] col missing value)
      :abb (replace-missing-with-abb col missing)
      :lerp (replace-missing-with-lerp :lerp col missing)
      :nearest (replace-missing-with-nearest col missing)
      :midpoint (replace-missing-with-lerp :midpoint col missing)
      (replace-missing-with-value col missing value))))



(defn replace-missing
  "Replace missing values in some columns with a given strategy.
  The columns selector may be:

  - seq of any legal column names
  - or a column filter function, such as `numeric` and `categorical`

  Strategies may be:

  - `:down` - take value from previous non-missing row if possible else use provided value.
  - `:up` - take value from next non-missing row if possible else use provided value.
  - `:downup` - take value from previous if possible else use next.
  - `:updown` - take value from next if possible else use previous.
  - `:nearest` - Use nearest of next or previous values.  `:mid` is an alias for `:nearest`.
  - `:midpoint` - Use midpoint of averaged values between previous and next nonmissing
     rows.
  - `:abb` - Impute missing with approximate bayesian bootstrap.  See [r's ABB](https://search.r-project.org/CRAN/refmans/LaplacesDemon/html/ABB.html).
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
