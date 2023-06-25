(ns tech.v3.dataset.math
  "Various mathematic transformations of datasets such as (inefficiently)
  building simple tables, pca, and normalizing columns to have mean of 0 and variance of 1.
  More in-depth transformations are found at `tech.v3.dataset.neanderthal`."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.statistics :as statistics]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.utils :as ds-utils]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.missing :as ds-missing]
            [ham-fisted.set :as set]
            [ham-fisted.api :as hamf]
            [ham-fisted.function :as hamf-fn]
            [clj-commons.primitive-math :as pmath]
            [clojure.tools.logging :as log]
            [clojure.set :as c-set])
  (:import [org.apache.commons.math3.analysis.interpolation LoessInterpolator]
           [tech.v3.datatype DoubleReader]
           [org.roaringbitmap RoaringBitmap]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defn correlation-table
  "Return a map of colname->list of sorted tuple of [colname, coefficient].
  Sort is:
  (sort-by (comp #(Math/abs (double %)) second) >)

  Thus the first entry is:
  [colname, 1.0]

  There are three possible correlation types:
  :pearson
  :spearman
  :kendall

  :pearson is the default."
  [dataset & {:keys [correlation-type
                     colname-seq]}]
  (let [missing-columns (ds-base/columns-with-missing-seq dataset)
        _ (when missing-columns
            (println "WARNING - excluding columns with missing values:\n"
                     (mapv :column-name  missing-columns)))
        non-numeric (->> (ds-base/columns dataset)
                         (map meta)
                         (remove #(casting/numeric-type?
                                   (:datatype %)))
                         (map :name)
                         seq)
        _ (when non-numeric
            (println "WARNING - excluding non-numeric columns:\n"
                     (vec non-numeric)))
        _ (when-let [selected-non-numeric
                     (seq (c-set/intersection (set colname-seq)
                                              (set non-numeric)))]
            (throw (ex-info (format "Selected columns are non-numeric: %s"
                                    selected-non-numeric)
                            {:selected-columns colname-seq
                             :non-numeric-columns non-numeric})))
        dataset (ds-base/select-columns dataset
                                        (->> (ds-base/columns dataset)
                                             (map ds-col/column-name)
                                             (remove (set (concat
                                                           (map :column-name  missing-columns)
                                                           non-numeric)))))
        lhs-colseq (->> (if (seq colname-seq)
                          (map (partial ds-base/column dataset) colname-seq)
                          (ds-base/columns dataset))
                        (map (fn [col] [(ds-col/column-name col)
                                        (hamf/double-array (dtype/->reader col :float64))])))
        rhs-colseq (->> (ds-base/columns dataset)
                        (map (fn [col] [(ds-col/column-name col)
                                        (hamf/double-array (dtype/->reader col :float64))])))
        correlation-type (or correlation-type :pearson)
        corr-fn (case correlation-type
                  :pearson #(statistics/pearsons-correlation %1 %2 {:nan-strategy :keep})
                  :spearman #(statistics/spearmans-correlation %1 %2 {:nan-strategy :keep})
                  :kendall #(statistics/kendalls-correlation %1 %2 {:nan-strategy :keep}))]
    (->> (for [[lname ldata] lhs-colseq]
           [lname
            (->> rhs-colseq
                 (map (fn [[rname rdata]]
                        (let [corr (corr-fn ldata rdata)]
                          (if (Double/isFinite corr)
                            [rname corr]
                            (do
                              (log/warnf "Correlation failed: %s-%s" lname rname)
                              nil)))))
                 (remove nil?)
                 ;;Using a type-hinted key-fn allows the sort operator to use a double array
                 ;;which has a very fast indirect sort operation.
                 (hamf/sort-by (hamf-fn/obj->double
                                v (-> (second v)
                                      (double)
                                      (Math/abs))) >))])
         (into {}))))


(defn interpolate-loess
  "Interpolate using the LOESS regression engine.  Useful for smoothing out graphs."
  ([ds x-colname y-colname
    {:keys [bandwidth iterations accuracy result-name]
     ;;Using R defaults, as close as we can get.
     :or {bandwidth 0.75
          iterations 4
          accuracy LoessInterpolator/DEFAULT_ACCURACY}}]
   (let [interp (LoessInterpolator. (double bandwidth)
                                    (int iterations)
                                    (double accuracy))
         x-col (ds x-colname)
         x-orig-datatype (dtype/elemwise-datatype x-col)
         x-datetime? (dtype-dt/datetime-datatype? x-orig-datatype)
         x-col (if x-datetime?
                 (dtype-dt/datetime->milliseconds x-col)
                 x-col)
         y-col (ds y-colname)
         spline (.interpolate interp
                              (dtype/->double-array x-col)
                              (dtype/->double-array y-col))
         new-col-name (or result-name
                          (keyword (str (ds-utils/column-safe-name y-colname) "-loess")))
         n-elems (ds-base/row-count ds)
         x-rdr (dtype/->buffer x-col)]
     (-> (ds-base/add-or-update-column ds new-col-name
                                    (reify DoubleReader
                                      (lsize [rdr] n-elems)
                                      (readDouble [rdr idx]
                                        (.value spline (.readDouble x-rdr idx)))))
         (ds-base/update-column new-col-name
                             #(with-meta % (assoc (meta %)
                                                  :interpolator spline))))))
  ([ds x-colname y-colname]
   (interpolate-loess ds x-colname y-colname {})))


(defn- scatter-missing
  [index-rdr ^RoaringBitmap missing]
  (let [retval (RoaringBitmap.)
        iter (.getIntIterator missing)]
    (loop [continue? (.hasNext iter)]
      (when continue?
        (.add retval (int (index-rdr (.next iter))))
        (recur (.hasNext iter))))
    retval))


(defn fill-range-replace
  "Given an in-order column of a numeric or datetime type, fill in spans that are
  larger than the given max-span.  The source column must not have missing values.
  For more documentation on fill-range, see tech.v3.datatype.function.fill-range.

  If the column is a datetime type the operation happens in millisecond space and
  max-span may be a datetime type convertible to milliseconds.

  The result column has the same datatype as the input column.

  After the operation, if missing strategy is not nil the newly produced missing
  values along with the existing missing values will be replaced using the given
  missing strategy for all other columns.  See
  `tech.v3.dataset.missing/replace-missing` for documentation on missing strategies.
  The missing strategy defaults to :down unless explicity set.

  Returns a new dataset."
  ([ds colname max-span]
   (fill-range-replace ds colname max-span :down nil))
    ([ds colname max-span missing-strategy]
   (fill-range-replace ds colname max-span missing-strategy nil))
  ([ds colname max-span missing-strategy missing-value]
   (let [target-col (ds colname)
         _ (when-not (== 0 (dtype/ecount (ds-col/missing target-col)))
             (throw (Exception. "Fill-range column must not have missing values")))
         target-dtype (dtype/get-datatype target-col)
         target-col (if (dtype-dt/datetime-datatype? target-dtype)
                      (dtype-dt/datetime->milliseconds target-col)
                      target-col)
         max-span (if (dtype-dt/datetime-datatype? max-span)
                    (dtype-dt/datetime->milliseconds max-span)
                    max-span)
         {:keys [result missing]} (dfn/fill-range target-col max-span)
         ;;This is the set of values that have not changed.  Iterating through it and
         ;;and a source vector in parallel allow us to scatter the original data into
         ;;a new storage container.
         original-indexes (-> (doto  (bitmap/->bitmap
                                      (range (dtype/ecount result)))
                                (.andNot ^RoaringBitmap missing))
                              (bitmap/->random-access))
         result (if (dtype-dt/datetime-datatype? target-dtype)
                  (dtype-dt/milliseconds->datetime target-dtype result)
                  result)]

     (->> (ds-base/columns ds)
          (pfor/pmap
           (fn [col]
             (if (= colname (ds-col/column-name col))
               (ds-col/new-column colname result)
               (let [new-colname (ds-col/column-name col)
                     new-data (dtype/make-container
                               :jvm-heap
                               (dtype/get-datatype col)
                               (dtype/ecount result))
                     ^RoaringBitmap col-missing (ds-col/missing col)
                     any-missing? (not (.isEmpty col-missing))
                     updated-missing (if any-missing?
                                       (scatter-missing original-indexes col-missing)
                                       (bitmap/->bitmap))
                     total-missing (set/union updated-missing missing)
                     ;;Scatter original data into new locations
                     _ (dtype/copy! col
                                    (dtype/indexed-buffer original-indexes new-data))
                     ;;Use col-impl as it skips the scanning of the data and we know
                     ;;that it is dense
                     new-col (ds-col/new-column new-colname new-data
                                                (meta col)
                                                total-missing)]
                 (if-not (nil? missing-strategy)
                   (ds-missing/replace-missing-with-strategy
                    new-col total-missing missing-strategy missing-value)
                   new-col)))))
          (ds-impl/new-dataset (meta ds))))))


(defrecord StdScaleTransform [])


(defn fit-std-scale
  "Calculate nan-aware means, stddev - per-column - of a dataset.

  Options are passed through to
  tech.v3.datatype.statistics/descriptive-statistics."
  ([dataset {:keys [mean? stddev?]
             :or {mean? true stddev? true}
             :as options}]
   (let [stats-data (cond-> []
                      mean? (conj :mean)
                      stddev? (conj :standard-deviation))]
     (errors/when-not-error
      (seq stats-data)
      "Either mean? or stddev? must be true")
     (map->StdScaleTransform
      (->> (vals dataset)
           (map (fn [col]
                  [(:name (meta col))
                   (statistics/descriptive-statistics col stats-data options)]))
           (into {})))))
  ([dataset]
   (fit-std-scale dataset nil)))


(defn transform-std-scale
  "Given a dataset and a standard scale transform return a new dataset
  with the columns "
  [dataset std-scale-xform]
  (reduce (fn [dataset [colname {:keys [mean standard-deviation]}]]
            (let [mean (double (or mean 0.0))
                  standard-deviation (double  (or standard-deviation 1.0))]
              (ds-base/update-column
               dataset colname
               (fn [coldata]
                 (ds-col/column-map
                  (fn [val]
                    (pmath// (pmath/- (double val) mean)
                             standard-deviation))
                  :float64
                  coldata)))))
          dataset
          std-scale-xform))


(extend-type StdScaleTransform
  ds-proto/PDatasetTransform
  (transform [t dataset]
    (transform-std-scale dataset t)))


(defrecord MinMaxTransform [min max column-data])


(defn fit-minmax
  "nan-aware min-max fit of the dataset.  Returns an object that can be used
  in transform-minmax.  target Min-max default to -0.5,0.5"
  ([dataset {:keys [min max]
              :or {min -0.5
                   max 0.5}
             :as options}]
   (let [min (double min)
         max (double max)]
     (map->MinMaxTransform
      {:min min :max max
       :column-data
       (->> (vals dataset)
            (map (fn [col]
                   [(:name (meta col))
                    (statistics/descriptive-statistics col [:min :max] options)]))
            (into {}))})))
  ([dataset]
   (fit-minmax dataset nil)))


(defn transform-minmax
  "Scale columns listed in the min-max transform to the mins and maxes dictated
  by that transform."
  [dataset {:keys [min max column-data]}]
  (let [min (double min)
        max (double max)
        target-range (- max min)]
    (reduce (fn [dataset [colname coldata]]
              (let [{column-min :min
                     column-max :max} coldata
                    column-min (double column-min)
                    column-max (double column-max)
                    column-range (- column-max column-min)]
                (ds-base/update-column
                 dataset colname
                 (fn [coldata]
                   (ds-col/column-map
                    (fn [val]
                      (-> (double val)
                          (pmath/- column-min)
                          (pmath// column-range)
                          (pmath/* target-range)
                          (pmath/+ min)))
                    :float64
                    coldata)))))
            dataset
            column-data)))


(extend-type MinMaxTransform
  ds-proto/PDatasetTransform
  (transform [t dataset]
    (transform-minmax dataset t)))
