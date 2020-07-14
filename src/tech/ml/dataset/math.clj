(ns ^:no-doc tech.ml.dataset.math
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.writers.indexed :as indexed-wtr]
            [tech.ml.utils :as ml-utils]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.base
             :refer [columns-with-missing-seq
                     columns select update-column]
             :as base]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.missing :as ds-missing]
            [tech.parallel.for :as parallel-for]
            [tech.parallel.require :as parallel-req]
            [clojure.tools.logging :as log]
            [clojure.set :as c-set])
  (:import [smile.clustering KMeans GMeans XMeans PartitionClustering]
           [org.apache.commons.math3.analysis.interpolation LoessInterpolator]
           [tech.v2.datatype DoubleReader]
           [org.roaringbitmap RoaringBitmap]))


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
  (let [missing-columns (columns-with-missing-seq dataset)
        _ (when missing-columns
            (println "WARNING - excluding columns with missing values:\n"
                     (vec missing-columns)))
        non-numeric (->> (columns dataset)
                         (map ds-col/metadata)
                         (remove #(ml-utils/numeric-datatype?
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
        dataset (select dataset
                        (->> (columns dataset)
                             (map ds-col/column-name)
                             (remove (set (concat
                                           (map :column-name  missing-columns)
                                           non-numeric))))
                        :all)
        lhs-colseq (if (seq colname-seq)
                     (map (partial base/column dataset) colname-seq)
                     (columns dataset))
        rhs-colseq (columns dataset)
        correlation-type (or :pearson correlation-type)]
    (->> (for [lhs lhs-colseq]
           [(ds-col/column-name lhs)
            (->> rhs-colseq
                 (map (fn [rhs]
                        (when-not rhs
                          (throw (ex-info "Failed" {})))
                        (let [corr (ds-col/correlation lhs rhs correlation-type)]
                          (if (dfn/valid? corr)
                            [(ds-col/column-name rhs) corr]
                            (do
                              (log/warnf "Correlation failed: %s-%s"
                                         (ds-col/column-name lhs)
                                         (ds-col/column-name rhs))
                              nil)))))
                 (remove nil?)
                 (sort-by (comp #(Math/abs (double %)) second) >))])
         (into {}))))


(defn to-column-major-double-array-of-arrays
  "Convert a dataset to a row major array of arrays.
  Note that if error-on-missing is false, missing values will appear as NAN."
  ^"[[D" [dataset & [error-on-missing?]]
  (into-array (Class/forName "[D")
              (->> (columns dataset)
                   (map #(ds-col/to-double-array % error-on-missing?)))))


(defn transpose-double-array-of-arrays
  ^"[[D" [^"[[D" input-data]
  (let [[n-cols n-rows] (dtype/shape input-data)
        ^"[[D" retval (into-array (repeatedly n-rows #(double-array n-cols)))
        n-cols (int n-cols)
        n-rows (int n-rows)]
    (parallel-for/parallel-for
     row-idx
     n-rows
     (let [^doubles target-row (aget retval row-idx)]
       (parallel-for/serial-for
        col-idx n-cols
        (aset target-row col-idx (aget ^doubles (aget input-data col-idx)
                                       row-idx)))))
    retval))


(defn to-row-major-double-array-of-arrays
    "Convert a dataset to a column major array of arrays.
  Note that if error-on-missing is false, missing values will appear as NAN."
  ^"[[D" [dataset & [error-on-missing?]]
  (-> (to-column-major-double-array-of-arrays dataset error-on-missing?)
      transpose-double-array-of-arrays))


(defn k-means
  "Nan-aware k-means.
  Returns array of centroids in row-major array-of-array-of-doubles format."
  ^"[[D" [dataset & [k max-iterations num-runs error-on-missing?
                     tolerance]]
  ;;Smile expects data in row-major format.  If we use ds/->row-major, then NAN
  ;;values will throw exceptions and it won't be as efficient as if we build the
  ;;datastructure with a-priori knowledge
  (let [num-runs (int (or num-runs 1))
        tolerance (double (or tolerance 1E-4))]
    (if true ;;(= num-runs 1)
      (-> (KMeans/lloyd (to-row-major-double-array-of-arrays dataset error-on-missing?)
                        (int (or k 5)))
          (.centroids))
      ;;This fails as the initial distortion calculation returns nan
      (-> (KMeans/fit
           (to-row-major-double-array-of-arrays dataset error-on-missing?)
           (int (or k 5))
           (int (or max-iterations 100))
           (int num-runs)
           tolerance)
          (.centroids)))))


(defn- ensure-no-missing!
  [dataset msg-begin]
  (when-let [cols-miss (columns-with-missing-seq dataset)]
    (throw (ex-info msg-begin
                    {:missing-columns cols-miss}))))


(defn g-means
  "g-means. Not NAN aware, missing is an error.
  Returns array of centroids in row-major array-of-array-of-doubles format."
  ^"[[D" [dataset & [max-k error-on-missing?]]
  ;;Smile expects data in row-major format.  If we use ds/->row-major, then NAN
  ;;values will throw exceptions and it won't be as efficient as if we build the
  ;;datastructure with a-priori knowledge
  (ensure-no-missing! dataset "G-Means - dataset cannot have missing values")
  (-> (GMeans/fit (to-row-major-double-array-of-arrays dataset error-on-missing?)
                  (int (or max-k 5)))
      (.centroids)))


(defn x-means
  "x-means. Not NAN aware, missing is an error.
  Returns array of centroids in row-major array-of-array-of-doubles format."
  ^"[[D" [dataset & [max-k error-on-missing?]]
  ;;Smile expects data in row-major format.  If we use ds/->row-major, then NAN
  ;;values will throw exceptions and it won't be as efficient as if we build the
  ;;datastructure with a-priori knowledge
  (ensure-no-missing! dataset "X-Means - dataset cannot have missing values")
  (-> (XMeans/fit (to-row-major-double-array-of-arrays dataset error-on-missing?)
                  (int (or max-k 5)))
      (.centroids)))


(def find-static
  (parallel-req/memoize
   (fn [^Class cls ^String fn-name & fn-arg-types]
     (let [method (doto (.getDeclaredMethod cls fn-name (into-array ^Class fn-arg-types))
                    (.setAccessible true))]
       (fn [& args]
         (.invoke method nil (into-array ^Object args)))))))


(defn nan-aware-mean
  ^double [^doubles col-data]
  (let [col-len (alength col-data)
        [sum n-elems]
        (loop [sum (double 0)
                 n-elems (int 0)
                 idx (int 0)]
            (if (< idx col-len)
              (let [col-val (aget col-data (int idx))]
                (if-not (Double/isNaN col-val)
                  (recur (+ sum col-val)
                         (unchecked-add n-elems 1)
                         (unchecked-add idx 1))
                  (recur sum
                         n-elems
                         (unchecked-add idx 1))))
              [sum n-elems]))]
    (if-not (= 0 (long n-elems))
      (/ sum (double n-elems))
      Double/NaN)))


(defn nan-aware-squared-distance
  "Nan away squared distance."
  ^double [lhs rhs]
  ;;Wrap find-static so we have good type hinting.
  ((find-static PartitionClustering "squaredDistance"
                (Class/forName "[D")
                (Class/forName "[D"))
   lhs rhs))


(defn group-rows-by-nearest-centroid
  [dataset ^"[[D" row-major-centroids & [error-on-missing?]]
  (let [[num-centroids num-columns] (dtype/shape row-major-centroids)
        [ds-cols _ds-rows] (dtype/shape dataset)
        num-centroids (int num-centroids)
        num-columns (int num-columns)
        ds-cols (int ds-cols)]
    (when-not (= num-columns ds-cols)
      (throw (ex-info (format "Centroid/Dataset column count mismatch - %s vs %s"
                              num-columns ds-cols)
                      {:centroid-num-cols num-columns
                       :dataset-num-cols ds-cols})))

    (when (= 0 num-centroids)
      (throw (ex-info "No centroids passed in."
                      {:centroid-shape (dtype/shape row-major-centroids)})))

    (->> (to-row-major-double-array-of-arrays dataset error-on-missing?)
         (map-indexed vector)
         (pmap (fn [[row-idx row-data]]
                 {:row-idx row-idx
                  :row-data row-data
                  :centroid-idx
                  (loop [current-idx (int 0)
                         best-distance (double 0.0)
                         best-idx (int 0)]
                    (if (< current-idx num-centroids)
                      (let [new-distance (nan-aware-squared-distance
                                          (aget row-major-centroids current-idx)
                                          row-data)]
                        (if (or (= current-idx 0)
                                (< new-distance best-distance))
                          (recur (unchecked-add current-idx 1)
                                 new-distance
                                 current-idx)
                          (recur (unchecked-add current-idx 1)
                                 best-distance
                                 best-idx)))
                      best-idx))}))
         (group-by :centroid-idx))))


(defn compute-centroid-and-global-means
  "Return a map of:
  centroid-means - centroid-index -> (double array) column means.
  global-means - global means (double array) for the dataset."
  [dataset ^"[[D" row-major-centroids]
  {:centroid-means
   (->> (group-rows-by-nearest-centroid dataset row-major-centroids false)
        (map (fn [[centroid-idx grouping]]
               [centroid-idx (->> (map :row-data grouping)
                                  (into-array (Class/forName "[D"))
                                  ;;Make column major
                                  transpose-double-array-of-arrays
                                  (pmap nan-aware-mean)
                                  double-array)]))
        (into {}))
   :global-means (->> (columns dataset)
                      (pmap (comp nan-aware-mean
                                  #(ds-col/to-double-array % false)))
                      double-array)})


(defn- non-nan-column-mean
  "Return the column mean, if it exists in the groupings else return nan."
  [centroid-groupings centroid-means row-idx col-idx]
  (let [applicable-means (->> centroid-groupings
                              (filter #(contains? (:row-indexes %) row-idx))
                              seq)]
    (when-not (< (count applicable-means) 2)
      (throw (ex-info "Programmer Error...Multiple applicable means seem to apply"
                      {:applicable-mean-count (count applicable-means)
                       :row-idx row-idx})))
    (when-let [{:keys [centroid-idx]} (first applicable-means)]
      (when-let [centroid-means (get centroid-means centroid-idx)]
        (let [col-mean (aget ^doubles centroid-means (int col-idx))]
          (when-not (Double/isNaN col-mean)
            col-mean))))))


(defn impute-missing-by-centroid-averages
  "Impute missing columns by first grouping by nearest centroids and then computing the
  mean.  In the case where the grouping for a given centroid contains all NaN's, use the
  global dataset mean.  In the case where this is NaN, this algorithm will fail to
  replace the missing values with meaningful values.  Return a new dataset."
  [dataset row-major-centroids {:keys [centroid-means global-means]}]
  (let [columns-with-missing (->> (columns dataset)
                                  (map-indexed vector)
                                  ;;For the columns that actually have something missing
                                  ;;that we care about...
                                  (filter #(> (count (ds-col/missing (second %)))
                                              0)))]
    (if-not (seq columns-with-missing)
      dataset
      (let [;;Partition data based on all possible columns
            centroid-groupings
            (->> (group-rows-by-nearest-centroid dataset row-major-centroids false)
                 (mapv (fn [[centroid-idx grouping]]
                         {:centroid-idx centroid-idx
                          :row-indexes (set (map :row-idx grouping))})))
            [_n-cols n-rows] (dtype/shape dataset)
            n-rows (int n-rows)
            ^doubles global-means global-means]
        (->> columns-with-missing
             (reduce (fn [dataset [col-idx source-column]]
                       (let [col-idx (int col-idx)]
                         (update-column
                          dataset (ds-col/column-name source-column)
                          (fn [old-column]
                            (let [src-doubles (ds-col/to-double-array old-column false)
                                  new-col (ds-col/new-column
                                           old-column :float64
                                           (dtype/ecount old-column)
                                           (ds-col/metadata old-column))
                                  ^doubles col-doubles (dtype/->array new-col)]
                              (parallel-for/parallel-for
                               row-idx
                               n-rows
                               (if (Double/isNaN (aget src-doubles row-idx))
                                 (aset col-doubles row-idx
                                       (double
                                        (or (non-nan-column-mean centroid-groupings
                                                                 centroid-means
                                                                 row-idx col-idx)
                                            (aget global-means col-idx))))
                                 (aset col-doubles row-idx
                                       (aget src-doubles row-idx))))
                              new-col)))))
                     dataset))))))

(defn- key-sym->str
  ^String [item]
  (cond
    (keyword? item) (name item)
    (symbol? item) (name item)
    :else
    (str item)))


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
         y-col (ds y-colname)
         spline (.interpolate interp
                              (dtype/make-container :java-array :float64 x-col)
                              (dtype/make-container :java-array :float64 y-col))
         new-col-name (or result-name
                          (keyword (str (key-sym->str y-colname) "-loess")))
         n-elems (base/row-count ds)
         x-rdr (typecast/datatype->reader :float64 x-col)]
     (-> (base/add-or-update-column ds new-col-name
                                    (reify DoubleReader
                                      (lsize [rdr] n-elems)
                                      (read [rdr idx]
                                        (.value spline (.read x-rdr idx)))))
         (base/update-column new-col-name
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
  For more documentation on fill-range, see tech.v2.datatype.function.fill-range.

  If the column is a datetime type the operation happens in millisecond space and
  max-span may be a datetime type convertible to milliseconds.

  The result column has the same datatype as the input column.

  After the operation, if missing strategy is not nil the newly produced missing
  values along with the existing missing values will be replaced using the given
  missing strategy for all other columns.  See
  `tech.ml.dataset.missing/replace-missing` for documentation on missing strategies.
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
                      (dtype-dt-ops/->milliseconds target-col)
                      target-col)
         max-span (if (dtype-dt/datetime-datatype? max-span)
                    (dtype-dt/to-milliseconds max-span (dtype/get-datatype max-span))
                    max-span)
         {:keys [result missing]} (dfn/fill-range target-col max-span)
         ;;This is the set of values that have not changed.  Iterating through it and
         ;;and a source vector in parallel allow us to scatter the original data into
         ;;a new storage container.
         original-indexes (-> (doto  (dtype/->bitmap-set
                                      (range (dtype/ecount result)))
                                (.andNot ^RoaringBitmap missing))
                              (bitmap/bitmap->efficient-random-access-reader))
         _ (println (dtype/get-datatype result))
         result (if (dtype-dt/datetime-datatype? target-dtype)
                  (dtype-dt-ops/milliseconds->datetime target-dtype result)
                  result)]
     (println "RESULT DTYPE" target-dtype (dtype/get-datatype result))
     (->> (base/columns ds)
          (pmap
           (fn [col]
             (if (= colname (ds-col/column-name col))
               (ds-col/new-column colname result)
               (let [new-colname (ds-col/column-name col)
                     new-data (dtype/make-container
                               :java-array
                               (dtype/get-datatype col)
                               (dtype/ecount result))
                     ^RoaringBitmap col-missing (ds-col/missing col)
                     any-missing? (not (.isEmpty col-missing))
                     updated-missing (if any-missing?
                                       (scatter-missing original-indexes col-missing)
                                       (bitmap/->bitmap))
                     total-missing (dtype/set-or updated-missing missing)
                     ;;Scatter original data into new locations
                     _ (dtype/copy! col
                                    (indexed-wtr/make-indexed-writer
                                     original-indexes new-data {}))
                     new-col (ds-col/new-column new-colname new-data
                                                (meta col)
                                                total-missing)]
                 (if-not (nil? missing-strategy)
                   (ds-missing/replace-missing-with-strategy
                    new-col total-missing missing-strategy missing-value)
                   new-col)))))
          (ds-impl/new-dataset (meta ds))))))
