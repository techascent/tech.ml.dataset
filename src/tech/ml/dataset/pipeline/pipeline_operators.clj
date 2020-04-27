(ns tech.ml.dataset.pipeline.pipeline-operators
  (:require [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.pca :as ds-pca]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline.column-filters :as col-filters]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.ml.dataset.pipeline.base :as pipe-base
             :refer [context-datatype]]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.update :as update-rdr]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.tensor :as tens]
            [tech.v2.datatype.typecast :as typecast]
            [tech.parallel.next-item-fn :as parallel-nfn]
            [clojure.set :as c-set])
  (:refer-clojure :exclude [replace filter])
  (:import [tech.ml.protocols.etl
            PETLSingleColumnOperator
            PETLMultipleColumnOperator]))


(defmacro def-single-column-etl-operator
  [op-symbol _docstring op-context-code op-code]
  `(def ~op-symbol
     (reify PETLSingleColumnOperator
       (build-etl-context [~'op ~'dataset ~'column-name ~'op-args]
         ~op-context-code)
       (perform-etl [~'op ~'dataset ~'column-name ~'op-args ~'context]
         ~op-code))))


(defmacro def-multiple-column-etl-operator
  [op-symbol _docstring op-context-code op-code]
  `(def ~op-symbol
     (reify PETLMultipleColumnOperator
       (build-etl-context-columns [~'op ~'dataset ~'column-name-seq ~'op-args]
         ~op-context-code)
       (perform-etl-columns [~'op ~'dataset ~'column-name-seq ~'op-args ~'context]
         ~op-code))))


(defmacro ^:private context-wrap-fn
  [& body]
  `(try
     ~@body
     (catch Throwable e#
       (throw (ex-info (format "Dataset %s execution failed at %s"
                               (ds/dataset-name pipe-base/*pipeline-dataset*)
                               (or pipe-base/*pipeline-column-name*
                                   pipe-base/*pipeline-column-name-seq*))
                       {:error e#})))))


(defn default-etl-context-columns
  "Default implementation of build-etl-context-columns"
  [op dataset column-name-seq op-args]
  (->> column-name-seq
       (map (fn [col-name]
              (when-let [etl-ctx (pipe-base/with-pipeline-vars nil col-name nil nil
                                   (context-wrap-fn
                                    (etl-proto/build-etl-context op dataset
                                                                 col-name op-args)))]
                [col-name etl-ctx])))
       (remove nil?)
       (into {})))


(defn default-perform-etl-columns
  [op dataset column-name-seq op-args context]
  (->> column-name-seq
       (reduce (fn [dataset col-name]
                 (pipe-base/with-pipeline-vars dataset col-name nil nil
                   (context-wrap-fn
                    (etl-proto/perform-etl op dataset col-name op-args
                                           (get context col-name)))))
               dataset)))


(extend-protocol etl-proto/PETLMultipleColumnOperator
  Object
  (build-etl-context-columns [op dataset column-name-seq op-args]
    (default-etl-context-columns op dataset column-name-seq op-args))

  (perform-etl-columns [op dataset column-name-seq op-args context]
    (default-perform-etl-columns op dataset column-name-seq op-args context)))


(def ^:dynamic *pipeline-context* nil)
(def ^:dynamic *pipeline-training?* true)
(def ^:dynamic *pipeline-env* (atom {}))


(defmacro pipeline-train-context
  [& body]
  `(with-bindings {#'*pipeline-context* (atom [])
                   #'*pipeline-training?* true
                   #'*pipeline-env* (atom {})}
     (let [dataset# ~@body]
       {:dataset dataset#
        :context {:operator-context @*pipeline-context*
                  :pipeline-environment @*pipeline-env*}})))


(defmacro pipeline-inference-context
  [train-context & body]
  `(with-bindings {#'*pipeline-context* (parallel-nfn/create-next-item-fn
                                         (:operator-context ~train-context))
                   #'*pipeline-training?* false
                   #'*pipeline-env* (atom {})}
     {:dataset ~@body}))


(defn store-variables
  [dataset varmap-fn]
  (let [varmap-data (if *pipeline-training?*
                      (if (fn? varmap-fn)
                        (varmap-fn dataset)
                        varmap-fn)
                      (*pipeline-context*))]
    (when (and *pipeline-training?* *pipeline-context*)
      (swap! *pipeline-context* conj (or varmap-data {})))
    (swap! *pipeline-env* merge varmap-data)
    dataset))


(defn read-var
  [varname]
  (get @*pipeline-env* varname))


(defn pif
  [dataset bool-expr pipe-when-true pipe-when-false]
  (if (if (fn? bool-expr)
        (bool-expr dataset)
        bool-expr)
    (pipe-when-true dataset)
    (pipe-when-false dataset)))


(defn pwhen
  [dataset bool-expr pipe-when-true]
  (pif dataset bool-expr pipe-when-true identity))


(defmacro without-recording
  [& body]
  `(with-bindings {#'*pipeline-context* nil}
     ~@body))


(defn inline-perform-operator
  [etl-op dataset column-filter op-args]
  (pipe-base/with-pipeline-vars dataset nil nil nil
    (let [pipeline-ctx (when (and (not *pipeline-training?*)
                                  (fn? *pipeline-context*))
                         (*pipeline-context*))]
      (if-let [colname-seq (if pipeline-ctx
                             (:column-name-seq pipeline-ctx)
                             (seq (col-filters/select-column-names column-filter)))]
        (pipe-base/with-pipeline-vars nil nil nil colname-seq
          (context-wrap-fn
           (let [context (if pipeline-ctx
                           (:context pipeline-ctx)
                           (etl-proto/build-etl-context-columns
                            etl-op dataset colname-seq op-args))]
             (when (and *pipeline-training?* *pipeline-context*)
               (swap! *pipeline-context* conj {:column-name-seq colname-seq
                                               :context context}))
             (etl-proto/perform-etl-columns
              etl-op dataset colname-seq op-args context))))
        dataset))))


(defn training?
  []
  *pipeline-training?*)


(def-single-column-etl-operator assoc-metadata
  "Assoc a new value into the metadata."
  nil
  (ds/update-column
   dataset column-name
   (fn [col]
     (ds-col/set-metadata col
                          (assoc (ds-col/metadata col)
                                 (:key op-args)
                                 (:value op-args))))))


(def-multiple-column-etl-operator remove-columns
  "Remove columns selected via the column filter"
  nil
  (ds/remove-columns dataset column-name-seq))


(def-multiple-column-etl-operator string->number
  "Replace any string values with numeric values.  Updates the label map of the options.
Arguments may be notion or a vector of either expected strings or tuples of expected
strings to their hardcoded values."
  ;;Label maps are special and used outside of this context do we have
  ;;treat them separately
  (categorical/build-categorical-map
   dataset column-name-seq
   (:table-value-list op-args))
  (-> (ds/update-columns dataset column-name-seq
                         (partial categorical/column-categorical-map
                                  context
                                  (context-datatype op-args)))
      (ds/set-metadata (update (ds/metadata dataset)
                               :label-map
                               merge
                               context))))


(def-single-column-etl-operator replace-missing
  "Replace missing values with a constant.  The constant may be the result
of running a math expression.  e.g.:
(mean (col))"
  {:missing-value (let [op-arg (:missing-value op-args)]
                    (if (fn? op-arg)
                      (pipe-base/eval-math-fn dataset column-name op-arg)
                      op-arg))}
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [missing-indexes (bitmap/->bitmap (ds-col/missing col))]
       (if (not (.isEmpty missing-indexes))
         (update-rdr/update-reader col (bitmap/bitmap-value->bitmap-map
                                        missing-indexes
                                        (:missing-value context)))
         col)))))

(def-multiple-column-etl-operator one-hot
  "Replace string columns with one-hot encoded columns.  Argument can be nothing
or a map containing keys representing the new derived column names and values
representing which original values to encode to that particular column.  The special
keyword :rest indicates any remaining unencoded columns:
example argument:
{:main [\"apple\" \"mandarin\"]
 :other :rest}"
  (categorical/build-one-hot-map
   dataset column-name-seq (:table-value-list op-args))

  (let [lmap context
        retval
        (->> column-name-seq
             (reduce (partial categorical/column-one-hot-map lmap
                              (context-datatype op-args))
                     dataset))]
    (ds/set-metadata retval
                     (update (ds/metadata dataset)
                             :label-map
                             merge
                             context))))


(def-single-column-etl-operator replace
  "Replace a given string value with another value.  Useful for blanket replacing empty
strings with a known value."
  {:value-map (let [val-map (:value-map op-args)]
                (if (fn? val-map)
                  (val-map dataset column-name)
                  val-map))}
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [result-dtype (or (:result-datatype op-args)
                            (dtype/get-datatype col))
           map-fn (:value-map context)
           new-col-name (or (:result-name op-args)
                            (ds-col/column-name col))]
       (as-> (mapv #(get map-fn % %) (dtype/->reader col)) col-values
         (ds-col/new-column column-name col-values (ds-col/metadata col)))))))


(def-single-column-etl-operator update-column
  "Update a column via a function.  Function takes a dataset and column and returns either a
column, and iterable, or a reader."
  nil
  (ds/update-column dataset column-name (partial op-args dataset)))


(def-single-column-etl-operator ->datatype
  "Marshall columns to be the etl datatype.  This changes numeric columns to be
a unified backing store datatype.  Necessary before full-table datatype declarations."
  nil
  (let [etl-dtype (context-datatype op-args)]
    (ds/update-column
     dataset column-name
     (fn [col]
       (if-not (= (dtype/get-datatype col) etl-dtype)
         (let [new-col-dtype etl-dtype
               col-values (dtype/->reader col)
               data-values (dtype/make-array-of-type new-col-dtype col-values)]
           (ds-col/new-column (ds-col/column-name col) data-values))
         col)))))


(defn- ->row-broadcast
  [item-tensor target-shape]
  (if (number? item-tensor)
    item-tensor
    (-> (tens/reshape item-tensor [(dtype/ecount item-tensor) 1])
        (tens/broadcast target-shape))))


(defn- sub-divide-bias
  "Perform the operation:
  (-> col
      (- sub-val)
      (/ divide-val)
      (+ bias-val))
  across the dataset.
  return a new dataset."
  [dataset datatype column-name-seq sub-val divide-val bias-val]
  (let [src-data (ds/select dataset column-name-seq :all)
        colseq (ds/columns src-data)
        etl-dtype (or datatype pipe-base/*pipeline-datatype*)
        ;;Storing data column-major so the row is incremention fast.
        backing-store (ds-tens/dataset->column-major-tensor src-data etl-dtype)
        ds-shape (dtype/shape backing-store)

        backing-store (-> backing-store
                          (dtype-fn/- (->row-broadcast sub-val ds-shape))
                          (dtype-fn// (->row-broadcast divide-val ds-shape))
                          (dtype-fn/+ (->row-broadcast bias-val ds-shape)))]

    ;;apply result back to main table
    (->> colseq
         (map-indexed vector)
         (reduce (fn [dataset [col-idx col]]
                   (ds/update-column
                    dataset (ds-col/column-name col)
                    (fn [incoming-col]
                      (ds-col/new-column (ds-col/column-name incoming-col)
                                         (tens/select backing-store col-idx :all)
                                         (dissoc (ds-col/metadata incoming-col)
                                                 :categorical?)))))
                 dataset))))


(def-multiple-column-etl-operator range-scaler
  "Range-scale a set of columns to be within either [-1 1] or the range provided
by the first argument.  Will fail if columns have missing values."
  (->> column-name-seq
       (map (fn [column-name]
              [column-name
               (-> (ds/column dataset column-name)
                   (ds-col/stats [:min :max]))]))
       (into {}))

  (if-let [column-name-seq (->> column-name-seq
                                (clojure.core/remove #(= (get-in context [% :min])
                                                         (get-in context [% :max])))
                                seq)]
    (let [colseq (map (partial dataset ds/column) column-name-seq)
          etl-dtype (context-datatype op-args)
          context-map-seq (map #(get context %) column-name-seq)
          min-values (tens/->tensor (mapv :min context-map-seq) :datatype etl-dtype)
          max-values (tens/->tensor (mapv :max context-map-seq) :datatype etl-dtype)
          col-ranges (dtype-fn/- max-values min-values)
          [range-min range-max] (or (:value-range op-args)
                                    [-1 1])
          range-min (double range-min)
          range-max (double range-max)
          target-range (- range-max
                          range-min)
          divisor (dtype-fn// col-ranges target-range)]
      (sub-divide-bias dataset etl-dtype column-name-seq min-values divisor range-min))
    ;;No columns, noop.
    dataset))


(defn- bool-arg
  [argmap argname default-value]
  (if (contains? argmap argname)
    (boolean (get argmap argname))
    (boolean default-value)))


(def-multiple-column-etl-operator std-scaler
  "Scale columns to have 0 mean and 1 std deviation.  Will fail if columns
contain missing values."
 (let [argmap op-args
       with-mean? (bool-arg argmap :with-mean? true)
       with-std? (bool-arg argmap :with-std? true)
       stats-seq (-> (concat (when with-mean? [:mean])
                             (when with-std? [:standard-deviation])))]
   (->> column-name-seq
        (map (fn [column-name]
               [column-name
                (-> (ds/column dataset column-name)
                    (ds-col/stats stats-seq))]))
        (into {})))

  ;;Avoid divide by zero.
  (if-let [column-name-seq (->> column-name-seq
                                (clojure.core/remove
                                 #(= 0 (get-in context [% :standard-deviation]))))]
    (let [first-ctx (get context (first column-name-seq))
          colseq (map (partial ds/column dataset) column-name-seq)
          use-mean? (contains? first-ctx :mean)
          use-std? (contains? first-ctx :standard-deviation)
          etl-dtype (context-datatype op-args)
          context-map-seq (map #(get context (ds-col/column-name %)) colseq)
          mean-values (if use-mean?
                        (tens/->tensor (mapv :mean context-map-seq) :datatype etl-dtype)
                        0)
          std-values (if use-std?
                       (tens/->tensor (mapv :standard-deviation context-map-seq)
                                      :datatype etl-dtype)
                       1.0)]
      (sub-divide-bias dataset etl-dtype column-name-seq mean-values std-values 0.0))
    ;;no columns, noop
    dataset))


(def-single-column-etl-operator m=
  "Perform some math.  This operator sets up context so the 'col' operator works."
  nil
  (let [result (pipe-base/eval-math-fn dataset column-name op-args)]
    (ds/add-or-update-column dataset column-name result)))


(def-multiple-column-etl-operator impute-missing
  "NAN-aware k-means missing value imputation.
1.  Perform k-means.
2.  Cluster rows according to centroids.  Use centroid means (if not NAN)
to replace missing values or global means if the centroid mean was NAN.

Algorithm fails if the entire column is missing (entire column gets NAN)."
 (let [dataset (ds/select dataset column-name-seq :all)
       argmap (merge {:method :k-means
                      :k 5
                      :max-iterations 100
                      :runs 5}
                     (first op-args))
       ;; compute centroids.
       row-major-centroids (case (:method argmap)
                             :k-means (ds/k-means dataset
                                                  (:k argmap)
                                                  (:max-iterations argmap)
                                                  (:runs argmap)
                                                  false))]
   (merge {:row-major-centroids row-major-centroids
           :method :centroids}
          (ds/compute-centroid-and-global-means dataset row-major-centroids)))

  (let [dataset (ds/select dataset column-name-seq :all)
        columns-with-missing (ds/columns-with-missing-seq dataset)]
    ;;Attempt a fast-out.
    (if-not columns-with-missing
      dataset
      (let [imputed-dataset (case (:method context)
                              :centroids (ds/impute-missing-by-centroid-averages
                                          (ds/select dataset column-name-seq :all)
                                          (:row-major-centroids context)
                                          context))]
        (ds/update-columns dataset (map :column-name columns-with-missing)
                           #(ds/column imputed-dataset (ds-col/column-name %)))))))


(def-single-column-etl-operator filter
  "Filter the dataset based on a math expression.  The expression must return a reader
and it must be table-rows in length.  It will be casted to doubles so only values == 0
will be exluded.  Indexes where the return value is 0 are stripped while indexes where
the return value is nonzero are kept.  The math expression is implicitly applied to the
result of the column selection if the (col) operator is used."
  nil
  (let [result (pipe-base/eval-math-fn dataset column-name op-args)
        bool-reader (typecast/datatype->reader :boolean result)]
    (when-not (= (dtype/ecount result)
                 (second (dtype/shape dataset)))
      (throw (ex-info "Either scalar returned or result's is wrong length" {})))
    (ds/select dataset :all (->> (range (dtype/ecount result))
                                 (clojure.core/filter #(.read bool-reader %))))))


(def-multiple-column-etl-operator pca
  "Perform PCA storing context during training."
  (let [dataset (ds/select dataset column-name-seq :all)
        argmap (merge {:method :svd
                       :variance 0.95}
                      (first op-args))
        pca-info (ds-pca/pca-dataset dataset
                                     :method (:method argmap)
                                     :datatype pipe-base/*pipeline-datatype*)
        n-components (long (or (:n-components argmap)
                               (let [target-var-percent (double
                                                         (or (:variance argmap)
                                                             0.95))
                                     eigenvalues (:eigenvalues pca-info)
                                     var-total (apply + 0 eigenvalues)
                                     n-cols (long (first (dtype/shape dataset)))]
                                 (loop [idx 0
                                        var-sum 0.0]
                                   (if (and (< idx n-cols)
                                            (< (/ var-sum
                                                  var-total)
                                               target-var-percent))
                                     (recur (inc idx)
                                            (+ var-sum (dtype/get-value eigenvalues
                                                                        idx)))
                                     idx)))))]
    (assoc pca-info :n-components n-components))
  (let [pca-info context
        target-dataset (ds/select dataset column-name-seq :all)
        leftover-column-names (->> (c-set/difference (set (->> (ds/columns dataset)
                                                               (map ds-col/column-name)))
                                                     (set column-name-seq))
                                   (ds/order-column-names dataset))
        transform-ds (ds-pca/pca-transform-dataset target-dataset pca-info
                                                   (:n-components pca-info)
                                                   pipe-base/*pipeline-datatype*)]
    (ds/from-prototype transform-ds (ds/dataset-name target-dataset)
                       (concat (ds/columns transform-ds)
                               (->> (ds/select dataset leftover-column-names :all)
                                    ds/columns)))))
