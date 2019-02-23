(ns tech.ml.dataset.etl.pipeline-operators
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset.etl.impl.pipeline-operators
             :refer [def-single-column-etl-operator
                     def-multiple-column-etl-operator]
             :as pipe-ops]
            [tech.compute.tensor.functional.impl :as func-impl]
            [tech.datatype :as dtype]
            [tech.datatype.java-primitive :as primitive]
            [tech.ml.dataset.etl.column-filters :as column-filters]
            [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.etl.impl.math-ops :as math-ops]
            [tech.ml.utils :as utils]
            [tech.compute.tensor :as ct]
            [tech.compute.tensor.operations :as ct-ops]
            [clojure.set :as c-set]
            [clojure.core.matrix :as m]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.options :as options])
  (:refer-clojure :exclude [remove filter]))


(def-single-column-etl-operator set-attribute
  "Set a metadata attribute on a column.  Return a new dataset."
  nil
  (let [retval (ds/update-column
                dataset column-name
                (fn [col]
                  (->> (merge (ds-col/metadata col)
                              (apply hash-map op-args))
                       (ds-col/set-metadata col))))]
    retval))


(def-single-column-etl-operator remove
  "Remove a column from the dataset."
  nil
  (ds/remove-column dataset column-name))


(def-single-column-etl-operator replace-missing
  "Replace missing values with a constant.  The constant may be the result
of running a math expression.  e.g.:
(mean (col))
"
  {:missing-value (math-ops/eval-expr {:dataset dataset
                                       :column-name column-name}
                                      (first op-args))}
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [missing-indexes (ds-col/missing col)]
       (if (> (count missing-indexes) 0)
         (ds-col/set-values col (map vector
                                     (seq missing-indexes)
                                     (repeat (:missing-value context))))
         col)))))


(def-multiple-column-etl-operator string->number
  "Replace any string values with numeric values.  Updates the label map
of the options.  Arguments may be notion or a vector of either expected
strings or tuples of expected strings to their hardcoded values."
 ;;Label maps are special and used outside of this context do we have
  ;;treat them separately
  (options/set-label-map {} (categorical/build-categorical-map
                             dataset column-name-seq (first op-args)))

  (ds/update-columns dataset column-name-seq
                     (partial categorical/column-categorical-map
                              (options/->dataset-label-map context)
                              (etl-datatype))))

(def-multiple-column-etl-operator one-hot
  "Replace string columns with one-hot encoded columns.  Argument can be nothign
or a map containing keys representing the new derived column names and values
representing which original values to encode to that particular column.  The special
keyword :rest indicates any remaining unencoded columns:
example argument:
{:main [\"apple\" \"mandarin\"]
 :other :rest}
"
  (options/set-label-map {}
                         (categorical/build-one-hot-map
                          dataset column-name-seq (first op-args)))

  (let [lmap (options/->dataset-label-map context)]
    (->> column-name-seq
         (reduce (partial categorical/column-one-hot-map lmap (etl-datatype))
                 dataset))))


(def-single-column-etl-operator replace-string
  "Replace a given string value with another value.  Useful for blanket replacing empty
strings with a known value."
  nil
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [existing-values (ds-col/column-values col)
           [src-str replace-str] op-args
           data-values (into-array String (->> existing-values
                                            (map (fn [str-value]
                                                   (if (= str-value src-str)
                                                     replace-str
                                                     str-value)))))]
       (ds-col/new-column col :string data-values)))))


(def-single-column-etl-operator ->etl-datatype
  "Marshall columns to be the etl datatype.  This changes numeric columns to be
a unified backing store datatype.  Necessary before full-table datatype declarations."
  nil
  (ds/update-column
   dataset column-name
   (fn [col]
     (if-not (= (dtype/get-datatype col) (etl-datatype))
       (let [new-col-dtype (etl-datatype)
             col-values (ds-col/column-values col)
             data-values (dtype/make-array-of-type
                          new-col-dtype
                          (if (= :boolean (dtype/get-datatype col))
                            (map #(if % 1 0) col-values)
                            col-values))]
         (ds-col/new-column col new-col-dtype data-values))
       col))))


(defn- finalize-math-result
  [result dataset column-name]
  (-> (if-let [src-col (ds/maybe-column dataset column-name)]
        (ds-col/set-metadata result (dissoc (ds-col/metadata src-col)
                                            :categorical?))
        (ds-col/set-metadata result (dissoc (ds-col/metadata result)
                                            :categorical?
                                            :target?)))
      (ds-col/set-name column-name)))


(defn- operator-eval-expr
  [dataset column-name math-expr]
  (ds/add-or-update-column
   dataset
   (-> (math-ops/eval-expr {:dataset dataset
                            :column-name column-name}
                           math-expr)
       (finalize-math-result dataset column-name))))


(def-single-column-etl-operator m=
  "Perform the math operation assigning result to selected columns."
  nil
  (operator-eval-expr dataset column-name (first op-args)))


(defn- ->row-broadcast
  [item-tensor]
  (if (number? item-tensor)
    item-tensor
    (ct/in-place-reshape item-tensor [(ct/ecount item-tensor) 1])))


(defn- sub-divide-bias
  "Perform the operation:
  (-> col
      (- sub-val)
      (/ divide-val)
      (+ bias-val))
  across the dataset.
  return a new dataset."
  [dataset column-name-seq sub-val divide-val bias-val]
  (let [src-data (ds/select dataset column-name-seq :all)
        [src-cols src-rows] (ct/shape src-data)
        colseq (ds/columns src-data)
        etl-dtype (etl-datatype)
        ;;Storing data column-major so the row is incremention fast.
        backing-store (ct/new-tensor [src-cols src-rows]
                                     :datatype etl-dtype
                                     :init-value nil)
        _ (dtype/copy-raw->item!
           (->> colseq
                (map
                 (fn [col]
                   (when-not (= (int src-rows) (ct/ecount col))
                     (throw (ex-info "Column is wrong size; ragged table not supported."
                                     {})))
                   (ds-col/column-values col))))
           (ct/tensor->buffer backing-store)
           0 {:unchecked? true})

        backing-store (-> backing-store
                          (ct-ops/- (->row-broadcast sub-val))
                          (ct-ops// (->row-broadcast divide-val))
                          (ct-ops/+ (->row-broadcast bias-val)))]

    ;;apply result back to main table
    (->> colseq
         (map-indexed vector)
         (reduce (fn [dataset [col-idx col]]
                   (ds/update-column
                    dataset (ds-col/column-name col)
                    (fn [incoming-col]
                      (ds-col/new-column incoming-col etl-dtype
                                         (ct/select backing-store col-idx :all)
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
          etl-dtype (etl-datatype)
          context-map-seq (map #(get context %) column-name-seq)
          min-values (ct/->tensor (mapv :min context-map-seq) :datatype etl-dtype)
          max-values (ct/->tensor (mapv :max context-map-seq) :datatype etl-dtype)
          col-ranges (ct/binary-op! (ct/clone min-values) 1.0 max-values
                                    1.0 min-values :-)
          [range-min range-max] (if (seq op-args)
                                  (first op-args)
                                  [-1 1])
          range-min (double range-min)
          range-max (double range-max)
          target-range (- range-max
                          range-min)
          divisor (ct/binary-op! (ct/clone col-ranges) 1.0 col-ranges
                                 1.0 target-range :/)]
      (sub-divide-bias dataset column-name-seq min-values divisor range-min))
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
 (let [argmap (apply hash-map op-args)
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
          etl-dtype (etl-datatype)
          context-map-seq (map #(get context (ds-col/column-name %)) colseq)
          mean-values (if use-mean?
                        (ct/->tensor (mapv :mean context-map-seq) :datatype etl-dtype)
                        0)
          std-values (if use-std?
                       (ct/->tensor (mapv :standard-deviation context-map-seq)
                                    :datatype etl-dtype)
                       1.0)]
      (sub-divide-bias dataset column-name-seq mean-values std-values 0.0))
    ;;no columns, noop
    dataset))


(def-multiple-column-etl-operator impute-missing
  "NAN-aware k-means missing value imputation.
1.  Perform k-means.
2.  Cluster rows according to centroids.  Use centroid means (if not NAN)
to replace missing values or global means if the centroid mean was NAN.

Algorithm fails if the entire column is missing (entire column gets NAN)."
 (let [dataset (ds/select dataset column-name-seq :all)
       argmap (or (first op-args) {:method :k-means
                                   :k 5
                                   :max-iterations 100
                                   :runs 5})
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
  "Filter the dataset based on a math expression.  The expression must return a tensor
and it must be table-rows in length.  Indexes where the return value is 0 are stripped
while indexes where the return value is nonzero are kept.  The math expression is
implicitly applied to the result of the column selection if the (col) operator is used."
  nil
  (let [result (math-ops/eval-expr {:dataset dataset
                                    :column-name column-name}
                                   (first op-args))]
    (when-not (and (func-impl/tensor? result)
                   (= (m/ecount result)
                      (second (m/shape dataset))))
      (throw (ex-info "Either scalar returned or result's is wrong length" {})))
    (let [^ints int-data (dtype/copy! result (int-array (m/ecount result)))
          num-ints (alength int-data)]
      (ds/select dataset :all (->> (range (alength int-data))
                                   (map (fn [idx]
                                          (when-not (= 0 (aget int-data (int idx)))
                                            idx)))
                                   (clojure.core/remove nil?))))))
