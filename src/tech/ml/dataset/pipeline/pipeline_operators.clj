(ns tech.ml.dataset.pipeline.pipeline-operators
  (:require [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset.options :as options]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.categorical :as categorical]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype :as dtype]
            [tech.v2.tensor :as tens])
  (:import [tech.ml.protocols.etl
            PETLSingleColumnOperator
            PETLMultipleColumnOperator]))


(defmacro def-single-column-etl-operator
  [op-symbol docstring op-context-code op-code]
  `(def ~op-symbol
     (reify PETLSingleColumnOperator
       (build-etl-context [~'op ~'dataset ~'column-name ~'op-args]
         ~op-context-code)
       (perform-etl [~'op ~'dataset ~'column-name ~'op-args ~'context]
         ~op-code))))


(defmacro def-multiple-column-etl-operator
  [op-symbol docstring op-context-code op-code]
  `(def ~op-symbol
     (reify PETLMultipleColumnOperator
       (build-etl-context-columns [~'op ~'dataset ~'column-name-seq ~'op-args]
         ~op-context-code)
       (perform-etl-columns [~'op ~'dataset ~'column-name-seq ~'op-args ~'context]
         ~op-code))))


(def ^:dynamic *pipeline-datatype* :float64)

(defn context-datatype
  [context]
  (or (:datatype context) *pipeline-datatype*))


(defn inline-perform-operator
  [etl-op dataset colname-seq op-args]
  (let [context (etl-proto/build-etl-context-columns
                 etl-op dataset colname-seq op-args)]
    (etl-proto/perform-etl-columns
     etl-op dataset colname-seq op-args context)))


(def-multiple-column-etl-operator string->number
  "Replace any string values with numeric values.  Updates the label map
of the options.  Arguments may be notion or a vector of either expected
strings or tuples of expected strings to their hardcoded values."
 ;;Label maps are special and used outside of this context do we have
  ;;treat them separately
  (options/set-label-map {} (categorical/build-categorical-map
                             dataset column-name-seq
                             {:table-value-list op-args}))

  (ds/update-columns dataset column-name-seq
                     (partial categorical/column-categorical-map
                              (options/->dataset-label-map context)
                              (context-datatype op-args))))


(def-single-column-etl-operator replace-missing
  "Replace missing values with a constant.  The constant may be the result
of running a math expression.  e.g.:
(mean (col))"
  {:missing-value (let [op-arg op-args]
                    (if (fn? op-arg)
                      (op-arg dataset column-name)
                      op-arg))}
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [missing-indexes (ds-col/missing col)]
       (if (> (count missing-indexes) 0)
         (ds-col/set-values col (map vector
                                     (seq missing-indexes)
                                     (repeat (:missing-value context))))
         col)))))

(def-multiple-column-etl-operator one-hot
  "Replace string columns with one-hot encoded columns.  Argument can be nothing
or a map containing keys representing the new derived column names and values
representing which original values to encode to that particular column.  The special
keyword :rest indicates any remaining unencoded columns:
example argument:
{:main [\"apple\" \"mandarin\"]
 :other :rest}"
  (options/set-label-map {}
                         (categorical/build-one-hot-map
                          dataset column-name-seq (:table-value-list op-args)))

  (let [lmap (options/->dataset-label-map context)]
    (->> column-name-seq
         (reduce (partial categorical/column-one-hot-map lmap
                          (context-datatype op-args))
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


(def-single-column-etl-operator ->datatype
  "Marshall columns to be the etl datatype.  This changes numeric columns to be
a unified backing store datatype.  Necessary before full-table datatype declarations."
  nil
  (let [etl-dtype (context-datatype {:datatype op-args})]
    (ds/update-column
     dataset column-name
     (fn [col]
       (if-not (= (dtype/get-datatype col) etl-dtype)
         (let [new-col-dtype etl-dtype
               col-values (if (= 0 (count (ds-col/missing col)))
                            (ds-col/column-values col)
                            (tech.ml.protocols.column/to-double-array col false))
               data-values (dtype/make-array-of-type
                            new-col-dtype
                            col-values)]
           (ds-col/new-column col new-col-dtype data-values))
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
        etl-dtype (or datatype *pipeline-datatype*)
        ;;Storing data column-major so the row is incremention fast.
        backing-store (ds-tens/dataset->column-major-tensor
                       dataset :datatype etl-dtype)
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
                      (ds-col/new-column incoming-col etl-dtype
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
      (sub-divide-bias dataset column-name-seq mean-values std-values 0.0))
    ;;no columns, noop
    dataset))
