(ns tech.v3.dataset.tensor
  "Conversion mechanisms from dataset to tensor and back."
  (:require [tech.v3.tensor :as dtt]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.parallel.for :as pfor]
            [clj-commons.primitive-math :as pmath])

  (:import  [tech.v3.datatype DoubleReader Buffer]
            [java.util List]
            [java.nio DoubleBuffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defn tensor->dataset
  "Convert a tensor to a dataset.  Columns of the tensor will be converted to
  columns of the dataset."
  [dtt & [table-name]]
  (when-not (= 2 (count (dtype/shape dtt)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (let [[_n-rows n-cols] (dtype/shape dtt)
        table-name (or table-name :_unnamed)]
    (ds/new-dataset table-name (->> (range n-cols)
                                    (map
                                     #(ds-col/new-column
                                       %
                                       (dtt/select dtt :all %)))))))


(defn dataset->tensor
  "Convert a dataset to a tensor.  Columns of the dataset will be converted
  to columns of the tensor.  Default datatype is :float64."
  ([dataset datatype]
   (let [retval (dtt/new-tensor (dtype/shape dataset)
                                :datatype datatype
                                :init-value nil)]
     (dtype/coalesce! retval
                      (->> (ds/columns dataset)
                           (map #(dtype/elemwise-cast % datatype))))
     (dtt/transpose retval [1 0])))
  ([dataset]
   (dataset->tensor dataset :float64)))



(defn- externally-safe
  "Can we expose this tensor to outside systems - means
  no broadcasting and no offsetting (rotation)"
  ([tens datatype]
   (if (and (= datatype (dtype/elemwise-datatype tens))
            (dtt/dims-suitable-for-desc? tens))
     tens
     (dtt/clone tens :datatype datatype)))
  ([tens]
   (externally-safe tens :float64)))


(defn- jvm-matrix-multiply
  "**SLOW** fallback for when openblas/mkl aren't available."
  [lhs rhs]
  (let [;;Slice is lazy, so we make it concrete and perform all the
        ;;translations necessary up front for rapid reading
        lhs-rows ^List (mapv dtype/->buffer (dtt/slice lhs 1))
        rhs-trans (dtt/transpose rhs [1 0])
        n-rows (long (.size lhs-rows))
        rhs-columns ^List (mapv dtype/->buffer (dtt/slice rhs-trans 1))
        n-cols (long (.size rhs-columns))
        n-elems (* n-rows n-cols)
        n-inner (long (count (first lhs-rows)))]
    (-> (reify DoubleReader
          (lsize [rdr] n-elems)
          (readDouble [rdr idx]
            (let [row-idx (quot idx n-cols)
                  col-idx (rem idx n-cols)
                  ^Buffer lhs (.get lhs-rows row-idx)
                  ^Buffer rhs (.get rhs-columns col-idx)]
              ;;Hardcode operation to avoid dispatch costs.  In this case we know
              ;;we are dealing with readers and we know the result is a double summation.
              (loop [idx 0
                     sum 0.0]
                (if (< idx n-inner)
                  (recur (unchecked-inc idx)
                         (pmath/+ sum (pmath/* (.readDouble lhs idx)
                                               (.readDouble rhs idx))))
                  sum)))))
        ;;Make buffer concrete
        (dtype/clone)
        ;;Reshape into result shape
        (dtt/reshape [n-rows n-cols]))))


(defn mean-center-columns!
  "in-place nan-aware mean-center the rows of the tensor.  If tensor is writeable then this
  writes the result directly to the tensor else it clones the tensor.
  Return value is `{:means :tensor}`.

  Pre-calculated per-column means may be provided."
  ([tens {:keys [nan-strategy means]
          :or {nan-strategy :remove}}]
   (errors/when-not-errorf
    (== 2 (count (dtype/shape tens)))
    "Tensor must have 2 dimensional shape: %s"
    (dtype/shape tens))
   ;;Transpose tensor so our calculation can be in row-major form
   (let [tens (dtt/transpose tens [1 0])
         ;;Note I reversed the shape result to be column-major due to the
         ;;transpose above.
         [_n-cols n-rows] (dtype/shape tens)
         n-rows (long n-rows)
         ;;Remember the tensor is transposed, so the rows are the columns.
         ^List columns (mapv dtype/->buffer (dtt/slice tens 1))
         means (or means
                   (-> (reify DoubleReader
                         (lsize [rdr] (long (.size columns)))
                         (readDouble [rdr idx]
                           (let [^Buffer data (.get columns idx)]
                             (stats/mean data {:nan-strategy nan-strategy}))))
                       (dtype/clone)))
         mean-buf (dtype/->buffer means)
         tens-buf (dtype/->buffer tens)
         ^Buffer tens-buf (if-not (.allowsWrite tens-buf)
                            (dtype/->buffer (dtype/clone tens))
                            tens-buf)
         n-elems (.lsize tens-buf)]
     ;;Now with the transposed tensor, we can iterate and get full
     ;;parallelism but still have each thread run through the tensor
     ;;column by column.
     (pfor/parallel-for
      idx n-elems
      (let [col-idx (quot idx n-rows)
            mean (.readDouble mean-buf col-idx)]
        (.writeDouble tens-buf idx
                      (pmath/- (.readDouble tens-buf idx)
                               mean))))
     {:means means
      :tensor (dtt/transpose tens [1 0])}))
  ([tens]
   (mean-center-columns! tens nil)))


(comment
  (def test-tens (-> (dtt/transpose
                      (dtt/->tensor [[7 4 6 8 8 7 5 9 7 8]
                                     [4 1 3 6 5 2 3 5 4 2]
                                     [3 8 5 1 7 9 3 8 5 2]] :datatype :float64)
                      [1 0])))

  ;; answer
  {:means  [8.882E-17, 0.000, -4.441E-17],
   :method :svd,
   :eigenvalues [8.629338515997084, 5.751970392932843, 2.597951765363097],
   :eigenvectors [[-0.1376 0.6990 -0.7017]
                  [-0.2505 0.6609  0.7075]
                  [ 0.9583 0.2731 0.08416]]}
  )
