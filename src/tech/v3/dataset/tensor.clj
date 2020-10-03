(ns tech.v3.dataset.tensor
  "Conversion mechanisms from dataset to tensor and back"
  (:require [tech.v3.tensor :as dtt]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype :as dtype]
            [tech.v3.parallel.for :as pfor]
            [primitive-math :as pmath]
            [clojure.tools.logging :as log])
  (:import [smile.math.matrix Matrix]
           [smile.math.blas BLAS]
           [tech.v3.datatype DoubleReader Buffer NDBuffer]
           [java.util List]
           [java.nio DoubleBuffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defn column-major-tensor->dataset
  [dtt & [table-name]]
  (when-not (= 2 (count (dtype/shape dtt)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (let [[n-cols n-rows] (dtype/shape dtt)
        table-name (or table-name :_unnamed)]
    (ds/new-dataset table-name (->> (range n-cols)
                                    (map
                                     #(ds-col/new-column
                                       %
                                       (dtt/select dtt % :all)))))))


(defn row-major-tensor->dataset
  [dtt & [table-name]]
  (when-not (= 2 (count (dtype/shape dtt)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (column-major-tensor->dataset (-> (dtt/transpose dtt [1 0])
                                    (dtt/clone))
                                table-name))


(defn dataset->column-major-tensor
  [dataset datatype]
  (let [retval (dtt/new-tensor (dtype/shape dataset)
                               :datatype datatype
                               :init-value nil)]
    (dtype/copy-raw->item!
     (->> (ds/columns dataset)
          (map #(dtype/elemwise-cast % datatype)))
     retval)
    retval))



(defn dataset->row-major-tensor
  [dataset datatype]
  (-> (dataset->column-major-tensor dataset datatype)
      ;;transpose is in-place
      (dtt/transpose [1 0])
      ;;clone makes it real.
      (dtt/clone)))


(def matrix-data
  (let [data-field (doto (.getDeclaredField Matrix "A")
                     (.setAccessible true))]
    (fn [^Matrix mat]
      (.get data-field mat))))


(defn smile-dense->tensor
  "Smile matrixes are row-major."
  [^Matrix dense-mat]
  (let [mdata (matrix-data dense-mat)]
    (if (= (.layout dense-mat) smile.math.blas.Layout/COL_MAJOR)
      (dtt/reshape mdata [(.ncols dense-mat)
                          (.nrows dense-mat)])
      (dtt/reshape mdata [(.nrows dense-mat)
                          (.ncols dense-mat)]))))

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


(defn tensor->smile-dense
  ^Matrix [tens-data]
  (when-not (= 2 (count (dtype/shape tens-data)))
    (throw (ex-info "Data is not right shape" {})))
  (let [[n-rows n-cols] (dtype/shape tens-data)
        ;;Smile blas sometimes crashes with column major tensors
        [high-stride low-stride] (:strides (dtt/tensor->dimensions tens-data))
        high-stride (long high-stride)
        low-stride (long low-stride)
        ^smile.math.blas.Layout layout (if (> high-stride low-stride)
                                         smile.math.blas.Layout/ROW_MAJOR
                                         smile.math.blas.Layout/COL_MAJOR)
        leading-dimension (if (= layout smile.math.blas.Layout/ROW_MAJOR)
                            n-cols n-rows)
        ;;Force computations and coalesce into one buffer
        tens-data (externally-safe tens-data)
        array-buf (dtype/->array-buffer :float64 {:nan-strategy :keep}
                                        (dtt/tensor->buffer tens-data))
        ^doubles ary-data (.ary-data array-buf)
        nio-buf (DoubleBuffer/wrap ary-data
                                   (.offset array-buf)
                                   (.n-elems array-buf))]
    (Matrix/of layout n-rows n-cols
               leading-dimension nio-buf)))


(defn- mmul-check
  [lhs rhs]
  (let [lhs-shape (dtype/shape lhs)
        rhs-shape (dtype/shape rhs)
        rhs-shape (if (= 1 (count rhs-shape))
                    [(first rhs-shape) 1]
                    rhs-shape)]
    (when-not (and (= 2 (count lhs-shape))
                   (= 2 (count rhs-shape)))
      (throw (ex-info "Both items must have shape count 2" {})))
    (when-not (= (second lhs-shape) (first rhs-shape))
      (throw (ex-info "Inner dimensions don't match"
                      {:lhs-shape lhs-shape
                       :rhs-shape rhs-shape})))
    [lhs-shape rhs-shape]))


(defn- jvm-matrix-multiply
  [lhs rhs]
  (let [_ (mmul-check lhs rhs)
        ;;Slice is lazy, so we make it concrete and perform all the
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
                  col-idx (rem idx n-cols)]
              ;;Hardcode operation to avoid dispatch costs.  In this case we know
              ;;we are dealing with readers and we know the result is a double summation.
              (let [^Buffer lhs (.get lhs-rows row-idx)
                    ^Buffer rhs (.get rhs-columns col-idx)]
                (loop [idx 0
                       sum 0.0]
                  (if (< idx n-inner)
                    (recur (unchecked-inc idx)
                           (pmath/+ sum (pmath/* (.readDouble lhs idx)
                                                 (.readDouble rhs idx))))
                    sum))))))
        ;;Make buffer concrete
        (dtype/clone)
        ;;Reshape into result shape
        (dtt/reshape [n-rows n-cols]))))


(defonce blas* (delay (try (BLAS/getInstance)
                           (catch Throwable e))))

(defn- smile-transpose
  ^smile.math.blas.Transpose [trans?]
  (if trans?
    smile.math.blas.Transpose/TRANSPOSE
    smile.math.blas.Transpose/NO_TRANSPOSE))


(defn- blas-matrix-multiply
  [lhs rhs]
  (let [^BLAS blas @blas*
        lhs-dtype (dtype/elemwise-datatype lhs)
        rhs (dtt/ensure-tensor rhs)]
    (let [[lhs-shape rhs-shape] (mmul-check lhs rhs)
          ;;It is worth it to copy because copy is a O(N) while matrix*matrix is
          ;;O(N^3).
          lhs (externally-safe lhs)
          rhs (externally-safe rhs)
          alpha 1.0
          beta 0.0
          op-space (if (and (= lhs-dtype :float32)
                            (= (dtype/elemwise-datatype rhs) :float32))
                     :float32
                     :float64)
          C (dtt/new-tensor [(first lhs-shape) (second rhs-shape)]
                            :datatype op-space
                            :container-type :jvm-heap)
          lhs-dims (dtt/tensor->dimensions lhs)
          rhs-dims (dtt/tensor->dimensions rhs)
          C-dims (dtt/tensor->dimensions C)
          lhs-strides (get lhs-dims :strides)
          rhs-strides (get rhs-dims :strides)
          lhs-min-stride (int (apply min lhs-strides))
          rhs-min-stride (int (apply min rhs-strides))
          lhs-max-stride (int (apply max lhs-strides))
          rhs-max-stride (int (apply max rhs-strides))
          c-max-stride (int (apply max (get C-dims :strides)))
          lhs-trans? (= lhs-min-stride (first lhs-strides))
          rhs-trans? (= rhs-min-stride (first rhs-strides))]
      (if (= op-space :float64)
        (.gemm blas smile.math.blas.Layout/ROW_MAJOR
               (smile-transpose lhs-trans?)
               (smile-transpose rhs-trans?)
               ;;nmk
               (int (first lhs-shape)) (int (second rhs-shape)) (int (first rhs-shape))
               1.0 ;;alpha
               (dtype/->double-array (dtt/tensor->buffer lhs))
               lhs-max-stride
               (dtype/->double-array (dtt/tensor->buffer rhs))
               rhs-max-stride
               0.0 ;;beta
               ;;C was just created so it has to be dense.
               (dtype/->double-array C)
               c-max-stride)
        (.gemm blas smile.math.blas.Layout/ROW_MAJOR
               (smile-transpose lhs-trans?)
               (smile-transpose rhs-trans?)
               ;;nmk
               (int (first lhs-shape)) (int (second rhs-shape)) (int (first rhs-shape))
               (float 1.0) ;;alpha
               (dtype/->float-array (dtt/tensor->buffer lhs))
               lhs-max-stride
               (dtype/->float-array (dtt/tensor->buffer rhs))
               rhs-max-stride
               (float 0.0) ;;beta
               ;;C was just created so it has to be dense.
               (dtype/->float-array C)
               c-max-stride))
      C)))


(defn matrix-multiply
  ([lhs rhs {:keys [force-jvm?]}]
   (mmul-check lhs rhs)
   (if (and @blas*
            (not force-jvm?))
     (blas-matrix-multiply lhs rhs)
     (jvm-matrix-multiply lhs rhs)))
  ([lhs rhs]
   (matrix-multiply lhs rhs nil)))



(defn mean-center!
  "in-place nan-aware mean-center the rows of the tensor.  Tensor must be writable and return value
  is {:means :tensor}.

  Per-column means may be provided if pre-calculated"
  ([tens {:keys [nan-strategy means]
          :or {nan-strategy :remove}}]
   (errors/when-not-errorf
    (== 2 (count (dtype/shape tens)))
    "Tensor must have 2 dimensional shape: %s"
    (dtype/shape tens))
   (let [^List rows (mapv dtype/->buffer (dtt/slice tens 1))
         means (or means
                   (-> (reify DoubleReader
                         (lsize [rdr] (long (.size rows)))
                         (readDouble [rdr idx]
                           (let [^Buffer data (.get rows idx)]
                             (stats/mean {:nan-strategy nan-strategy} data))))
                       (dtype/clone)))
         mean-buf (dtype/->buffer means)
         n-rows (.size rows)]
     (pfor/parallel-for
      idx n-rows
      (let [^Buffer row (.get rows idx)
            mean (.readDouble mean-buf idx)]
        (dotimes [elem-idx (.lsize row)]
          (.writeDouble row elem-idx
                        (pmath/- (.readDouble row elem-idx)
                                 mean)))))
     {:means means
      :tensor tens}))
  ([tens]
   (mean-center! tens nil)))
