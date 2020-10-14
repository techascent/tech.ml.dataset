(ns tech.v3.dataset.tensor
  "Conversion mechanisms from dataset to tensor and back as will as zerocopy
  bindings to smile dense matrixes.  Smile has full BLAS/LAPACK bindings so this
  makes it easy to move from a dataset into transformations such as PCA and back."
  (:require [tech.v3.tensor :as dtt]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.parallel.for :as pfor]
            [primitive-math :as pmath])
  (:import [smile.math.matrix Matrix]
           [smile.math.blas BLAS]
           [tech.v3.datatype DoubleReader Buffer NDBuffer]
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


(def ^:private matrix-data
  (let [data-field (doto (.getDeclaredField Matrix "A")
                     (.setAccessible true))]
    (fn [^Matrix mat]
      (.get data-field mat))))


(defn smile-matrix-as-tensor
  "Zero-copy convert a smile matrix to a tensor."
  [^Matrix dense-mat]
  (let [mdata (matrix-data dense-mat)
        ld (.ld dense-mat)]
    ;;Current smile matrices are assuming their lowest dimension
    ;;is packed and the upper dimension is indicated by ld
    (if (= (.layout dense-mat) smile.math.blas.Layout/COL_MAJOR)
      (dtt/construct-tensor mdata
                            (dims/dimensions [(.nrows dense-mat)
                                              (.ncols dense-mat)]
                                             [1 ld]))
      (dtt/construct-tensor mdata
                            (dims/dimensions [(.nrows dense-mat)
                                              (.ncols dense-mat)]
                                             [ld 1])))))


(extend-type Matrix
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [mat] :float64)
  dtype-proto/PECount
  (ecount [mat] (* (.ncols mat) (.nrows mat)))
  dtype-proto/PToTensor
  (as-tensor [mat] (smile-matrix-as-tensor mat))
  dtype-proto/PShape
  (shape [mat] (dtype-proto/shape (dtype-proto/as-tensor mat)))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [mat]
    (dtype-proto/convertible-to-array-buffer?
     (dtype-proto/as-tensor mat)))
  (->array-buffer [mat]
    (dtype-proto/->array-buffer (dtype-proto/as-tensor mat)))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [mat]
    (dtype-proto/convertible-to-native-buffer?
     (dtype-proto/as-tensor mat)))
  (->native-buffer [mat]
    (dtype-proto/->native-buffer (dtype-proto/as-tensor mat)))
  dtype-proto/PClone
  (clone [mat] (.clone mat)))

(dtype-pp/implement-tostring-print Matrix)


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


(defn tensor->smile-matrix
  "Convert a tensor into a smile matrix.  If the tensor is backed by a concrete buffer
  and has a dimensions transformations that are supported by smile (no rotation, no broadcast,
  no complex striding),
  the conversion will be in place.  Else the tensor is first copied to a new tensor and
  then the conversion takes place."
  ^Matrix [tens-data]
  (when-not (= 2 (count (dtype/shape tens-data)))
    (throw (ex-info "Data is not right shape" {})))
  (let [[n-rows n-cols] (dtype/shape tens-data)
        ;;Force computations and coalesce into one buffer
        tens-data (externally-safe tens-data)
        ;;Smile blas sometimes crashes with row major tensors
        [high-stride low-stride] (:strides (dtt/tensor->dimensions tens-data))
        high-stride (long high-stride)
        low-stride (long low-stride)
        ;;Lower striding to what smile currently supports.  Smile can only handle packed
        ;;striding.
        [tens-data high-stride low-stride]
        (if-not (== 1 (min high-stride low-stride))
          (let [new-tens (dtt/clone tens-data :datatype :float64)
                [high-stride low-stride] (:strides (dtt/tensor->dimensions tens-data))]
            [new-tens high-stride low-stride])
          [tens-data high-stride low-stride])

        high-stride (long high-stride)
        low-stride (long low-stride)
        leading-dimension (max high-stride low-stride)
        ;;TODO - handle native buffer pathway
        array-buf (dtype/->array-buffer :float64 (dtt/tensor->buffer tens-data))
        ^doubles ary-data (.ary-data array-buf)
        nio-buf (DoubleBuffer/wrap ary-data
                                   (.offset array-buf)
                                   (.n-elems array-buf))]
    (if (= leading-dimension high-stride)
      (Matrix/of smile.math.blas.Layout/ROW_MAJOR n-rows n-cols
                 leading-dimension nio-buf)
      (->
       (Matrix/of smile.math.blas.Layout/ROW_MAJOR n-cols n-rows
                  leading-dimension nio-buf)
       (.transpose)))))


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
  "**SLOW** fallback for when openblas/mkl aren't available."
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


(defonce ^{:doc "Delay that loads the smile blas implementation when dereferenced."}
  blas* (delay (try (BLAS/getInstance)
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
  "Matrix multiply using BLAS if available with an extremely slow fallback
  on the JVM if BLAS isn't available."
  ([lhs rhs {:keys [force-jvm?]}]
   (mmul-check lhs rhs)
   (if (and @blas*
            (not force-jvm?))
     (blas-matrix-multiply lhs rhs)
     (jvm-matrix-multiply lhs rhs)))
  ([lhs rhs]
   (matrix-multiply lhs rhs nil)))



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


(defn fit-pca!
  "Run Principle Component Analysis on a tensor.

  Keep in mind that PCA may be highly influenced by outliers in the dataset
  and a probabilistic or some level of auto-encoder dimensionality reduction
  more effective for your problem.


  Returns a map of:

    * :means - vec of means
    * :eigenvalues - vec of eigenvalues.  These are the variance of columns of the
       post-projected tensor if :cov is used.
    * :eigenvectors - matrix of eigenvectors

  Options:

  - method - svd, cov - Either use SVD or covariance based method.  SVD is faster
    but covariance method means the post-projection variances are accurate.  Both
    methods produce an identical or extremely similar projection matrix. Defaults
    to `:cov`.
  - covariance-bias? - When using :cov, divide by n-rows if true and (dec n-rows)
    if false. defaults to false."
  ([tensor {:keys [method
                   covariance-bias?]
            :or {method :cov
                 covariance-bias? false}
            :as options}]
   (errors/when-not-errorf
    (= 2 (count (dtype/shape tensor)))
    "PCA only applies to tensors with rank 2; rank %d passed in"
    (count (dtype/shape tensor)))
   (let [{:keys [means tensor]} (mean-center-columns! tensor {:nan-strategy :keep})
         [n-rows n-cols] (dtype/shape tensor)
         n-rows (long n-rows)
         n-cols (long n-cols)
         smile-matrix (tensor->smile-matrix tensor)]
     (case method
       :svd
       (let [svd-data (.svd smile-matrix true true)]
         {:means means
          :method :svd
          :eigenvalues (.-s svd-data)
          :eigenvectors (dtt/ensure-tensor (.-V svd-data))})
       :cov
       (let [;;Because we have subtracted out the means above, the covariance matrix
             ;;is defined by (/ (Xt*X) (- n-rows 1))
             cov-mat (-> (matrix-multiply (dtt/transpose tensor [1 0]) tensor)
                         (dfn// (double (if covariance-bias?
                                          n-rows
                                          (dec n-rows))))
                         (tensor->smile-matrix))
             ;;Indicate this is an lower-triangular matrix.  This means the eigenvalues
             ;;calculation is much cheaper.
             _ (.uplo cov-mat smile.math.blas.UPLO/LOWER)
             ;;And here is a bug in smile.  For svd, the eigenvectors are sorted
             ;;ascending - greatest to least.  In smile's implementation when doing
             ;;corr, the result is sorted least to greatest (end result of EIG/sort
             ;;method).  This means you would be screwed if you need to, for instance,
             ;;decide n-columns by keeping variances until a threshold was met.
             eig-data (.eigen cov-mat false true true)
             eigenvalues (.-wr eig-data)
             ;;Sort DESCENDING to match svd decomposition.
             eig-order (argops/argsort :tech.numerics/> eigenvalues)]
         {:means means
          :method :cov
          :eigenvalues (dtype/indexed-buffer eig-order (.-wr eig-data))
          :eigenvectors (-> (.-Vr eig-data)
                            ;;select implicitly does an in-place transformation into a
                            ;;tensor from a smile matrix.
                            (dtt/select :all eig-order))}))))
  ([tensor]
   (fit-pca! tensor nil)))


(defn transform-pca!
  "PCA transform the dataset returning a new tensor.  Mean-centers
  the tensor in-place."
  [tensor pca-info n-components]
  (let [[_n-row n-cols] (dtype/shape tensor)
        eigenvectors (:eigenvectors pca-info)
        [_n-eig-rows n-eig-cols] (dtype/shape eigenvectors)
        _ (errors/when-not-errorf
           (= (long n-cols) (long n-eig-cols))
           "Column count of dataset (%d) does not match column count of eigenvectors (%d)"
           n-cols n-eig-cols)
        _ (errors/when-not-errorf
           (<= (long n-components) (long n-cols))
           "Num components (%d) must be <= num cols (%d)"
           n-components n-cols)
        tensor (:tensor (mean-center-columns! tensor (select-keys pca-info [:means])))
        project-matrix (dtt/select eigenvectors :all (range n-components))]
    (matrix-multiply tensor project-matrix)))
