(ns tech.v3.libs.smile.matrix
  "Bindings to smile matrix pathways.


  Please include these libraries in addition to smile deps -

  `[org.bytedeco/openblas \"0.3.10-1.5.4\"]`
  `[org.bytedeco/openblas-platform \"0.3.10-1.5.4\"]`

  "
  (:require [tech.v3.tensor :as dtt]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [smile.math.matrix Matrix]
           [smile.math.blas BLAS]
           [java.nio DoubleBuffer]))




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


(defonce ^{:doc "Delay that loads the smile blas implementation when dereferenced."}
  blas* (delay (try (BLAS/getInstance)
                    (catch Throwable _e))))

(defn- smile-transpose
  ^smile.math.blas.Transpose [trans?]
  (if trans?
    smile.math.blas.Transpose/TRANSPOSE
    smile.math.blas.Transpose/NO_TRANSPOSE))


(defn- blas-matrix-multiply
  [lhs rhs]
  (let [^BLAS blas @blas*
        lhs-dtype (dtype/elemwise-datatype lhs)
        rhs (dtt/ensure-tensor rhs)
        [lhs-shape rhs-shape] (mmul-check lhs rhs)
          ;;It is worth it to copy because copy is a O(N) while matrix*matrix is
          ;;O(N^3).
        ;; lhs (externally-safe lhs)
        ;; rhs (externally-safe rhs)
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
    C))

#_(defn fit-pca-smile!
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
            :as _options}]
   (errors/when-not-errorf
    (= 2 (count (dtype/shape tensor)))
    "PCA only applies to tensors with rank 2; rank %d passed in"
    (count (dtype/shape tensor)))
   (let [{:keys [means tensor]} (ds-tens/mean-center-columns! tensor {:nan-strategy :keep})
         [n-rows _n-cols] (dtype/shape tensor)
         n-rows (long n-rows)
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
   (fit-pca-smile! tensor nil)))


#_(defn transform-pca-smile!
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
        tensor (:tensor (ds-tens/mean-center-columns! tensor (select-keys pca-info [:means])))
        project-matrix (dtt/select eigenvectors :all (range n-components))]
    (matrix-multiply tensor project-matrix)))
