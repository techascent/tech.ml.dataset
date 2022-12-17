(ns tech.v3.dataset.neanderthal
  "Conversion of a dataset to/from a neanderthal dense matrix as well as various
  dataset transformations such as pca, covariance and correlation matrixes.


  Please include these additional dependencies in your project:

```clojure
  [uncomplicate/neanderthal \"0.45.0\"]
```"
  (:require [uncomplicate.neanderthal.core :as n-core]
            [uncomplicate.neanderthal.native :as n-native]
            [uncomplicate.neanderthal.linalg :as linalg]
            [uncomplicate.commons.core :as n-com-core]
            [tech.v3.libs.neanderthal :as dt-nean]
            [tech.v3.libs.neanderthal]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.tensor :as dtt]
            [tech.v3.parallel.for :as pfor]
            [clojure.tools.logging :as log]))


(defn dataset->dense
  "Convert a dataset into a dense neanderthal CPU matrix.  If the matrix
  is column-major, then potentially you can get accelerated copies from the dataset
  into neanderthal.

  * neanderthal-layout - either :column for a column-major matrix or :row for a row-major
    matrix.
  * datatype - either :float64 or :float32"
  ([dataset neanderthal-layout datatype]
   (let [[n-cols n-rows] (dtype/shape dataset)
         retval (case datatype
                  :float64
                  (n-native/dge n-rows n-cols {:layout neanderthal-layout})
                  :float32
                  (n-native/fge n-rows n-cols {:layout neanderthal-layout}))
         tens (dtt/ensure-tensor retval)
         tens-cols (dtt/columns tens)]
     ;;If possible, these will be accelerated copies
     (->> (pfor/pmap (fn [tens-col ds-col]
                       (dtype/copy! ds-col tens-col))
                     tens-cols
                     (vals dataset))
          (dorun))
     retval))
  ([dataset neanderthal-layout]
   (dataset->dense dataset neanderthal-layout :float64))
  ([dataset]
   (dataset->dense dataset :column :float64)))


(defn dense->dataset
  "Given a neanderthal matrix, convert its columns into the columns of a
  tech.v3.dataset.  This does the conversion in-place.  If you would like to copy
  the neanderthal matrix into JVM arrays, then after method use dtype/clone."
  [matrix]
  (->> (n-core/cols matrix)
       (map dtt/ensure-tensor)
       (ds-impl/new-dataset :neandtheral)))


(defn fit-pca!
  "Run Principle Component Analysis on a tensor.

  Keep in mind that PCA may be highly influenced by outliers in the dataset
  and a probabilistic or some level of auto-encoder dimensionality reduction
  more effective for your problem.


  Returns a map of:

    * :means - vec of means
    * :eigenvalues - vec of eigenvalues.  These are the variance of columns of the
       post-projected tensor if :cov is used.  They are in the ballpark if :svd is used.
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
   (let [{:keys [means tensor]} (ds-tens/mean-center-columns! tensor {:nan-strategy :keep})
         [n-rows n-cols] (dtype/shape tensor)
         n-rows (long n-rows)
         n-cols (long n-cols)
         matrix (dt-nean/tensor->matrix tensor)]
     (case method
       :svd
       (let [{:keys [sigma u vt]} (linalg/svd matrix false true)
             retval {:eigenvalues (dtype/clone (dtt/as-tensor (n-core/dia sigma)))
                     :means means
                     :method :svd
                     :eigenvectors (dtype/clone (dtt/transpose (dtt/as-tensor vt) [1 0]))}]
         (n-com-core/release matrix)
         (n-com-core/release sigma)
         (n-com-core/release u)
         (n-com-core/release vt)
         retval)
       :cov
       (let [;;Because we have subtracted out the means above, the covariance matrix
             ;;is defined by (/ (Xt*X) (- n-rows 1))
             tens-dt (dtype/elemwise-datatype tensor)
             trans-mat (n-core/trans matrix)
             cov-mat (n-core/mm trans-mat matrix)
             bias (double (if covariance-bias?
                            n-rows
                            (dec n-rows)))
             cov-buf (dtype/as-buffer cov-mat)
             ;; Neanderthal's vecmath space failed us here
             inv-bias (/ 1.0 bias)
             _ (pfor/parallel-for idx (* n-cols n-cols)
                                  (.writeDouble cov-buf idx
                                                (* inv-bias (.readDouble cov-buf idx))))
             fact (dt-nean/datatype->native-factory tens-dt)
             w (n-core/ge fact n-cols 1)
             vl (n-core/ge fact n-cols n-cols)
             sym-cov (n-core/view-sy cov-mat)
             _ (linalg/ev! sym-cov w vl nil)
             wt (dtt/as-tensor w)
             eigvals (dtt/select wt :all 0)
             validx (argops/argsort :tech.numerics/> eigvals)
             eigvals (dtype/clone (dtt/select eigvals validx))
             eigvecs (dtype/clone (dtt/select vl :all validx))]
         (n-com-core/release matrix)
         (n-com-core/release cov-mat)
         (n-com-core/release w)
         (n-com-core/release vl)
         {:eigenvalues eigvals
          :means means
          :eigenvectors eigvecs
          :method :cov}))))
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
        tensor (:tensor (ds-tens/mean-center-columns! tensor (select-keys pca-info [:means])))
        project-matrix (dtt/select eigenvectors :all (range n-components))
        ntens (dt-nean/tensor->matrix tensor)
        nproj (dt-nean/tensor->matrix project-matrix nil (dtype/elemwise-datatype tensor))
        resmat (n-core/mm ntens nproj)
        result (dtype/clone (dtt/as-tensor resmat))]
    (n-com-core/release ntens)
    (n-com-core/release nproj)
    (n-com-core/release resmat)
    result))


(defrecord PCATransform [means eigenvalues eigenvectors n-components result-datatype])

(def ^:private neanderthal-fns*
  (delay
    (try
      {:fit-pca (requiring-resolve 'tech.v3.dataset.neanderthal/fit-pca!)
       :transform-pca (requiring-resolve 'tech.v3.dataset.neanderthal/transform-pca!)}
      (catch Exception e
        (log/debugf "Neanderthal loading failed: %s" (str e))
        {}))))


(defn neanderthal-enabled?
  []
  (empty? @neanderthal-fns*))


(defn fit-pca
  "Run PCA on the dataset.  Dataset must not have missing values
  or non-numeric string columns.

  Keep in mind that PCA may be highly influenced by outliers in the dataset
  and a probabilistic or some level of auto-encoder dimensionality reduction
  more effective for your problem.


  Returns pca-info:
  {:means - vec of means
   :eigenvalues - vec of eigenvalues
   :eigenvectors - matrix of eigenvectors
  }


  Use transform-pca with a dataset and the the returned value to perform
  PCA on a dataset.

  Options:

    - method - svd, cov - Either use SVD or covariance based method.  SVD is faster
      but covariance method means the post-projection variances are accurate.
      Defaults to cov.  Both methods produce similar projection matrixes.
    - variance-amount - fractional amount of variance to keep.  Defaults to 0.95.
    - n-components - If provided overrides variance amount and sets the number of
      components to keep. This controls the number of result columns directly as an
      integer.
    - covariance-bias? - When using :cov, divide by n-rows if true and (dec n-rows)
      if false. defaults to false."
  (^PCATransform [dataset {:keys [n-components variance-amount]
                           :or {variance-amount 0.95} :as options}]
   (errors/when-not-error
    (== 0 (dtype/ecount (ds-base/missing dataset)))
    "Cannot pca a dataset with missing entries.  See replace-missing.")
   (let [result-datatype :float64
         {:keys [eigenvalues] :as pca-result}
         (fit-pca! (ds-tens/dataset->tensor dataset :float64) options)
         ;;The eigenvalues are the variance.
         variance-amount (double variance-amount)
         ;;We know the eigenvalues are sorted from greatest to least
         used-variance? (nil? n-components)
         n-components (long
                       (or n-components
                           (let [variance-sum (double (dfn/reduce-+ eigenvalues))
                                 target-variance-sum (* variance-amount variance-sum)
                                 n-variance (dtype/ecount eigenvalues)]
                             (loop [idx 0
                                    tot-var 0.0]
                               (if (and (< idx n-variance)
                                        (< tot-var target-variance-sum))
                                 (recur (unchecked-inc idx)
                                        (+ tot-var (double (nth eigenvalues idx))))
                                 idx)))))]
     (map->PCATransform (merge (assoc pca-result
                                      :n-components n-components
                                      :result-datatype result-datatype)
                               (when used-variance?
                                 {:variance-amount variance-amount})))))
  (^PCATransform [dataset]
   (fit-pca dataset nil)))


(defn transform-pca
  "PCA transform the dataset returning a new dataset.  The method used to generate the pca information
  is indicated in the metadata of the dataset."
  [dataset {:keys [n-components result-datatype] :as pca-transform}]
  (-> (ds-tens/dataset->tensor dataset result-datatype)
      (transform-pca! pca-transform n-components)
      (ds-tens/tensor->dataset dataset :pca-result)
      (vary-meta assoc :pca-method (:method pca-transform))))


(extend-type PCATransform
  ds-proto/PDatasetTransform
  (transform [t dataset]
    (transform-pca dataset t)))


#_(defn matrix-desc-stats
  "Return a set of information about neanderthal matrix A
  - per-column means
  - per-column variances
  - per-column standard deviations
  - covariance matrix
  - correlation matrix"
  ([A] (matrix-desc-stats nil A))
  ([options A]
   (with-release [n-cols (second (nc/dims a))
                  fge1s (entry! (nm/fge 1 5) 1)
                  means (scal! (/ 1.0 5) (nc/mm fge1s A))
                  -meancols (rk  (nc/scal -1 (nc/view-vctr fge1s)) (nc/view-vctr means))
                  A-means (axpy! 1.0 A (copy -meancols))
                  A-means-sqr (vm/sqr A-means)
                  A-means-sqr-summed (mm fge1s A-means-sqr)
                  A-var (nc/scal (/ 1.0 5) A-means-sqr-summed)
                  A-stdevs (vm/sqrt A-var)
                  A-cov (nc/scal (/ 1.0 5) (mm (trans A-means) A-means))
                  sigx*sigy (rk (view-vctr A-stdevs) (view-vctr A-stdevs))
                  A-cor (vm/div A-cov sigx*sigy)]
     (pr-str [A
              means -meancols
              A-means A-means-sqr
              A-means-sqr-summed
              A-var
              A-stdevs
              A-cov
              sigx*sigy
              A-cor]))))
