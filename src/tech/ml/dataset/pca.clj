(ns tech.ml.dataset.pca
  "PCA and K-PCA using smile implementations."
  (:require [tech.v2.tensor :as tens]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.v2.datatype :as dtype])
  (:import [smile.projection PCA]
           [smile.math.matrix DenseMatrix Matrix]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- smile-dense->tensor
  "Smile matrixes are row-major."
  [^DenseMatrix dense-mat]
  (-> (.data dense-mat)
      (tens/reshape [(.nrows dense-mat)
                     (.ncols dense-mat)])))


(defn- tensor->smile-dense
  ^DenseMatrix [tens-data]
  (when-not (= 2 (count (dtype/shape tens-data)))
    (throw (ex-info "Data is not right shape" {})))
  (let [[n-rows n-cols] (dtype/shape tens-data)
        retval (Matrix/zeros n-rows n-cols)]
    ;;This should hit the optimized pathways if the datatypes line up.
    ;;If they don't, at least it will still work.
    (dtype/copy! tens-data (.data retval))
    retval))


(defn pca-dataset
  "Run PCA on the dataset.  Dataset must not have missing values
  or non-numeric string columns. Returns pca-info:
  {:means - vec of means
   :eigenvalues - vec of eigenvalues
   :eigenvectors - matrix of eigenvectors
  }"
  [dataset & {:keys [method datatype]
              :or {method :svd
                   datatype :float64}}]
  (let [array-of-arrays (->> (ds-tens/dataset->row-major-tensor dataset :float64)
                             (tens/rows)
                             (map dtype/->array-copy)
                             (into-array (Class/forName "[D")))
        ^PCA pca-data (case method
                        :svd (PCA. array-of-arrays)
                        :correlation (PCA. array-of-arrays true))
        ;;We transform out of the tensor system so that we can be sure the output of
        ;;pca-dataset can be saved with a simple system.  Tensors aren't serializeable.
        data-transform (fn [item]
                         (tens/->jvm
                          (tens/ensure-tensor item)
                          :datatype :float64
                          :base-storage :java-array))]
    {:means (data-transform (.getCenter pca-data))
     :eigenvalues (data-transform (.getVariance pca-data))
     :eigenvectors (data-transform (smile-dense->tensor (.getLoadings pca-data)))}))


(defn pca-transform-dataset
  "PCA transform the dataset returning a new dataset."
  [dataset pca-info n-components result-datatype]
  (let [dataset-tens (ds-tens/dataset->column-major-tensor dataset result-datatype)
        [n-cols n-rows] (dtype/shape dataset-tens)
        eigenvectors (tens/->tensor (:eigenvectors pca-info))
        [n-eig-rows n-eig-cols] (dtype/shape eigenvectors)
        [n-mean-cols] (dtype/shape (:means pca-info))
        _ (when-not (= (long n-cols) (long n-eig-cols))
            (throw (ex-info "Things aren't lining up."
                            {:eigenvectors (dtype/shape (:eigenvectors pca-info))
                             :dataset (dtype/shape dataset-tens)})))
        _ (when-not (<= (long n-components) (long n-cols))
            (throw (ex-info (format "Num components %s must be <= num cols %s"
                                    n-components n-cols)
                            {:n-components n-components
                             :n-cols n-cols})))
        project-matrix (tens/select eigenvectors (range n-components) :all)
        ;;The old system would auto-broadcast.  Given the confusion that broadcasting causes,
        ;;the simpler manual way may be better.
        subtract-result (dtype-fn/- dataset-tens (-> (tens/reshape (:means pca-info) [n-cols 1])
                                                     (tens/broadcast (dtype/shape dataset-tens))))]

    (-> (tens/matrix-multiply project-matrix subtract-result)
        (ds-tens/column-major-tensor->dataset dataset "pca-result"))))
