(ns tech.ml.dataset.pca
  "PCA and K-PCA using smile implementations."
  (:require [tech.compute.tensor :as ct]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.compute.tensor.operations :as tens-ops]
            [tech.datatype :as dtype])
  (:import [smile.projection PCA]
           [smile.math.matrix DenseMatrix Matrix]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- smile-dense->tensor
  "Smile matrixes are row-major."
  [^DenseMatrix dense-mat]
  (-> (.data dense-mat)
      (ct/in-place-reshape [(.nrows dense-mat)
                            (.ncols dense-mat)])))


(defn- tensor->smile-dense
  ^DenseMatrix [tens-data]
  (when-not (= 2 (count (ct/shape tens-data)))
    (throw (ex-info "Data is not right shape" {})))
  (let [[n-rows n-cols] (ct/shape tens-data)
        retval (Matrix/zeros n-rows n-cols)]
    ;;This should hit the optimized pathways if the datatypes line up.
    ;;If they don't, at least it will still work.
    (dtype/copy! tens-data (.data retval))
    retval))


(defn pca-dataset
  "Run PCA on the dataset.  Dataset must not of missing values
  or non-numeric string columns. Returns pca-info:
  {:means - vec of means
   :eigenvalues - vec of eigenvalues
   :eigenvectors - matrix of eigenvectors
  }"
  [dataset & {:keys [method datatype]
              :or {method :svd
                   datatype :float64}}]
  (let [array-of-arrays (->> (ds-tens/dataset->row-major-tensor dataset :float64)
                             (ct/rows)
                             (map dtype/->array-copy)
                             (into-array (Class/forName "[D")))
        ^PCA pca-data (case method
                        :svd (PCA. array-of-arrays)
                        :correlation (PCA. array-of-arrays true))
        data-transform (if (= datatype :float64)
                         identity
                         #(ct/clone % :datatype datatype))]
    {:means (data-transform (.getCenter pca-data))
     :eigenvalues (data-transform (.getVariance pca-data))
     :eigenvectors (data-transform (smile-dense->tensor (.getLoadings pca-data)))}))


(defn pca-transform-dataset
  "PCA transform the dataset returning a new dataset."
  [dataset pca-info n-components result-datatype]
  (let [dataset-tens (ds-tens/dataset->column-major-tensor dataset result-datatype)
        [n-cols n-rows] (ct/shape dataset-tens)
        [n-eig-rows n-eig-cols] (ct/shape (:eigenvectors pca-info))
        [n-mean-cols] (ct/shape (:means pca-info))
        _ (when-not (= (long n-cols) (long n-eig-cols))
            (throw (ex-info "Things aren't lining up."
                            {:eigenvectors (ct/shape (:eigenvectors pca-info))
                             :dataset (ct/shape dataset-tens)})))
        _ (when-not (<= (long n-components) (long n-cols))
            (throw (ex-info (format "Num components %s must be <= num cols %s"
                                    n-components n-cols)
                            {:n-components n-components
                             :n-cols n-cols})))
        project-matrix (ct/select (:eigenvectors pca-info) (range n-components) :all)
        ;;Uninitialized result - column major so conversion back to dataset is fast.
        result-matrix (ct/new-tensor [n-components n-rows] :init-value nil)]
    ;;in-place subtract means from dataset using broadcast
    (tens-ops/- dataset-tens (-> (:means pca-info)
                                 (ct/in-place-reshape [n-cols 1])))
    ;;Single gemm to transform the world
    (ct/gemm! result-matrix false false 1.0 project-matrix dataset-tens 0.0)
    ;;And off we go.
    (ds-tens/column-major-tensor->dataset result-matrix dataset "pca-result")))
