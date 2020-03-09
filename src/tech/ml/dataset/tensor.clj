(ns tech.ml.dataset.tensor
  "Conversion mechanisms from dataset to tensor and back"
  (:require [tech.v2.tensor :as tens]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.v2.datatype :as dtype]))


(defn column-major-tensor->dataset
  [tens & [proto-dataset table-name]]
  (when-not (= 2 (count (dtype/shape tens)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (let [[n-cols n-rows] (dtype/shape tens)
        proto-dataset (or proto-dataset (ds/->dataset [{:a 1 :b 2} {:a 2 :b 3}]))
        table-name (or table-name "_unnamed")
        first-col (first (ds/columns proto-dataset))
        datatype (dtype/get-datatype tens)]
    (ds/from-prototype proto-dataset table-name
                       (->> (range n-cols)
                            (map
                             #(ds-col/new-column
                               %
                               (tens/select tens % :all)))))))


(defn row-major-tensor->dataset
  [tens & [proto-dataset table-name]]
  (when-not (= 2 (count (dtype/shape tens)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (column-major-tensor->dataset (-> (tens/transpose tens [1 0])
                                    (tens/clone))
                                proto-dataset table-name))


(defn dataset->column-major-tensor
  [dataset datatype]
  (-> (dtype/copy-raw->item! dataset
                             (tens/new-tensor (dtype/shape dataset)
                                              :datatype datatype
                                              :init-value nil)
                             0
                             {:unchecked? true})
      first))



(defn dataset->row-major-tensor
  [dataset datatype]
  (let [[n-cols n-rows] (dtype/shape dataset)]
    (-> (dataset->column-major-tensor dataset datatype)
        ;;transpose is in-place
        (tens/transpose [1 0])
        ;;clone makes it real.
        (tens/clone))))
