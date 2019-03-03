(ns tech.ml.dataset.tensor
  "Conversion mechanisms from dataset to tensor and back"
  (:require [tech.compute.tensor :as ct]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.datatype :as dtype]))


(defn column-major-tensor->dataset
  [tens & [proto-dataset table-name]]
  (when-not (= 2 (count (ct/shape tens)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (let [[n-cols n-rows] (ct/shape tens)
        proto-dataset (or proto-dataset (ds/->dataset [{:a 1 :b 2} {:a 2 :b 3}]))
        table-name (or table-name "_unnamed")
        first-col (first (ds/columns proto-dataset))
        datatype (dtype/get-datatype tens)]
    (ds/from-prototype proto-dataset table-name
                       (->> (range n-cols)
                            (map
                             #(ds-col/new-column
                               first-col
                               datatype
                               (ct/select tens % :all)
                               {:name %}))))))


(defn row-major-tensor->dataset
  [tens & [proto-dataset table-name]]
  (when-not (= 2 (count (ct/shape tens)))
    (throw (ex-info "Tensors must be 2 dimensional to transform to datasets" {})))
  (column-major-tensor->dataset (-> (ct/transpose tens [1 0])
                                    (ct/clone))
                                proto-dataset table-name))


(defn dataset->column-major-tensor
  [dataset datatype]
  (-> (dtype/copy-raw->item! dataset
                             (ct/new-tensor (ct/shape dataset)
                                            :datatype datatype
                                            :init-value nil)
                             0
                             {:unchecked? true})
      first))



(defn dataset->row-major-tensor
  [dataset datatype]
  (let [[n-cols n-rows] (ct/shape dataset)]
    (-> (dataset->column-major-tensor dataset datatype)
        ;;transpose is in-place
        (ct/transpose [1 0])
        ;;clone makes it real.
        (ct/clone))))
