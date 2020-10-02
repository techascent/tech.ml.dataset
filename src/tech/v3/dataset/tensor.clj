(ns tech.v3.dataset.tensor
  "Conversion mechanisms from dataset to tensor and back"
  (:require [tech.v3.tensor :as dtt]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype :as dtype]))


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
          (map #(dtype/set-datatype % datatype)))
     retval)
    retval))



(defn dataset->row-major-tensor
  [dataset datatype]
  (let [[n-cols n-rows] (dtype/shape dataset)]
    (-> (dataset->column-major-tensor dataset datatype)
        ;;transpose is in-place
        (dtt/transpose [1 0])
        ;;clone makes it real.
        (dtt/clone))))
