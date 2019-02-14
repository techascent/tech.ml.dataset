(ns tech.ml.dataset.etl
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset.etl.pipeline-operators :as pipeline-operators]
            [tech.ml.dataset.etl.column-filters :as column-filters]
            [tech.ml.dataset.etl.defaults :as defaults]
            [clojure.set :as c-set])
  (:import [tech.ml.protocols.etl PETLSingleColumnOperator]))


(defn apply-pipeline
  "Returns a map of:
  :pipeline - sequence of {:operation :context}
  :dataset - new dataset."
  [dataset pipeline {:keys [inference?
                            recorded?] :as options}]
  (let [dataset (ds/->dataset dataset options)]
    (with-bindings {#'defaults/*etl-datatype* (or :float64
                                                  (:datatype options))}
      (let [recorded? (or recorded? inference?)
            [dataset options] (if-not inference?
                                [(if-let [target-name (:target options)]
                                   (pipeline-operators/set-attribute dataset target-name :target? true)
                                   dataset)
                                 ;;Get datatype of all columns initially and full set of columns.
                                 (assoc options
                                        :dataset-column-metadata
                                        {:pre-pipeline
                                         (mapv ds-col/metadata (ds/columns dataset))})]
                                ;;No change for inference case
                                [dataset options])
            {:keys [options dataset] :as retval}
            (->> pipeline
                 (map-indexed vector)
                 (reduce (fn [retval [idx op]]
                           (try
                             (pipeline-operators/apply-pipeline-operator retval op)
                             (catch Throwable e
                               (let [local-seq (->> pipeline
                                                    (take (+ idx 2))
                                                    (drop (max 0 (- idx 2))))]
                                 (throw (ex-info (format "Operator[%s]:\n%s\n Failed at %s:\n%s"
                                                         idx (with-out-str
                                                               (clojure.pprint/pprint (vec local-seq)))
                                                         (str op) (.getMessage e))
                                                 {:operator op
                                                  :error e}))))))
                         {:pipeline [] :options options :dataset dataset}))
            target-columns (set (column-filters/execute-column-filter dataset :target?))
            feature-columns (c-set/difference (set (map ds-col/column-name (ds/columns dataset)))
                                              (set target-columns))]

        (assoc retval :options
               (-> options
                   ;;The column sequence cannot be a set as when you train
                   ;;the model is tightly bound to the sequence of columns
                   (assoc :feature-columns (vec (sort feature-columns))
                          :label-columns (vec (sort target-columns)))
                   (assoc-in [:dataset-column-metadata :post-pipeline]
                             (mapv ds-col/metadata (ds/columns dataset)))))))))
