(ns tech.ml.dataset.etl
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.dataset.etl.impl.pipeline-operators :as pipeline-operators]
            [tech.ml.dataset.etl.column-filters :as column-filters]
            [tech.ml.dataset.etl.defaults :as defaults]
            [clojure.set :as c-set]
            [tech.ml.dataset.options :as options]
            ;;Load all standard filters, math-ops, and operators
            [tech.ml.dataset.etl.pipeline-operators :as std-operators]
            [tech.ml.dataset.etl.column-filters :as std-filters]
            [tech.ml.dataset.etl.math-ops :as std-math])
  (:import [tech.ml.protocols.etl PETLSingleColumnOperator]))


(defn apply-pipeline
  "Apply a pipeline to a dataset.  If inference? or recorded? is true,
  then the context is expected to be saved with the pipeline.
  If not running a recorded pipeline, the system inserts a pipelien command at the
  beginning to auto-convert all numeric or boolean columns to be the ETL datatype.  This
  generally fixes more problems than it creates.  If this behavior isn't desired,
  however, :do-not-auto-convert? allows you to avoid this behavior.

  Returns a map of:
  :pipeline - sequence of {:operation :context}
  :options - options map to use with dataset.
  :dataset - new dataset."
  [dataset pipeline {:keys [inference?
                            recorded?
                            do-not-auto-convert?]
                     :as options}]
  (let [dataset (ds/->dataset dataset options)]
    (with-bindings {#'defaults/*etl-datatype* (or :float64
                                                  (:datatype options))}
      (let [recorded? (or recorded? inference?)
            [dataset options] (if-not inference?
                                [(if-let [target-name (:target options)]
                                   (std-operators/set-attribute dataset target-name
                                                                :target? true)
                                   dataset)
                                 ;;Get datatype of all columns initially and full set of
                                 ;;columns.
                                 (assoc options
                                        :dataset-column-metadata
                                        {:pre-pipeline
                                         (mapv ds-col/metadata (ds/columns dataset))})]
                                ;;No change for inference case
                                [dataset options])
            ;;Check for autoconversion
            pipeline (if (or do-not-auto-convert? recorded?)
                       pipeline
                       (concat ['[->etl-datatype [or numeric? boolean?]]]
                               pipeline))
            {:keys [options dataset] :as retval}
            (->> pipeline
                 (map-indexed vector)
                 (reduce
                  (fn [retval [idx op]]
                    (try
                      (pipeline-operators/apply-pipeline-operator retval op)
                      (catch Throwable e
                        (let [local-seq (->> pipeline
                                             (take (+ idx 2))
                                             (drop (max 0 (- idx 2))))]
                          (throw (ex-info
                                  (format "Operator[%s]:\n%s\n Failed at %s:\n%s\n%s"
                                          idx (with-out-str
                                                (clojure.pprint/pprint (vec local-seq)))
                                          (str op) (.getMessage e)
                                          (if (ex-data e)
                                            (with-out-str
                                              (clojure.pprint/pprint (ex-data e)))
                                            ""))
                                          {:operator op
                                           :local-stack local-seq
                                           :error e}))))))
                  {:pipeline [] :options options :dataset dataset}))
            target-columns (set (std-filters/target? dataset))
            feature-columns (c-set/difference (set (map ds-col/column-name
                                                        (ds/columns dataset)))
                                              (set target-columns))]
        (assoc retval :options
               (-> options
                   ;;The column sequence cannot be a set as when you train
                   ;;the model is tightly bound to the sequence of columns
                   (options/set-feature-column-names (vec (ds/order-column-names
                                                           dataset
                                                           feature-columns)))
                   (options/set-label-column-names (vec (ds/order-column-names
                                                         dataset
                                                         target-columns)))
                   (assoc-in [:dataset-column-metadata :post-pipeline]
                             (mapv ds-col/metadata (ds/columns dataset)))))))))
