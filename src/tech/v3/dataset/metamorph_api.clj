(ns tech.v3.dataset.metamorph-api
  "This is an auto-generated api system - it scans the namespaces and changes the first
  to be metamorph-compliant which means transforming an argument that is just a dataset into
  an argument that is a metamorph context - a map of `{:metamorph/data ds}`.  They also return
  their result as a metamorph context."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.modelling]
            [tech.v3.dataset.protocols :as prot])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth shuffle
                            rand-nth update]))

(defmacro build-pipelined-function
  [f m]
  (let [args (map (comp vec rest) (:arglists m))
        doc-string (:doc m)]
    `(defn ~(symbol (name f)) {:doc ~doc-string :orig (symbol (var ~f))}
       ~@(for [arg args
               :let [narg (mapv #(if (map? %) 'options %) arg)
                     [a & r] (split-with (partial not= '&) narg)]]
           (list narg `(fn [ds#]
                         (let [ctx# (if (ds/dataset? ds#)
                                      {:metamorph/data ds#} ds#)]
                           (assoc ctx# :metamorph/data (apply ~f (ctx# :metamorph/data) ~@a ~(rest r))))))))))



(def ^:private excludes-dataset
  '#{bind-> all-descriptive-stats-names major-version ->dataset ->>dataset column-map-m
     mapseq-parser mapseq-rf})



(defmacro ^:private process-all-api-symbols-dataset
  []
  (let [ps (ns-publics 'tech.v3.dataset)]
    `(do ~@(for [[f v] ps
                 :when (not (excludes-dataset f))
                 :let [m (meta v)
                       f (symbol "tech.v3.dataset" (name f))]]
             `(build-pipelined-function ~f ~m)))))


(def ^:private excludes-dataset-modelling
  '#{ds-mod/inference-column?})

(defmacro ^:private process-all-api-symbols-dataset-modelling
  []
  (let [ps (ns-publics 'tech.v3.dataset.modelling)]
    `(do ~@(for [[f v] ps
                 :when (not (excludes-dataset-modelling f))
                 :let [m (meta v)
                       f (symbol "tech.v3.dataset.modelling" (name f))]]
             `(build-pipelined-function ~f ~m)))))



(process-all-api-symbols-dataset)
(process-all-api-symbols-dataset-modelling)

(comment
  (require '[tech.v3.datatype.export-symbols :as export-symbols])
  (export-symbols/write-api! 'tech.v3.dataset.metamorph-api
                             'tech.v3.dataset.metamorph
                             "src/tech/v3/dataset/metamorph.clj"
                             '[filter group-by sort-by concat take-nth shuffle
                               rand-nth update])
  )
