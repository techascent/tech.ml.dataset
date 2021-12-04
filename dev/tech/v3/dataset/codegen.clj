(ns tech.v3.dataset.codegen
  (:require [tech.v3.datatype.export-symbols :as export-symbols]
            [tech.v3.datatype.errors :as errors]
            [clojure.tools.logging :as log])
  (:gen-class))


(defn -main
  [& args]

  (log/info "Generating dataset api files")

  (export-symbols/write-api! 'tech.v3.dataset-api
                             'tech.v3.dataset
                             "src/tech/v3/dataset.clj"
                             '[filter group-by sort-by concat take-nth shuffle
                               rand-nth update])

  (export-symbols/write-api! 'tech.v3.dataset.metamorph-api
                             'tech.v3.dataset.metamorph
                             "src/tech/v3/dataset/metamorph.clj"
                             '[filter group-by sort-by concat take-nth shuffle
                               rand-nth update])


  (log/info "Finished generating dataset files"))
