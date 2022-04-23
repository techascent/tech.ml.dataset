(ns ubertest.main
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.tensor :as dtt])
  (:gen-class))


(defn -main
  [& args]
  (let [ds (ds/->dataset {:a [1 2]
                          :b [3 4]})]
    (println ds)
    (println (ds-tens/dataset->tensor ds))
    (shutdown-agents)))
