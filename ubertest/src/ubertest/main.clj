(ns ubertest.main
  (:require [tech.v3.dataset :as ds]
            ;;[tech.v3.libs.arrow :as arrow]
            )
  (:gen-class))


(defn -main
  [& args]
  (let [ds (ds/->dataset {:a (range 10)
                          :b (mapv double (range 10))})]
    ;;(arrow/dataset->stream! ds "test.arrow")
    )
  (println "exiting main"))
