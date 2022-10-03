(ns ubertest.main
  (:require [tech.v3.dataset :as ds]
            ;;[tech.v3.libs.arrow :as arrow]
            )
  (:gen-class))


(defn -main
  [& args]
  (let [ds (ds/->dataset [{:a 1 :b 2}])]
    ;;(arrow/dataset->stream! ds "test.arrow")
    (println ds)
    )
  (println "exiting main"))
