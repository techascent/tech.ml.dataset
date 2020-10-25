(ns tech.v3.dataset.github-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [clojure.test :refer [deftest is]]))



(comment
  ;;This sometimes returns a 500 error.
(deftest load-github-events
  (let [ds (ds/->dataset "https://api.github.com/events" {:file-type :json
                                                          :key-fn keyword})]
    (is (every? keyword? (ds/column-names ds)))
    (is (= [8 30] (dtype/shape ds)))))

(require '[tech.v3.datatype.functional :as dfn])
(defonce flights  (ds/->dataset "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv"))
  )
