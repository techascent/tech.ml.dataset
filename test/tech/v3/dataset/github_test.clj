(ns tech.ml.dataset.github-test
  (:require [tech.ml.dataset :as ds]
            [tech.v2.datatype :as dtype]
            [tech.io :as io]
            [clojure.test :refer [deftest is]]))

(comment
  ;;This sometimes returns a 500 error.
  (deftest load-github-events
    (let [ds (-> (io/get-json "https://api.github.com/events"
                              :key-fn keyword)
                 (ds/->dataset))]
      (is (every? keyword? (ds/column-names ds)))
      (is (= [8 30] (dtype/shape ds)))))
  )
