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
(do
  (require '[tech.v3.datatype.functional :as dfn])
  (require '[tech.v3.datatype.argops :as argops])
  (require '[tech.v3.datatype.unary-pred :as un-pred])
  (defonce flights (ds/->dataset "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv")))

(time (-> (dfn/+ (flights "arr_delay")
                 (flights "dep_delay"))
          (dfn/< 0)
          (un-pred/bool-reader->indexes)
          (dtype/ecount)))

;;Another way to get the same result is to use summation.  Booleans are
;;interpreted very specifically below where false is 0 and 1 is true.
;;Double summation is very fast.
(time (-> (dfn/+ (flights "arr_delay")
                 (flights "dep_delay"))
          (dfn/< 0)
          (dfn/sum)))



  )
