(ns tech.v3.dataset.parser-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.protocols :as ds-proto]
            [clojure.test :refer [deftest is]]))


(deftest all-missing-ds
  (let [p (ds/dataset-parser)
        _ (ds-proto/add-row p {})
        ds @p]
    (is (not (ds/dataset? ds)))
    (is (= 1 (:tech.v3.dataset/row-count ds))
        (= :all (:tech.v3.dataset/missing ds)))))
