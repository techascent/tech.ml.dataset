(ns tech.v3.dataset.set-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.set :as ds-set]
            [clojure.test :refer [deftest is]]))



(deftest union-intersection-test
  (let [ds-a (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 2 :b 3}])
        ds-b (ds/->dataset [{:a 1 :b 2} {:a 1 :b 2} {:a 3 :b 3}])]
    (is (= [{:a 2, :b 3} {:a 3, :b 3} {:a 1, :b 2} {:a 1, :b 2}]
           (ds/rows (ds-set/reduce-union [ds-a ds-b]))))
    (is (= [{:a 1, :b 2} {:a 1, :b 2}]
           (ds/rows (ds-set/reduce-intersection [ds-a ds-b]))))))
