(ns tech.v3.dataset.update-columns-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.column-filters :as cf]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))

(deftest update-columns-selector-fn
  (let [ds (ds/->dataset {:a [1. 2. 3. 4.]
                          :b [5 6 7 8]
                          :c ["A" "B" "C" "D"]})
        ds' (-> ds
                (ds/update-columns cf/numeric
                                   #(dfn// (dfn/- % (dfn/mean %))
                                           (dfn/standard-deviation %)))
                )]
    (is (> 0.001 (Math/abs (reduce + (map - [-1.16189 -0.38729 0.38729 1.16189] (vec (ds' :a)))))))
    (is (> 0.001 (Math/abs (reduce + (map - [-1.16189 -0.38729 0.38729 1.16189] (vec (ds' :d)))))))
    (is (= ["A" "B" "C" "D"] (vec (ds' :c)))))

  (let [ds (ds/->dataset {:a [1. 2. 3. 4.]
                          :b [5 6 7 8]
                          :c ["A" "B" "C" "D"]})
        ds' (as-> ds $
              (ds/update-columns $ (ds/column-names (cf/numeric $))
                                 #(dfn// (dfn/- % (dfn/mean %))
                                         (dfn/standard-deviation %)))
                )]
    (is (> 0.001 (Math/abs (reduce + (map - [-1.16189 -0.38729 0.38729 1.16189] (vec (ds' :a)))))))
    (is (> 0.001 (Math/abs (reduce + (map - [-1.16189 -0.38729 0.38729 1.16189] (vec (ds' :d)))))))
    (is (= ["A" "B" "C" "D"] (vec (ds' :c))))))
