(ns tech.v3.dataset.infer-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.bitmap :as bitmap]
            [clojure.test :refer [deftest is]]))


(deftest simple-inference
  (letfn [(inferred-equals [lhs rhs]
            (let [test-col (-> (ds/->dataset [])
                               (assoc :testdata lhs)
                               (:testdata))]
              (is (= (dtype/elemwise-datatype test-col)
                     (dtype/elemwise-datatype rhs)))
              (is (every? identity (dfn/eq test-col rhs)))))]
    (inferred-equals [true false true false] (boolean-array [true false true false]))
    (inferred-equals (list 0 Double/NaN 1.0) (double-array [0.0 Double/NaN 1.0]))
    (inferred-equals #:tech.v3.dataset{:data [1 2 3 nil 4]
                                       :force-datatype? true}
                     [1 2 3 nil 4])
    (inferred-equals (list 0 Double/NaN 1.0 nil nil)
                     (double-array [0.0 Double/NaN 1.0 Double/NaN Double/NaN]))
    (is (= #{2 4}
           (set (ds/missing (-> (ds/->dataset [])
                                (assoc :test-data [1 2 nil 3 nil]))))))
    (is (= #{2 4}
           (set (ds/missing
                 (-> (ds/->dataset [])
                     (assoc :test-data #:tech.v3.dataset{:data [1 2 nil 3 nil]
                                                         :force-datatype? true}))))))
    (is
     (= #{}
        (set (ds/missing
              (-> (ds/->dataset [])
                  (assoc :test-data #:tech.v3.dataset{:data [1 2 nil 3 nil]
                                                      :force-datatype? true
                                                      :missing (bitmap/->bitmap)}))))))
    (is
     (= #{}
        (set (ds/missing
              (-> (ds/->dataset [])
                  (assoc :test-data #:tech.v3.dataset{:data [1 2 nil 3 nil]
                                                      :missing (bitmap/->bitmap)}))))))
    ))
