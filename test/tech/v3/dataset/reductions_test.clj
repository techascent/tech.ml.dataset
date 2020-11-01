(ns tech.v3.dataset.reductions-test
  (:require [tech.v3.dataset.reductions :as ds-reductions]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))


(deftest simple-reduction
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        data (->> (ds-reductions/group-by-column-aggregate
                   "symbol"
                   (ds-reductions/double-reducer ["price"]
                                                 ds-reductions/sum-consumer)
                   [stocks stocks stocks])
                  (ds-reductions/stream->vec)
                  (into {}))
        single-price (->> (ds/group-by-column stocks "symbol")
                          (map (fn [[k ds]]
                                 [k {"price" {:n-elems (ds/row-count ds)
                                              :sum (dfn/sum (ds "price"))}}]))
                          (into {}))
        key-seq (vec (keys data))
        vectorizer (fn [collection inner-key]
                     (mapv #(get-in collection [% "price" inner-key]) key-seq))]
    (is (= 5 (count data)))
    (is (dfn/equals (vectorizer data :n-elems)
                    (dfn/* 3 (vectorizer single-price :n-elems))))
    (is (dfn/equals (vectorizer data :sum)
                    (dfn/* 3 (vectorizer single-price :sum))))))
