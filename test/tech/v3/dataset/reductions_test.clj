(ns tech.v3.dataset.reductions-test
  (:require [tech.v3.dataset.reductions :as ds-reductions]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset.reductions.apache-data-sketch :as ds-sketch]
            [clojure.test :refer [deftest is]]))


(deftest simple-reduction
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        agg-ds (-> (ds-reductions/group-by-column-agg
                    :symbol
                    {:n-elems (ds-reductions/row-count)
                     :price-avg (ds-reductions/mean :price)
                     :price-sum (ds-reductions/sum :price)
                     :symbol (ds-reductions/first-value :symbol)
                     :n-dates (ds-reductions/count-distinct :date :int32)}
                    [stocks stocks stocks])
                   (ds/sort-by-column :symbol))
        single-price (-> (->> (ds/group-by-column stocks :symbol)
                              (map (fn [[k ds]]
                                     {:symbol k
                                      :n-elems (ds/row-count ds)
                                      :price-sum (dfn/sum (ds :price))
                                      :price-avg (dfn/mean (ds :price))}))
                              (ds/->>dataset))
                         (ds/sort-by-column :symbol))]
    (is (= 5 (ds/row-count agg-ds)))
    (is (dfn/equals (agg-ds :n-elems)
                    (dfn/* 3 (single-price :n-elems))))
    (is (dfn/equals (agg-ds :price-sum)
                    (dfn/* 3 (single-price :price-sum))))
    (is (dfn/equals (agg-ds :price-avg)
                    (single-price :price-avg)))))


(deftest issue-201-incorrect-result-column-count
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        agg-ds (ds-reductions/group-by-column-agg
                :symbol
                {:n-elems (ds-reductions/row-count)
                 :price-avg (ds-reductions/mean :price)
                 :price-avg2 (ds-reductions/mean :price)
                 :price-avg3 (ds-reductions/mean :price)
                 :price-sum (ds-reductions/sum :price)
                 :price-med (ds-reductions/prob-median :price)
                 :symbol (ds-reductions/first-value :symbol)
                 :n-dates (ds-reductions/count-distinct :date :int32)}
                [stocks stocks stocks])
        simple-agg-ds (ds-reductions/aggregate
                       {:n-elems (ds-reductions/row-count)
                        :price-avg (ds-reductions/mean :price)
                        :price-avg2 (ds-reductions/mean :price)
                        :price-avg3 (ds-reductions/mean :price)
                        :price-sum (ds-reductions/sum :price)
                        :price-med (ds-reductions/prob-median :price)
                        :symbol (ds-reductions/first-value :symbol)
                        :n-dates (ds-reductions/count-distinct :date :int32)}
                       [stocks stocks stocks])]
    (is (= 8 (ds/column-count agg-ds)))
    (is (= 8 (ds/column-count simple-agg-ds)))))


(deftest data-sketches-test
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        result (ds-reductions/aggregate
                {:n-elems (ds-reductions/row-count)
                 :n-dates (ds-reductions/count-distinct :date :int32)
                 :n-dates-hll (ds-sketch/prob-set-cardinality :date)
                 :n-dates-theta (ds-sketch/prob-set-cardinality :date
                                                           {:algorithm :theta})
                 :n-dates-cpc (ds-sketch/prob-set-cardinality :date
                                                         {:algorithm :cpc})
                 :n-symbols-hll (ds-sketch/prob-set-cardinality :symbol {:datatype :string})
                 :n-symbols-theta (ds-sketch/prob-set-cardinality :symbol
                                                             {:algorithm :theta
                                                              :datatype :string})
                 :n-symbols-cpc (ds-sketch/prob-set-cardinality :symbol
                                                           {:algorithm :cpc
                                                            :datatype :string})
                 :quantiles (ds-sketch/prob-quantiles :price [0.25 0.5 0.75])
                 :cdfs (ds-sketch/prob-cdfs :price [50 100 150])
                 :pmfs (ds-sketch/prob-pmfs :price [50 100 150])}
                [stocks stocks stocks])
        {:keys [n-dates-hll n-dates-theta n-symbols-hll n-symbols-theta
                n-dates-cpc n-symbols-cpc]} (first (ds/mapseq-reader result))]
    (is (dfn/equals [123 123 5 5 5]
                    [n-dates-hll n-dates-theta
                     n-symbols-hll n-symbols-theta n-symbols-cpc]
                    0.1))))
