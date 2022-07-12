(ns tech.v3.dataset.reductions-test
  (:require [tech.v3.dataset.reductions :as ds-reduce]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.jvm-map :as jvm-map]
            [tech.v3.dataset.reductions.apache-data-sketch :as ds-sketch]
            [tech.v3.parallel.for :as pfor]
            [clojure.test :refer [deftest is]])
  (:import [tech.v3.datatype UnaryPredicate]
           [java.time LocalDate YearMonth]
           [java.util ArrayList]))


(deftest simple-reduction
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        agg-ds (-> (ds-reduce/group-by-column-agg
                    :symbol
                    {:n-elems (ds-reduce/row-count)
                     :price-avg (ds-reduce/mean :price)
                     :price-sum (ds-reduce/sum :price)
                     :symbol (ds-reduce/first-value :symbol)
                     :n-dates (ds-reduce/count-distinct :date :int32)}
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


(deftest simple-reduction-filtered
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        agg-ds (-> (ds-reduce/group-by-column-agg
                    :symbol
                    {:n-elems (ds-reduce/row-count)
                     :price-avg (ds-reduce/mean :price)
                     :price-sum (ds-reduce/sum :price)
                     :symbol (ds-reduce/first-value :symbol)
                     :n-dates (ds-reduce/count-distinct :date :int32)}
                    {:index-filter (fn [dataset]
                                      (let [rdr (dtype/->reader (dataset :price))]
                                        (reify UnaryPredicate
                                          (unaryLong [p idx]
                                            (> (.readDouble rdr idx) 100.0)))))}
                    [stocks stocks stocks])
                   (ds/sort-by-column :symbol))
        fstocks (ds/filter-column stocks :price #(> % 100.0))
        single-price (->
                         (->> (ds/group-by-column fstocks  :symbol)
                              (map (fn [[k ds]]
                                     {:symbol k
                                      :n-elems (ds/row-count ds)
                                      :price-sum (dfn/sum (ds :price))
                                      :price-avg (dfn/mean (ds :price))}))
                              (ds/->>dataset))
                         (ds/sort-by-column :symbol))]
    (is (= 4 (ds/row-count agg-ds)))
    (is (dfn/equals (agg-ds :n-elems)
                    (dfn/* 3 (single-price :n-elems))))
    (is (dfn/equals (agg-ds :price-sum)
                    (dfn/* 3 (single-price :price-sum))))
    (is (dfn/equals (agg-ds :price-avg)
                    (single-price :price-avg)))))


(deftest issue-201-incorrect-result-column-count
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        agg-ds (ds-reduce/group-by-column-agg
                :symbol
                {:n-elems (ds-reduce/row-count)
                 :price-avg (ds-reduce/mean :price)
                 :price-avg2 (ds-reduce/mean :price)
                 :price-avg3 (ds-reduce/mean :price)
                 :price-sum (ds-reduce/sum :price)
                 :price-med (ds-reduce/prob-median :price)
                 :symbol (ds-reduce/first-value :symbol)
                 :n-dates (ds-reduce/count-distinct :date :int32)}
                [stocks stocks stocks])
        simple-agg-ds (ds-reduce/aggregate
                       {:n-elems (ds-reduce/row-count)
                        :price-avg (ds-reduce/mean :price)
                        :price-avg2 (ds-reduce/mean :price)
                        :price-avg3 (ds-reduce/mean :price)
                        :price-sum (ds-reduce/sum :price)
                        :price-med (ds-reduce/prob-median :price)
                        :symbol (ds-reduce/first-value :symbol)
                        :n-dates (ds-reduce/count-distinct :date :int32)}
                       [stocks stocks stocks])]
    (is (= 8 (ds/column-count agg-ds)))
    (is (= 8 (ds/column-count simple-agg-ds)))))


(deftest data-sketches-test
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        result (ds-reduce/aggregate
                {:n-elems (ds-reduce/row-count)
                 :n-dates (ds-reduce/count-distinct :date :int32)
                 :n-dates-hll (ds-sketch/prob-set-cardinality :date)
                 :n-dates-theta (ds-sketch/prob-set-cardinality
                                 :date {:algorithm :theta})
                 :n-dates-cpc (ds-sketch/prob-set-cardinality
                               :date {:algorithm :cpc})
                 :n-symbols-hll (ds-sketch/prob-set-cardinality
                                 :symbol {:datatype :string})
                 :n-symbols-theta (ds-sketch/prob-set-cardinality
                                   :symbol {:algorithm :theta :datatype :string})
                 :n-symbols-cpc (ds-sketch/prob-set-cardinality
                                 :symbol {:algorithm :cpc :datatype :string})
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


(deftest reservoir-sampling-test
  (let [stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        ds-seq [stocks stocks stocks]
        small-ds-seq [(-> (ds/shuffle stocks)
                          (ds/select-rows (range 50)))]
        agg-map {:n-elems (ds-reduce/row-count)
                 :price-std (ds-reduce/reservoir-desc-stat
                             :price 100 :standard-deviation)
                 :sub-ds (ds-reduce/reservoir-dataset 100)}
        straight (ds-reduce/aggregate agg-map ds-seq)
        straight-small (ds-reduce/aggregate agg-map small-ds-seq)
        grouped (ds-reduce/group-by-column-agg :symbol agg-map ds-seq)
        grouped-small (ds-reduce/group-by-column-agg :symbol agg-map ds-seq)]

    ;;Mainly ensuring that nothing throws.
    (is (every? #(or (= 3 (ds/column-count %))
                     (= 4 (ds/column-count %)))
                [straight straight-small
                 grouped grouped-small])))
  (let [missing-ds (ds/new-dataset [(ds-col/new-column
                                     :missing (range 1000)
                                     nil
                                     (->> (range 1000)
                                          (map (fn [^long idx]
                                                 (when (== 0 (rem idx 3))
                                                   idx)))
                                          (remove nil?)))])
        agg-ds
        (ds-reduce/aggregate {:sub-ds (ds-reduce/reservoir-dataset 50)}
                             [missing-ds])
        sub-ds (first (:sub-ds agg-ds))]
    ;;Make sure we carry the missing set across
    (is (not (.isEmpty ^org.roaringbitmap.RoaringBitmap (ds/missing sub-ds))))
    (is (every? #(or (nil? %)
                     (not= 0 (rem (long %) 3)))
                (:missing sub-ds)))))


(defn- create-otfrom-init-dataset
  [& [{:keys [n-simulations n-placements n-expansion n-rows]
       :or {n-simulations 100
            n-placements 50
            n-expansion 20
            n-rows 1000000}}]]
  (->> (for [idx (range n-rows)]
         (let [sd (.minusDays (dtype-dt/local-date) (+ 200 (rand-int 365)))
               ed (.plusDays sd (rand-int n-expansion))]
           {:simulation (rand-int n-simulations)
            :placement (rand-int n-placements)
            :start sd
            :end ed}))
       (ds/->>dataset)))


(defrecord YMC [year-month ^long count ^long _row-id])


(defn- tally-days-as-year-months
  [{:keys [^LocalDate start ^LocalDate end]}]
  (let [nd (.until start end java.time.temporal.ChronoUnit/DAYS)
        tally (jvm-map/hash-map)
        incrementor (jvm-map/bi-function k v
                                         (if v
                                           (unchecked-inc (long v))
                                           1))
        _ (dotimes [idx nd]
            (let [ym (YearMonth/from (.plusDays start idx))]
              (jvm-map/compute! tally ym incrementor)))
        retval (ArrayList. (.size tally))]
    (jvm-map/foreach! tally
                      (jvm-map/bi-consumer k v (.add retval (YMC. k v 0))))
    retval))


(defn- otfrom-pathway
  [ds]
  (->> (ds/row-mapcat ds tally-days-as-year-months
                      ;;generate a sequence of datasets
                      {:result-type :as-seq})
       ;;sequence of datasets
       (ds-reduce/group-by-column-agg
        [:simulation :placement :year-month]
        {:count (ds-reduce/sum :count)})
       ;;single dataset - do joins and such here
       (#(let [ds %
               count (ds :count)]
           ;;return a sequence of datasets for next step
           [(assoc ds :count2 (dfn/sq count))]))
       (ds-reduce/group-by-column-agg
        [:placement :year-month]
        {:min-count        (ds-reduce/prob-quantile :count 0.0)
         :low-95-count     (ds-reduce/prob-quantile :count 0.05)
         :q1-count         (ds-reduce/prob-quantile :count 0.25)
         :median-count     (ds-reduce/prob-quantile :count 0.50)
         :q3-count         (ds-reduce/prob-quantile :count 0.75)
         :high-95-count    (ds-reduce/prob-quantile :count 0.95)
         :max-count        (ds-reduce/prob-quantile :count 1.0)
         :count            (ds-reduce/sum :count)})))


(defn- tally-days-columnwise
  [ds]
  (let [starts (dtype/->buffer (ds :start))
        ends (dtype/->buffer (ds :end))
        n-rows (.lsize starts)
        indexes (dtype/prealloc-list :int64 n-rows)
        year-months (dtype/prealloc-list :object n-rows) ;;ArrayList works fine here also.
        counts (dtype/prealloc-list :int32 n-rows)
        incrementor (jvm-map/bi-function k v
                                         (if v
                                           (unchecked-inc (long v))
                                           1))
        tally (jvm-map/hash-map)]
    ;;Loop through dataset and append results columnwise.
    (dotimes [row-idx n-rows]
      ;;minimize hashtable resize operations
      (.clear tally)
      (let [^LocalDate start (starts row-idx)
            ^LocalDate end (ends row-idx)
            nd (.until start end java.time.temporal.ChronoUnit/DAYS)]
        (dotimes [day-idx nd]
          (let [ym (YearMonth/from (.plusDays start day-idx))]
            (jvm-map/compute! tally ym incrementor)))
        (pfor/doiter
         kv (.entrySet tally)
         (let [^java.util.Map$Entry kv kv]
           (.addLong indexes row-idx)
           (.add year-months (.getKey kv))
           (.add counts (.getValue kv))))))
    (-> (ds/select-rows ds indexes)
        (assoc :year-month year-months
               :count counts))))


(defn- otfrom-columnwise-pathway
  [ds]
  (->> (ds/pmap-ds ds tally-days-columnwise
                      ;;generate a sequence of datasets
                      {:result-type :as-seq})
       ;;sequence of datasets
       (ds-reduce/group-by-column-agg
        [:simulation :placement :year-month]
        {:count (ds-reduce/sum :count)})
       ;;single dataset - do joins and such here
       (#(let [ds %
               count (ds :count)]
           ;;return a sequence of datasets for next step
           [(assoc ds :count2 (dfn/sq count))]))
       (ds-reduce/group-by-column-agg
        [:placement :year-month]
        {:min-count        (ds-reduce/prob-quantile :count 0.0)
         :low-95-count     (ds-reduce/prob-quantile :count 0.05)
         :q1-count         (ds-reduce/prob-quantile :count 0.25)
         :median-count     (ds-reduce/prob-quantile :count 0.50)
         :q3-count         (ds-reduce/prob-quantile :count 0.75)
         :high-95-count    (ds-reduce/prob-quantile :count 0.95)
         :max-count        (ds-reduce/prob-quantile :count 1.0)
         :count            (ds-reduce/sum :count)})))


(deftest otfrom-pathway-test
  (let [ds (create-otfrom-init-dataset)
        start (ds :start)
        end (ds :end)
        total-count (->> (dtype/emap #(dtype-dt/between %1 %2 :days) :int64 start end)
                         (dfn/sum))
        ;;warmup
        _ (do (otfrom-pathway ds)
              (otfrom-columnwise-pathway ds))
        _ (println "otfrom pathway timing")
        ofds (time (otfrom-pathway ds))
        _ (println "otfrom columnwise pathway timing")
        of-cwise-ds (time (otfrom-columnwise-pathway ds))
        ofsum (dfn/sum (ofds :count))
        of-cwise-sum (dfn/sum (of-cwise-ds :count))]
    (is (= ofsum total-count))
    (is (= of-cwise-sum total-count))))


(deftest issue-314
  (let [dstds (->
               (ds-reduce/group-by-column-agg
                :foo
                {:foos (ds-reduce/distinct :value)}
                (ds/->dataset (into [] (map (fn [i] {:foo 'foo :value (str i)})) (range 3))))
               (ds/column-map :foos-2 (fn [values] values) [:foos]))]
    (is (= ["0" "1" "2"]
           (vec (first (dstds :foos-2)))))))


(deftest issue-312
  (let [ds (ds-reduce/aggregate
            {:n-elems (ds-reduce/row-count)}
            [(ds/->dataset "test/data/example-genres.nippy")])]))



(comment
  (require '[tech.v3.datatype.jvm-map :as jvm-map])
  (import '[java.time LocalDate YearMonth]
          '[java.time.temporal ChronoUnit]
          '[java.util ArrayList])


  ;; Defrecord because in my tests with the profile assoc onto an
  ;; arraymap gets called a lot.  There are a *ton* of these objects
  ;; produced.  Note that I also add in _row_id which is used in the
  ;; dataset reduction.  A little bit of inside details :-).
  (defrecord YMC [year-month ^long days ^long _row-id])

  ;; To do a high performance incrementing pathway on an implementation of java.util.Map
  ;; you want to use the compute operator which takes an implementation of java.util.function.BiFunction
  (def incrementor (jvm-map/bi-function k v
                                        (if v
                                          (unchecked-inc (long v))
                                          1)))


  (defn tally-days-as-year-months
    "Fast inline implementation tallying the days from start to end in a map
  by year-month.  Returns map of year-month to day tally."
    [{:keys [^LocalDate start ^LocalDate end]}]
    ;;nd is num-days
    ;;This is equivalent of tick/range but avoids the clojure
    ;;sequence abstraction in favor of a direct dotimes
    (let [nd (.until start end ChronoUnit/DAYS)
          tally (jvm-map/hash-map)
          ;;nd means num-days
          _ (dotimes [idx nd]
              (jvm-map/compute! tally (YearMonth/from (.plusDays start idx)) incrementor))
          retval (ArrayList. (.size tally))]
      (jvm-map/foreach! tally (jvm-map/bi-consumer k v (.add retval (YMC. k v 0))))
      ;;Return list of maps of
      retval))

  (defn prep-days-per-month-3
    [{:keys [simulation-results costs-and-internal-weighting]}]
    (as-> simulation-results $
      ;;Make as small of an incoming dataset as possible
      (tc/select-columns $ [:simulation :placement :start :end])
      ;; Expand the dataset into one row per day from start to end.
      ;;row-mapcat uses pmap-ds for its implementation and result-type means it
      ;;returns a lazy-ish sequence of datasets
      (ds/row-mapcat $ tally-days-as-year-months {:result-type :as-seq})
      ;;First reduction - row-mapcat is producing a sequence of datasets.
      (ds-reduce/group-by-column-agg [:simulation :placement :year-month] {:days (ds-reduce/sum :days)} $)
      ;;returns a single datasets
      (#(let [ds (tc/inner-join ds costs-and-internal-weighting [:placement])]
          (let [days (ds :days)
                internal (ds :internal)
                internal-days (dfn/* days internal)
                external-days (dfn/- days internal-days)
                monthly-internal-cost (dfn/* (ds :internal-daily-cost) internal-days)
                monthly-external-cost (dfn/* (ds :external-daily-cost) external-days)]
            ;;Return sequence of datasets for next step
            [(assoc ds
                    :internal-days internal-days
                    :external-days external-days
                    :monthly-internal-cost monthly-internal-cost
                    :monthly-external-cost monthly-external-cost)]))
       $)))

  )
