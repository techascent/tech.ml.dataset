(ns tech.v3.dataset.reductions-test
  (:require [tech.v3.dataset.reductions :as ds-reduce]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.dataset.reductions.apache-data-sketch :as ds-sketch]
            [tech.v3.parallel.for :as pfor]
            [ham-fisted.api :as hamf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.lazy-noncaching :as lznc]
            [clojure.test :refer [deftest is]]
            [clojure.core.protocols :as cl-proto])
  (:import [tech.v3.datatype UnaryPredicate FastStruct$FMapEntry]
           [java.time LocalDate YearMonth]
           [ham_fisted Consumers$IncConsumer MutHashTable Reductions]
           [java.util ArrayList Map$Entry Arrays]
           [clojure.lang MapEntry]))


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
                                        (hamf-fn/long-predicate
                                         idx (> (.readDouble rdr idx) 100.0))))}
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
                 :n-dates-hll (ds-sketch/prob-set-cardinality :date {:datatype :string})
                 :n-symbols-hll (ds-sketch/prob-set-cardinality
                                 :symbol {:datatype :string})
                 :quantiles (ds-sketch/prob-quantiles :price [0.25 0.5 0.75])
                 :cdfs (ds-sketch/prob-cdfs :price [50 100 150])
                 :pmfs (ds-sketch/prob-pmfs :price [50 100 150])}
                [stocks stocks stocks])
        {:keys [n-dates-hll n-symbols-hll]} (first (ds/mapseq-reader result))]
    (is (dfn/equals [123 5]
                    [n-dates-hll
                     n-symbols-hll]
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


;;Slightly less efficient than implementing an inline IReduceInit impl is to create
;;a record with a custom IReduceInit implementation.
(defrecord YMC [year-month ^long count]
  clojure.lang.IReduceInit
  (reduce [this rfn init]
    (let [init (hamf/reduced-> rfn init
                   (clojure.lang.MapEntry/create :year-month year-month)
                   (clojure.lang.MapEntry/create :count count))]
      (if (and __extmap (not (reduced? init)))
        (reduce rfn init __extmap)
        init))))


(def inc-cons-fn (hamf-fn/function k (Consumers$IncConsumer.)))

(defn- tally-days-as-year-months
  [{:keys [^LocalDate start ^LocalDate end]}]
  ;;Using a hash provider with equals semantics allows the hamf hashtable to
  ;;compete on equal terms with the java hashtable.  In that we find that compute,
  ;;computeIfAbsent and reduce perform as fast as anything on the jvm when we are using
  ;;Object/equals and Object/hashCode for the map functionality.
  (let [tally (hamf/java-hashmap)]
    (dotimes [idx (.until start end java.time.temporal.ChronoUnit/DAYS)]
      (let [ym (YearMonth/from (.plusDays start idx))]
        ;;Compute if absent is ever so slightly faster than compute as it involves
        ;;less mutation of the original hashtable.  It does, however, require the
        ;;value in the node itself to be mutable.
        (.inc ^Consumers$IncConsumer (.computeIfAbsent tally ym inc-cons-fn))))
    (hamf/custom-ireduce
     rfn acc
     (Reductions/iterReduce (.entrySet tally)
                            acc
                            (fn [acc ^Map$Entry kv]
                              (rfn acc
                                   (hamf/custom-ireduce
                                    rrfn aacc
                                    (-> aacc
                                        (rrfn (MapEntry/create :year-month (.getKey kv)))
                                        (rrfn (MapEntry/create :count (deref (.getValue kv))))))))))
    #_(lznc/map-reducible
     #(let [^Map$Entry e %]
        ;;Dataset construction using the mapseq-rf only requires the 'map' type to correctly
        ;;implement IReduceInit and for that function to produce implementations of Map$Entry.
        (hamf/custom-ireduce
         rfn acc
         ;;Elided reduced? checks for a tiny bit of extra oomph.
          (-> acc
              (rfn (MapEntry/create :year-month (.getKey e)))
              (rfn (MapEntry/create :count (deref (.getValue e)))))))
     (.entrySet tally))))


(defn- otfrom-pathway
  [ds]
  (->> (ds/row-mapcat ds tally-days-as-year-months
                      ;;generate a sequence of datasets
                      {:result-type :as-seq
                       :parser-fn {:count :int32
                                   :year-month :object}})
       ;;sequence of datasets
       (ds-reduce/group-by-column-agg
        [:simulation :placement :year-month]
        {:count (ds-reduce/sum :count)})
       ;;single dataset - do joins and such here
       (#(let [ds %
               count (ds :count)]
           (assoc ds :count2 (dfn/sq count))))
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
        incrementor (hamf-fn/bi-function k v
                                         (if v
                                           (unchecked-inc (long v))
                                           1))
        tally (hamf/java-hashmap)]
    ;;Loop through dataset and append results columnwise.
    (dotimes [row-idx n-rows]
      ;;minimize hashtable resize operations
      (.clear tally)
      (let [^LocalDate start (starts row-idx)
            ^LocalDate end (ends row-idx)
            nd (.until start end java.time.temporal.ChronoUnit/DAYS)]
        (dotimes [day-idx nd]
          (.inc ^Consumers$IncConsumer (.computeIfAbsent tally (YearMonth/from (.plusDays start day-idx)) inc-cons-fn)))
        (.forEach tally (hamf-fn/bi-consumer
                         k v
                         (.addLong indexes row-idx)
                         (.add year-months k)
                         (.add counts (deref v))))))
    (-> (ds/select-rows ds indexes)
        ;;avoid datatype and missing scans
        (assoc :year-month #:tech.v3.dataset{:data year-months
                                             :force-datatype? true
                                             :missing (tech.v3.datatype.bitmap/->bitmap)}
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
            {:n-elems (ds-reduce/count-distinct :genre)}
            [(ds/->dataset "test/data/example-genres.nippy")])]
    (is (pos? (first (ds :n-elems))))))


(deftest group-by-agg-changes-source
  (let [ds (-> [{:job "Professional" :sex "Male" :age "[35-40)" :salary 3991.2}
                {:job "Professional" :sex "Male" :age "[35-40)" :salary 2364.6}
                {:job "Professional" :sex "Male" :age "[35-40)" :salary 3114.7}
                {:job "Artist" :sex "Female" :age "[35-35)" :salary 2345.1}
                {:job "Artist" :sex "Female" :age "[35-35)" :salary 4562.1}
                {:job "Artist" :sex "Female" :age "[35-35)" :salary 1214.1}
                {:job "Artist" :sex "Female" :age "[35-35)" :salary 4531.1}]
               (ds/->dataset)
               (assoc "salary (binned)" ["a" "b" "c" "d" "e" "f" "g"]))
        ds2 (ds-reduce/group-by-column-agg
             [:job :sex :age]
             {:fj (ds-reduce/row-count)}
             [ds])]
    (is (= #{:job :sex :age :salary "salary (binned)"}
           (set (keys (.-colmap ds)))))

    ))
