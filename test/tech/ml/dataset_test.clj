(ns tech.ml.dataset-test
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline :as ds-pipe]
            [tech.v2.tensor :as tens]
            [tech.v2.datatype.functional :as dfn]
            [tech.ml.dataset.tensor :as ds-tens]
            [tech.ml.dataset.pca :as pca]
            [clojure.test :refer :all]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.unary-op :as unary-op]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [camel-snake-kebab.core :refer [->kebab-case]])
  (:import [smile.projection PCA]
           [java.util List HashSet UUID]
           [java.io File]))


(def mapseq-fruit-dataset
  (memoize
   (fn []
     (let [fruit-ds (slurp (io/resource "fruit_data_with_colors.txt"))
           dataset (->> (s/split fruit-ds #"\n")
                        (mapv #(s/split % #"\s+")))
           ds-keys (->> (first dataset)
                        (mapv (comp keyword ->kebab-case)))]
       (->> (rest dataset)
            (map (fn [ds-line]
                   (->> ds-line
                        (map (fn [ds-val]
                               (try
                                 (Double/parseDouble ^String ds-val)
                                 (catch Throwable e
                                   (-> (->kebab-case ds-val)
                                       keyword)))))
                        (zipmap ds-keys)))))))))


(deftest k-fold-sanity
  (let [dataset-seq (ds/->k-fold-datasets (mapseq-fruit-dataset) 5 {})]
    (is (= 5 (count dataset-seq)))
    (is (= [[7 47] [7 47] [7 47] [7 47] [7 48]]
           (->> dataset-seq
                (mapv (comp dtype/shape :train-ds)))))
    (is (= [[7 12] [7 12] [7 12] [7 12] [7 11]]
           (->> dataset-seq
                (mapv (comp dtype/shape :test-ds)))))))


(deftest train-test-split-sanity
  (let [dataset (ds/->train-test-split (mapseq-fruit-dataset) {})]
    (is (= [7 41]
           (dtype/shape (:train-ds dataset))))
    (is (= [7 18]
           (dtype/shape (:test-ds dataset))))))



(deftest tensor-and-back
  (let [test-tensor (tens/->tensor (->> (range 25)
                                        shuffle
                                        (partition 5)))
        ds (ds-tens/row-major-tensor->dataset test-tensor)

        ;; _ (println test-tensor)
        ;; _ (clojure.pprint/print-table (ds/->flyweight ds))

        result-tens (ds-tens/dataset->row-major-tensor ds :float64)]
    (is (= (tens/->jvm test-tensor :datatype :int32)
           (tens/->jvm result-tens :datatype :int32)))))


(deftest pca
  (let [test-data (tens/->tensor (->> (range 25)
                                    shuffle
                                    (partition 5)))
        test-ds (ds-tens/row-major-tensor->dataset test-data)
        pca-info (pca/pca-dataset test-ds)
        transformed-ds (pca/pca-transform-dataset test-ds pca-info 3 :float64)
        trans-tens (ds-tens/dataset->row-major-tensor transformed-ds :float64)
        smile-svd-pca (doto (PCA/fit (->> test-data
                                          tens/rows
                                          (map dtype/->array-copy)
                                          (into-array (Class/forName "[D"))))
                        (.setProjection (int 3)))
        smile-transformed-ds (-> (.project smile-svd-pca
                                           (->> test-data
                                                (tens/rows)
                                                (map dtype/->array-copy)
                                                (into-array (Class/forName "[D"))))
                                 (tens/->tensor))]
    ;;Make sure we get the same answer as smile.
    (is (dfn/equals trans-tens
                         smile-transformed-ds
                         0.01))))


(deftest n-permutations
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/set-inference-target :fruit-name))]
    (is (= 35
           (count (ds/n-permutations 3 ds))))
    (is (= 20
           (count (ds/n-feature-permutations 3 ds))))
    ;;The label column shows up in every permutation.
    (is (every? :fruit-name
                (->> (ds/n-feature-permutations 3 ds)
                     (map (comp set ds/column-names)))))))


(deftest iterable
  (let [ds (ds/->dataset (mapseq-fruit-dataset))]
    (is (= (ds/column-names ds)
           (map ds-col/column-name ds)))))


(deftest string-column-add-or-update
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/update-column :fruit-name
                                      #(->> (dtype/->reader %)
                                            (unary-op/unary-reader
                                             :string
                                             (.concat ^String (name x) "-fn")))))]
    (is (= ["apple-fn" "apple-fn" "apple-fn" "mandarin-fn" "mandarin-fn"
            "mandarin-fn" "mandarin-fn" "mandarin-fn" "apple-fn" "apple-fn"]
           (->> (ds :fruit-name)
                (dtype/->reader)
                (take 10)
                vec)))))


(deftest name-values-seq->dataset-test

  (is (= [{:a 0.0, :b "a"} {:a 1.0, :b "b"} {:a 2.0, :b "c"}
          {:a 3.0, :b "d"} {:a 4.0, :b "e"}]
         (-> (ds/name-values-seq->dataset
              {:a (double-array (range 5))
               :b ["a" "b" "c" "d" "e"]})
             (ds/mapseq-reader))))

  (is (= #{4}
         (-> (ds/name-values-seq->dataset
              {:a (double-array (range 5))
               :b ["a" "b" "c" "d"]})
             (ds/missing)
             (set))))

  (is (= [{:a 0, :b "a"} {:a 1, :b "b"} {:a 2, :b "c"}
          {:a 3, :b "d"} {:a 4, :b "e"}]
         (-> (ds/name-values-seq->dataset
              {:a (long-array (range 5))
               :b ["a" "b" "c" "d" "e"]})
             (ds/mapseq-reader)))))


(deftest unique-by-test
  (let [ds (ds/->dataset (mapseq-fruit-dataset))]
    (is (= [7 4]
           (dtype/shape (ds/unique-by :fruit-name ds))))
    (is (= [7 4]
           (dtype/shape (ds/unique-by-column :fruit-name ds))))
    (is (= #{:apple :orange :lemon :mandarin}
           (->> (ds/column (ds/unique-by-column :fruit-name ds)
                                :fruit-name)
                set)))

    (is (= [7 24]
           (dtype/shape (ds/unique-by :width ds))))
    (is (= [7 24]
           (dtype/shape (ds/unique-by-column :width ds))))
    (is (dfn/equals [5.8 5.9 6.0 6.1 6.2 6.3 6.5
                     6.7 6.8 6.9 7.0 7.1 7.2 7.3
                     7.4 7.5 7.6 7.7 7.8 8.0 8.4
                     9.0 9.2 9.6]
                    (->> (ds/column (ds/unique-by-column :width ds)
                                    :width)
                         sort
                         vec)))))


(deftest ds-concat-nil-pun
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/select :all (range 10)))
        d1 (ds/ds-concat nil ds)
        d2 (ds/ds-concat ds nil nil)
        nothing (ds/ds-concat nil nil nil)]
    (is (= (vec (ds :fruit-name))
           (vec (d1 :fruit-name))))
    (is (= (vec (ds :fruit-name))
           (vec (d2 :fruit-name))))
    (is (nil? nothing))))


(deftest ds-concat-missing
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/select [:fruit-name] (range 10))
               (ds/update-column :fruit-name #(ds-col/set-missing % [3 6])))
        d1 (ds/ds-concat ds ds)]
    (is (= (set [3 6 13 16]) (set (ds-col/missing (d1 :fruit-name)))))
    (is (= [:apple :apple :apple nil :mandarin
            :mandarin nil :mandarin :apple :apple
            :apple :apple :apple nil :mandarin
            :mandarin nil :mandarin :apple :apple ]
           (vec (d1 :fruit-name))))))


(deftest update-column-datatype-detect
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/select :all (range 10)))
        updated (ds/update-column ds :width #(->> %
                                                       (map (fn [data]
                                                              (* 10 data)))))
        add-or-updated (ds/add-or-update-column
                        ds :width (->> (ds :width)
                                       (map (fn [data]
                                              (* 10 data)))))
        width-answer (->> (ds :width)
                          (mapv (fn [data]
                                  (* 10 data))))]

    (is (dfn/equals width-answer
                    (updated :width)))
    (is (dfn/equals width-answer
                    (add-or-updated :width)))))


(deftest filter-fail-regression
  (let [ds (ds/->dataset (mapseq-fruit-dataset))]
    (is (= [:mandarin :mandarin :mandarin :mandarin]
           (vec (dtype/sub-buffer (ds :fruit-name) 4 4))))))


(deftest remove-missing-persistent-vec-data
  (let [ds (ds/name-values-seq->dataset {:a [1 nil 2 nil 3]
                                              :b (list 1 nil 2 nil 3)})
        rec (ds-pipe/replace-missing ds :all 5)]
    (is (= #{:int64}
           (set (map dtype/get-datatype ds))))
    (is (= [1 5 2 5 3]
           (vec (rec :a))))
    (is (= [1 5 2 5 3]
           (vec (rec :b))))))


(deftest simple-select-test
  (let [ds (ds/->dataset (mapseq-fruit-dataset))
        sel-col (ds-col/select (ds :fruit-name) (range 5 10))]
    (is (= [:mandarin :mandarin :mandarin :apple :apple]
           (vec sel-col)))
    (is (= 5 (dtype/ecount sel-col)))))


(deftest generic-sort-numbers
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/->sort-by-column :mass >))
        ds2 (-> (ds/->dataset (mapseq-fruit-dataset))
                (ds/->sort-by-column :mass))]

    (is (= (vec (take 10 (ds :mass)))
           (vec (take 10 (reverse (ds2 :mass))))))))


(deftest selection-map
  (let [ds (ds/->dataset (mapseq-fruit-dataset))
        colname-map (->> (take 3 (ds/column-names ds))
                         (map (juxt identity #(keyword (str (name %) "-selected"))))
                         (into {}))

        ;;Enforce the order in the map.
        ordered-ds (ds/select-columns ds colname-map)
        ;;ensure normal ordering rules apply, make a dataset with random column
        ;;name order
        shuffled-ds (-> (ds/select-columns ds (shuffle (ds/column-names ds)))
                        (ds/select-columns colname-map))
        shuffled-unordered (-> (ds/select-columns ds (reverse (ds/column-names ds)))
                               (ds/unordered-select colname-map :all))
        colname-vals (vec (vals colname-map))]
    (is (= colname-vals
           (vec (ds/column-names ordered-ds))))
    (is (= colname-vals
           (vec (ds/column-names shuffled-ds))))
    (is (not= colname-vals
              (vec (ds/column-names shuffled-unordered))))))


(deftest boolean-double-arrays
  (let [d (ds/->dataset [{:a true} {:a true} {:a false}])]
    (is (= [1.0 1.0 0.0]
           (vec (ds-col/to-double-array (d :a)))))))


(deftest remove-rows
  (let [d (ds/->dataset (mapseq-fruit-dataset))
        d2 (ds/remove-rows d (range 5))]
    (is (= (vec (drop 5 (d :fruit-name)))
           (vec (d2 :fruit-name))))))


(deftest long-double-promotion
  (is (= #{:float64}
         (->> (ds/->dataset [{:a 1 :b (float 2.2)} {:a 1.2 :b 2}])
              (map dtype/get-datatype)
              set))))


(deftest set-missing-range
  (let [ds (-> (ds/->dataset (mapseq-fruit-dataset))
               (ds/update-column :fruit-name #(ds-col/set-missing % (range))))]
    (is (= (vec (range (ds/row-count ds)))
           (vec (dtype/->reader (ds-col/missing (ds :fruit-name))))))))


(deftest columnwise-concat
  (let [ds (-> [{:a 1 :b 2 :c 3 :d 1} {:a 4 :b 5 :c 6 :d 2}]
               (ds/->dataset)
               (ds/columnwise-concat [:c :a :b]))]
    (is (= (vec [:c :c :a :a :b :b])
           (vec (ds :column))))
    (is (= (vec [3 6 1 4 2 5])
           (vec (ds :value))))
    (is (= (vec [1 2 1 2 1 2])
           (vec (ds :d))))))


(deftest default-names
  (is (= "test/data/stocks.csv"
         (ds/dataset-name (ds/->dataset "test/data/stocks.csv"))))
  (is (= "stocks"
         (ds/dataset-name (ds/->dataset "test/data/stocks.xlsx")))))


(deftest unroll
  (let [ds (-> (ds/->dataset [{:a 1 :b [2 3]}
                              {:a 2 :b [4 5]}
                              {:a 3 :b :a}])
               (ds/unroll-column :b))]
    (is (= [1 1 2 2 3]
           (vec (ds :a))))
    (is (= [2 3 4 5 :a]
           (vec (ds :b)))))
  (let [ds (-> (ds/->dataset (flatten (repeat 20
                                              [{:a 1 :b [:a :b]}
                                               {:a 2 :b [:c :d]}
                                               {:a 3 :b :a}])))
               (ds/unroll-column :b {:datatype :keyword}))]
    (is (= (flatten (repeat 20 [1 1 2 2 3]))
           (vec (ds :a))))
    (is (= (flatten (repeat 20 [:a :b :c :d :a]))
           (vec (ds :b)))))
  (let [ds (-> (ds/->dataset [{:a 1 :b [2 3]}
                              {:a 2 :b [4 5]}
                              {:a 3 :b :a}])
               (ds/unroll-column :b {:indexes? true}))]
    (is (= [1 1 2 2 3]
           (vec (ds :a))))
    (is (= [2 3 4 5 :a]
           (vec (ds :b))))
    (is (= [0 1 0 1 0]
           (vec (ds :indexes)))))
  (let [ds (-> (ds/->dataset [{:a 1 :b (int-array [2 3])}
                              {:a 2 :b [4 5]}
                              {:a 3 :b :a}])
               (ds/unroll-column :b {:indexes? :unroll-indexes}))]
    (is (= [1 1 2 2 3]
           (vec (ds :a))))
    (is (= [2 3 4 5 :a]
           (vec (ds :b))))
    (is (= [0 1 0 1 0]
           (vec (ds :unroll-indexes))))))


(deftest empty-bitmap
  (let [ds (ds/->dataset [{:a 1 :b 1} {:a 2 :b 2}])]
    (is (= 0 (ds/row-count (ds/select-rows ds (ds/missing ds))))))
  (let [ds (ds/->dataset [{:a 1 :b 1} {:b 2}])]
    (is (= 1 (ds/row-count (ds/select-rows ds (ds/missing ds)))))))


(deftest concat-columns-widening
  (let [ds (ds/->dataset [{:a (int 1) :b (float 1)}])
        ds2 (ds/->dataset [{:a (byte 2) :b 2}])
        cds1 (ds/concat ds ds2)
        cds2 (ds/concat ds2 ds)]
    (is (= #{:int32 :float64}
           (set (map dtype/get-datatype cds1))))
    (is (= #{:int32 :float64}
           (set (map dtype/get-datatype cds2)))))
  (let [ds (ds/->dataset [{:a (int 1) :b (float 1)}
                          {:b (float 2)}])
        ds2 (ds/->dataset [{:a (byte 2) :b 2}])
        cds1 (ds/concat ds ds2)
        cds2 (ds/concat ds2 ds)]
    (is (= #{:int32 :float64}
           (set (map dtype/get-datatype cds1))))
    (is (= #{:int32 :float64}
           (set (map dtype/get-datatype cds2))))
    (is (= [1 nil 2]
           (vec (cds1 :a))))))


(deftest concat-columns-various-datatypes
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        ds1 (ds/select-rows stocks (range 10))
        ds2 (ds/select-rows stocks (range 10 20))
        res (ds/concat ds1 ds2)]
    (is (= :packed-local-date
           (dtype/get-datatype (res "date")))))
  (let [ds (ds/->dataset [{:a "a" :b 0}])
        res (ds/concat ds ds)]
    (is (= :string (dtype/get-datatype (res :a))))))


(deftest set-datatype-lose-missing
  (let [ds (-> (ds/->dataset [{:a 1 :b 1} {:b 2}])
               (ds/update-column :a #(dtype/set-datatype % :int32)))]
    (is (== 1 (dtype/ecount (ds-col/missing (ds :a)))))
    (is (= :int32 (dtype/get-datatype (ds :a))))
    (is (= [1 nil]
           (vec (ds :a))))))


(deftest set-datatype-with-new-column
  (let [ds (-> (ds/->dataset [{:a 1 :b 1} {:b 2}])
               (ds/update-column :a #(ds-col/new-column
                                      (ds-col/column-name %)
                                      (let [src-rdr (dtype/->typed-reader % :int32)]
                                        (dtype/make-reader :int32
                                                           (.lsize src-rdr)
                                                           (.read src-rdr idx)))
                                      {}
                                      (ds-col/missing %))))]
    (is (== 1 (dtype/ecount (ds-col/missing (ds :a)))))
    (is (= :int32 (dtype/get-datatype (ds :a))))
    (is (= [1 nil]
           (vec (ds :a))))))


(deftest typed-column-map
  (let [ds (-> (ds/->dataset [{:a 1.0} {:a 2.0}])
               (ds/update-column
                :a
                #(dtype/typed-reader-map (fn ^double [^double in]
                                           (if (< in 2.0) (- in) in))
                                         %)))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [-1.0 2.0]
           (vec (ds :a))))))


(deftest typed-column-map-missing
  (let [ds (-> (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}])
               (ds/column-map
                :a
                (fn ^double [^double lhs ^double rhs]
                  (+ lhs rhs))
                :a :b))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/is-finite? (ds :a)))))))


(deftest mean-object-column
  (let [ds (-> (ds/->dataset [])
               (ds/add-or-update-column :a (map (fn [arg] (* 2 arg)) (range 9))))]
    (is (= :object (dtype/get-datatype (ds :a))))
    (is (= 8.0 (dfn/mean (ds :a))))))


(deftest column-cast-test
  (let [ds (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        price-dtype (dtype/get-datatype (ds :price))
        _ (is (dfn/equals (ds :price)
                          (-> (ds/column-cast ds :price :string)
                              (ds/column-cast :price price-dtype)
                              (ds/column :price))))
        date-dtype (dtype/get-datatype (ds :date))
        _ (is (dfn/equals (ds :date)
                          (-> (ds/column-cast ds :date :string)
                              (ds/column-cast :date date-dtype)
                              (ds/column :date))))]
    ;;Custom cast fn
    (is (= [40 36 43 28 25]
           (->> (ds/column-cast ds :price [:int32 #(Math/round (double %))])
                (#(ds/column % :price))
                (take 5)
                (vec))))))


(deftest column-clone-double-read
  (let [ds (ds/->dataset "test/data/stocks.csv"
                         {:key-fn keyword})
        read-indexes (HashSet.)
        new-ds (ds/assoc ds
                         :price-2
                         (dtype/clone
                          (dtype/make-reader
                           :boolean
                           (ds/row-count ds)
                           (do
                             (locking read-indexes
                               (when (.contains read-indexes idx)
                                 (throw (Exception. "Double read!!")))
                               (.add read-indexes idx))
                             true))))]
    (is (= [true true true true true]
           (vec (take 5 (new-ds :price-2)))))))


(deftest stats-with-missing
  (let [DSm2 (ds/name-values-seq->dataset {:a [nil nil nil 1 2 nil 3
                                               4 nil nil nil 11 nil]
                                           :b [nil 2   2   2 2 3   nil 3 nil
                                               3   nil   4  nil]})]
    (is (> (:mean (ds-col/stats (DSm2 :a) #{:mean})) 0.0))
    (is (> (:mean (ds-col/stats (DSm2 :b) #{:mean})) 0.0))))


(deftest uuids-test
  (let [uuids (repeatedly 5 #(UUID/randomUUID))
        ds (ds/->dataset
            (->> uuids
                 (map-indexed (fn [idx uuid]
                                {:a uuid
                                 :b (str uuid)
                                 :c idx}))))]
    (is (= :uuid (dtype/get-datatype (ds :a))))
    (is (= :uuid (dtype/get-datatype (ds :b))))
    (is (= (vec uuids)
           (vec (ds :a))))
    (let [test-fname (str (UUID/randomUUID) ".csv")
          _ (ds/write-csv! ds test-fname)
          loaded-ds (try (ds/->dataset test-fname
                                       {:key-fn keyword})
                         (finally
                           (.delete (File. test-fname))))]
      (is (= (vec (ds :a))
             (vec (loaded-ds :a)))))))


(deftest filter-empty
  (let [ds (ds/name-values-seq->dataset {:V1 (take 9 (cycle [1 2]))
                                         :V2 (range 1 10)
                                         :V3 (take 9 (cycle [0.5 1.0 1.5]))
                                         :V4 (take 9 (cycle [\A \B \C]))})
        result (ds/filter (constantly false) ds)]
    (is (= 0 (ds/row-count result)))
    (is (= (ds/column-count ds)
           (ds/column-count result)))
    (is (string? (.toString result)))))


(deftest nil-mapseq-values
  (let [ds (ds/->dataset [{:a nil} {:a 1} {}])]
    (is (= #{0 2}
           (set (ds/missing ds))))
    (is (= [nil 1 nil]
           (vec (dtype/->reader (ds :a) :object))))))


(deftest select-row
  (let [ds (ds/name-values-seq->dataset {:V1 (take 9 (cycle [1 2]))
                                         :V2 (range 1 10)
                                         :V3 (take 9 (cycle [0.5 1.0 1.5]))
                                         :V4 (take 9 (cycle [\A \B \C]))})]

    (is (= [2 6 1.5 \C]
           (-> (ds/select-rows ds 5)
               (ds/value-reader)
               (first)
               (vec))))

    (is (= [2 6 1.5 \C]
           (-> (ds/select-rows ds [5])
               (ds/value-reader)
               (first)
               (vec))))))


(deftest columns-named-false
  (let [DS (ds/->dataset [{false 1} {false 2}])]
    (is (= [1 2]
           (vec (DS false)))))
  (let [DS (ds/->dataset [{:a 1} {:a 2}])]
    (is (= [1 2]
           (-> (ds/rename-columns DS {:a false})
               (ds/column false)
               vec))))
  (let [DS (ds/->dataset [{:a 1} {:a 2}])]
    (is (= [1 2]
           (-> (ds/select-columns DS {:a false})
               (ds/column false)
               vec)))))


(deftest column-sequences-use-nil-missing
  (let [ds (ds/->dataset [{:a 1} {:b 2}])]
    (is (= [1 nil] (vec (ds :a))))
    (is (= [nil 2] (vec (ds :b))))))


(deftest ->dataset-nvs-parse-test
  (let [ds (ds/->dataset {:a [1 2 3]
                          :b [4 5 6]})]
    (is (= [1 2 3]
           (vec (ds :a))))
    (is (= [4 5 6]
           (vec (ds :b))))))



(comment

  (def test-ds (ds/->dataset
                "https://github.com/genmeblog/techtest/raw/master/data/who.csv.gz"))

  (->> '("new_sp_m014" "new_sp_m1524")
       (ds/select-columns test-ds)
       (ds/columns)
       (map (comp :datatype meta)))

  (ds/columnwise-concat test-ds '("new_sp_m014" "new_sp_m1524"))
  (ds/columnwise-concat test-ds '("new_sp_m1524" "new_sp_m014"))

  )
