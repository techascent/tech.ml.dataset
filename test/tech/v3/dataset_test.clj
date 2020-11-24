(ns tech.v3.dataset-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.join :as ds-join]
            [tech.v3.dataset.test-utils :as test-utils]
            ;;Loading multimethods required to load the files
            [tech.v3.libs.poi]
            [tech.v3.libs.fastexcel]
            [tech.v3.io :as tech-io]
            [taoensso.nippy :as nippy]
            [clojure.test :refer [deftest is]])
  (:import [java.util List HashSet UUID]
           [java.io File]))

(deftest datatype-parser
  (let [ds (ds/->dataset "test/data/datatype_parser.csv")]
    (is (= :int16 (dtype/get-datatype (ds/column ds "id"))))
    (is (= [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] (ds/column ds "id")))
    (is (= :string (dtype/get-datatype (ds/column ds "char"))))
    (is (= ["t", "f", "y", "n", "T", "F", "Y", "N", "A", "z"]
           (ds/column ds "char")))
    (is (= :string (dtype/get-datatype (ds/column ds "word"))))
    (is (= ["true", "False", "YES", "NO", "positive", "negative", "yep", "not", "pos", "neg"]
           (ds/column ds "word")))
    (is (= :boolean (dtype/get-datatype (ds/column ds "bool"))))
    (is (= [true, true, false, false, true, false, true, false, false, false]
           (ds/column ds "bool")))
    (is (= :string (dtype/get-datatype (ds/column ds "boolstr"))))
    (is (= ["true", "true", "false", "false", "true", "false", "true", "false", "False", "false"]
           (ds/column ds "boolstr")))
    (is (= :string (dtype/get-datatype (ds/column ds "boolean"))))
    (is (= ["t", "y", "n", "f", "true", "false", "positive", "negative", "negative", "negative"]
           (ds/column ds "boolean"))))
  (let [ds (ds/->dataset "test/data/datatype_parser.csv" {:parser-fn {"boolean" :boolean
                                                                      "boolstr" :boolean}})]
    (is (= :int16 (dtype/get-datatype (ds/column ds "id"))))
    (is (= [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] (ds/column ds "id")))
    (is (= :string (dtype/get-datatype (ds/column ds "char"))))
    (is (= ["t", "f", "y", "n", "T", "F", "Y", "N", "A", "z"]
           (ds/column ds "char")))
    (is (= :string (dtype/get-datatype (ds/column ds "word"))))
    (is (= ["true", "False", "YES", "NO", "positive", "negative", "yep", "not", "pos", "neg"]
           (ds/column ds "word")))
    (is (= :boolean (dtype/get-datatype (ds/column ds "bool"))))
    (is (= [true, true, false, false, true, false, true, false, false, false]
           (ds/column ds "boolean")))
    (is (= :boolean (dtype/get-datatype (ds/column ds "boolstr"))))
    (is (= [true, true, false, false, true, false, true, false, false, false]
           (ds/column ds "boolstr")))
    (is (= :boolean (dtype/get-datatype (ds/column ds "boolean"))))
    (is (= [true, true, false, false, true, false, true, false, false, false]
           (ds/column ds "boolean")))))

(deftest iterable
  (let [ds (ds/->dataset (test-utils/mapseq-fruit-dataset))]
    (is (= (ds/column-names ds)
           (map ds-col/column-name (vals ds))))))


(deftest string-column-add-or-update
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
               (ds/update-column :fruit-name (partial dtype/emap #(str (name %) "-fn") :string)))]
    (is (= ["apple-fn" "apple-fn" "apple-fn" "mandarin-fn" "mandarin-fn"
            "mandarin-fn" "mandarin-fn" "mandarin-fn" "apple-fn" "apple-fn"]
           (->> (ds :fruit-name)
                (take 10)
                vec)))))


(deftest name-values-seq->dataset-test
  (is (= [{:a 0.0, :b "a"} {:a 1.0, :b "b"} {:a 2.0, :b "c"}
          {:a 3.0, :b "d"} {:a 4.0, :b "e"}]
         (-> (ds/->dataset
              {:a (double-array (range 5))
               :b ["a" "b" "c" "d" "e"]})
             (ds/mapseq-reader))))

  (is (= #{4}
         (-> (ds/->dataset
              {:a (double-array (range 5))
               :b ["a" "b" "c" "d"]})
             (ds/missing)
             (set))))

  (is (= [{:a 0, :b "a"} {:a 1, :b "b"} {:a 2, :b "c"}
          {:a 3, :b "d"} {:a 4, :b "e"}]
         (-> (ds/->dataset
              {:a (long-array (range 5))
               :b ["a" "b" "c" "d" "e"]})
             (ds/mapseq-reader)))))


(deftest unique-by-test
  (let [ds (test-utils/mapseq-fruit-dataset)]
    (is (= [7 4]
           (dtype/shape (ds/unique-by ds :fruit-name))))
    (is (= [7 4]
           (dtype/shape (ds/unique-by-column ds :fruit-name))))
    (is (= #{:apple :orange :lemon :mandarin}
           (-> (ds/column (ds/unique-by-column ds :fruit-name)
                          :fruit-name)
               set)))

    (is (= [7 24]
           (dtype/shape (ds/unique-by ds :width))))
    (is (= [7 24]
           (dtype/shape (ds/unique-by-column ds :width))))
    (is (dfn/equals [5.8 5.9 6.0 6.1 6.2 6.3 6.5
                     6.7 6.8 6.9 7.0 7.1 7.2 7.3
                     7.4 7.5 7.6 7.7 7.8 8.0 8.4
                     9.0 9.2 9.6]
                    (->> (ds/column (ds/unique-by-column ds :width)
                                    :width)
                         sort
                         vec)))))


(deftest ds-concat-nil-pun
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
               (ds/select :all (range 10)))
        d1 (ds/concat nil ds)
        d2 (ds/concat ds nil nil)
        nothing (ds/concat nil nil nil)]
    (is (= (vec (ds :fruit-name))
           (vec (d1 :fruit-name))))
    (is (= (vec (ds :fruit-name))
           (vec (d2 :fruit-name))))
    (is (nil? nothing))))


(deftest ds-concat-copying-nil-pun
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
               (ds/select :all (range 10)))
        d1 (ds/concat-copying nil ds)
        d2 (ds/concat-copying ds nil nil)
        nothing (ds/concat nil nil nil)]
    (is (= (vec (ds :fruit-name))
           (vec (d1 :fruit-name))))
    (is (= (vec (ds :fruit-name))
           (vec (d2 :fruit-name))))
    (is (nil? nothing))))


(deftest ds-concat-missing
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
               (ds/select [:fruit-name] (range 10))
               (ds/update-column :fruit-name #(ds-col/set-missing % [3 6])))
        d1 (ds/concat ds ds)]
    (is (= (set [3 6 13 16]) (set (ds-col/missing (d1 :fruit-name)))))
    (is (= [:apple :apple :apple nil :mandarin
            :mandarin nil :mandarin :apple :apple
            :apple :apple :apple nil :mandarin
            :mandarin nil :mandarin :apple :apple ]
           (vec (d1 :fruit-name))))))


(deftest concat-copying-missing
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
               (ds/select [:fruit-name] (range 10))
               (ds/update-column :fruit-name #(ds-col/set-missing % [3 6])))
        d1 (ds/concat-copying ds ds)]
    (is (= (set [3 6 13 16]) (set (ds-col/missing (d1 :fruit-name)))))
    (is (= [:apple :apple :apple nil :mandarin
            :mandarin nil :mandarin :apple :apple
            :apple :apple :apple nil :mandarin
            :mandarin nil :mandarin :apple :apple ]
           (vec (d1 :fruit-name))))))


(deftest update-column-datatype-detect
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
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
  (let [ds (ds/->dataset (test-utils/mapseq-fruit-dataset))]
    (is (= [:mandarin :mandarin :mandarin :mandarin]
           (vec (dtype/sub-buffer (ds :fruit-name) 4 4))))))



(deftest simple-select-test
  (let [ds (ds/->dataset (test-utils/mapseq-fruit-dataset))
        sel-col (ds-col/select (ds :fruit-name) (range 5 10))]
    (is (= [:mandarin :mandarin :mandarin :apple :apple]
           (vec sel-col)))
    (is (= 5 (dtype/ecount sel-col)))))


(deftest generic-sort-numbers
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
               (ds/sort-by-column :mass >))
        ds2 (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
                (ds/sort-by-column :mass))]

    (is (= (vec (take 10 (ds :mass)))
           (vec (take 10 (reverse (ds2 :mass))))))))


(deftest selection-map
  (let [ds (ds/->dataset (test-utils/mapseq-fruit-dataset))
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
  (let [d (ds/->dataset (test-utils/mapseq-fruit-dataset))
        d2 (ds/remove-rows d (range 5))]
    (is (= (vec (drop 5 (d :fruit-name)))
           (vec (d2 :fruit-name))))))


(deftest long-double-promotion
  (is (= #{:float64}
         (->> (ds/->dataset [{:a 1 :b (float 2.2)} {:a 1.2 :b 2}])
              (vals)
              (map dtype/get-datatype)
              set))))


(deftest set-missing-range
  (let [ds (-> (ds/->dataset (test-utils/mapseq-fruit-dataset))
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
  (is (= "test/data/stocks.xlsx"
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
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds1)))))
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds2))))))
  (let [ds (ds/->dataset [{:a (int 1) :b (float 1)}
                          {:b (float 2)}])
        ds2 (ds/->dataset [{:a (byte 2) :b 2}])
        cds1 (ds/concat ds ds2)
        cds2 (ds/concat ds2 ds)]
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds1)))))
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds2)))))
    (is (= [1 nil 2]
           (vec (cds1 :a))))))


(deftest concat-copying-columns-widening
  (let [ds (ds/->dataset [{:a (int 1) :b (float 1)}])
        ds2 (ds/->dataset [{:a (byte 2) :b 2}])
        cds1 (ds/concat ds ds2)
        cds2 (ds/concat ds2 ds)]
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds1)))))
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds2))))))
  (let [ds (ds/->dataset [{:a (int 1) :b (float 1)}
                          {:b (float 2)}])
        ds2 (ds/->dataset [{:a (byte 2) :b 2}])
        cds1 (ds/concat-copying ds ds2)
        cds2 (ds/concat-copying ds2 ds)]
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds1)))))
    (is (= #{:int64 :float64}
           (set (map dtype/get-datatype (vals cds2)))))
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


(deftest concat-copying-columns-various-datatypes
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        ds1 (ds/select-rows stocks (range 10))
        ds2 (ds/select-rows stocks (range 10 20))
        res (ds/concat ds1 ds2)]
    (is (= :packed-local-date
           (dtype/get-datatype (res "date")))))
  (let [ds (ds/->dataset [{:a "a" :b 0}])
        res (ds/concat-copying ds ds)]
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
                                      (dtype/emap int :int32 %)
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
                #(dtype/emap (fn ^double [^double in]
                               (if (< in 2.0) (- in) in))
                             :float64
                             %)))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [-1.0 2.0]
           (vec (ds :a))))))


(deftest typed-column-map-missing
  (let [ds (ds/bind-> (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}]) ds
             (assoc :a
                    (ds-col/column-map (fn ^double [^double lhs ^double rhs]
                                         (+ lhs rhs))
                                       :float64
                                       (ds :a) (ds :b))))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/finite? (ds :a)))))))


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
        _ (is (dfn/equals (dtype-dt/datetime->milliseconds (ds :date))
                          (-> (ds/column-cast ds :date :string)
                              (ds/column-cast :date date-dtype)
                              (ds/column :date)
                              (dtype-dt/datetime->milliseconds))))]
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
        new-ds (assoc ds
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
  (let [DSm2 (ds/->dataset {:a [nil nil nil 1 2 nil 3
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
                                 :b uuid
                                 :c idx}))))]
    (is (= :uuid (dtype/get-datatype (ds :a))))
    (is (= :uuid (dtype/get-datatype (ds :b))))
    (is (= (vec uuids)
           (vec (ds :a))))
    (let [test-fname (str (UUID/randomUUID) ".csv")
          _ (ds/write! ds test-fname)
          loaded-ds (try (ds/->dataset test-fname
                                       {:key-fn keyword})
                         (finally
                           (.delete (File. test-fname))))]
      (is (= (vec (ds :a))
             (vec (loaded-ds :a)))))))


(deftest filter-empty
  (let [ds (ds/->dataset {:V1 (take 9 (cycle [1 2]))
                          :V2 (range 1 10)
                          :V3 (take 9 (cycle [0.5 1.0 1.5]))
                          :V4 (take 9 (cycle [\A \B \C]))})
        result (ds/filter ds (constantly false))]
    (is (= 0 (ds/row-count result)))
    (is (= (ds/column-count ds)
           (ds/column-count result)))
    (is (string? (.toString ^Object result)))))


(deftest nil-mapseq-values
  (let [ds (ds/->dataset [{:a nil} {:a 1} {}])]
    (is (= #{0 2}
           (set (ds/missing ds))))
    (is (= [nil 1 nil]
           (vec (dtype/->reader (ds :a)))))))


(deftest select-row
  (let [ds (ds/->dataset {:V1 (take 9 (cycle [1 2]))
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


(deftest apply-works-with-columns-and-vectors
  (let [ds (ds/->dataset {:a [1 2 3]
                          :b [4 5 6]})
        a-col (ds :a)]
    (is (= 2 (apply a-col [1])))
    (is (= 2 (apply (dtype/->reader a-col) [1])))))


(deftest vector-of-test
  (let [ds (ds/->dataset {:a (vector-of :float 1 2 3 4)
                          :b (vector-of :short 1 2 3 4)})]
    (is (= #{:float32 :int16}
           (set (map dtype/get-datatype (vals ds)))))
    (let [cds (dtype/clone ds)]
      (is (every? #(not (nil? %))
                  (map dtype/->array (vals cds)))))))


(deftest serialize-datetime
  (let [ds (ds/->dataset "test/data/stocks.csv")
        _ (ds/write! ds "test.tsv.gz")
        save-ds (ds/->dataset "test.tsv.gz")
        fdata (java.io.File. "test.tsv.gz")]
    (is (= (ds/row-count ds) (ds/row-count save-ds)))
    (is (= (ds/column-count ds) (ds/column-count save-ds)))
    (is (= (set (map dtype/get-datatype ds))
           (set (map dtype/get-datatype save-ds))))
    (when (.exists fdata)
      (.delete fdata))))


(deftest custom-packed-local-date-parser
  (let [ds (ds/->dataset "test/data/stocks.csv"
                         {:parser-fn {"date" [:packed-local-date
                                              "MMM d yyyy"]}})]
    (is (= 560 (ds/row-count ds)))))


(deftest stocks-to-from-nippy
  (let [fname (format "%s.nippy" (java.util.UUID/randomUUID))]
    (try
      (let [stocks (ds/->dataset "test/data/stocks.csv")
            _ (tech-io/put-nippy! fname stocks)
            nip-stocks (tech-io/get-nippy fname)]
        (is (= (ds/row-count stocks) (ds/row-count nip-stocks)))
        (is (= (ds/column-count stocks) (ds/column-count nip-stocks)))
        (is (= (vec (stocks "date"))
               (vec (nip-stocks "date"))))
        (is (= (mapv meta (vals stocks))
               (mapv meta (vals nip-stocks)))))
      (finally
        (let [file (java.io.File. fname)]
          (when (.exists file)
            (.delete file)))))))


(deftest empty-dataset-hasheq
  (let [ds (ds/->dataset [])]
    (is (== 0 (.hashCode ds)))))


(deftest columns-are-persistent-vectors
  (let [ds (-> (ds/->dataset "test/data/stocks.csv")
               (ds/head))
        sym-vec (vec (ds "symbol"))]
    ;;We use a clever impl of APersistentVector for the columns
    (is (= sym-vec (ds "symbol")))))


(deftest replace-missing-test
  (let [ds (ds/->dataset {:a [nil nil nil 1.0 2  nil nil nil
                              nil  nil 4   nil  11 nil nil]
                          :b [2   2   2 nil nil nil nil nil
                              nil 13   nil   3  4  5 5]})]
    (is (= [1.0 1.0 1.0 1.0 2.0 2.0 2.0 2.0 2.0 2.0 4.0 4.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :down) :a))))
    (is (= [1.0 1.0 1.0 1.0 2.0 4.0 4.0 4.0 4.0 4.0 4.0 11.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :up) :a))))
    (is (= [1.0 1.0 1.0 1.0 2.0 2.0 2.0 2.0 4.0 4.0 4.0 4.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :mid) :a))))
    (is (= [5.0 5.0 5.0 1.0 2.0 5.0 5.0 5.0 5.0 5.0 4.0 5.0 11.0 5.0 5.0]
           (vec ((ds/replace-missing ds :all :value 5.0) :a))))))


(deftest replace-missing-all-values-missing
  (let [empty-col (ds/->dataset {:a [nil nil]})]
    (is (= 2 (-> empty-col
                 (ds/replace-missing [:a] :value dfn/mean)
                 (ds/missing)
                 (dtype/ecount))))))


(deftest replace-missing-ldt
  (let [dtds (ds/->dataset {:dt [(java.time.LocalDateTime/of 2020 1 1 1 1 1)
                                 nil nil nil
                                 (java.time.LocalDateTime/of 2020 10 1 1 1 1)]})]
    (is (= (seq ((ds/replace-missing dtds :lerp) :dt))
           [(java.time.LocalDateTime/of 2020 1 1 1 1 1)
            (java.time.LocalDateTime/of 2020 3 9 13 1 1)
            (java.time.LocalDateTime/of 2020 5 17 1 1 1)
            (java.time.LocalDateTime/of 2020 7 24 13 1 1)
            (java.time.LocalDateTime/of 2020 10 1 1 1 1)]))))


(deftest dataset-column-nippy
  (let [ds (ds/->dataset {:a [1 2]
                          :datasets [(ds/->dataset [{:a 1}])
                                     (ds/->dataset [{:b 2}])]})
        nippy-data (nippy/freeze ds)
        thawed-ds (nippy/thaw nippy-data)]
    (is (= (map meta (vals ds))
           (map meta (vals thawed-ds))))
    (is (= ds thawed-ds))))


(deftest unique-by-nil-regression
  (-> (ds/->dataset [])
      (ds/add-column (ds-col/new-column :abc [nil nil]))
      (ds/unique-by-column :abc)))


(deftest missing-values-and-tensors
  (let [ds (ds/->dataset {:a [1 nil 2]
                          :b [1.0 nil 2.0]
                          :c [5 nil 6]})]
    (is (= 3
           (->> (ds-tens/dataset->tensor ds :float64)
                (dtype/->reader)
                (filter #(Double/isNaN %))
                (count))))))


(deftest bind->-test
  (is (= 42
         (ds/bind-> 41 x inc)))
  (is (= 82
         (ds/bind-> 41 x
           (+ x))))
  (is (= 31
         (ds/bind-> 41 x
           (- 10))))

  (is (dfn/equals
       [39.81 3.709 7.418]
       (ds/bind-> (ds/->dataset "test/data/stocks.csv") ds
         (assoc :logprice2 (dfn/log1p (ds "price")))
         (assoc :logp3 (dfn/* 2 (ds :logprice2)))
         (ds/select-columns ["price" :logprice2 :logp3])
         (ds-tens/dataset->tensor)
         (first)))))


(deftest parse-nils
  (let [ds-a (ds/->dataset {:a [nil nil]})
        ds-b (ds/->dataset [{:a nil} {:a nil}])]
    (is (= (ds/row-count ds-a)
           (ds/row-count ds-b)))
    (is (= 2 (dtype/ecount (ds/missing ds-a)))
        (= 2 (dtype/ecount (ds/missing ds-b))))))


(deftest parser-fn-failing-on-csv-entries
  (let [stocks (ds/->dataset "test/data/stocks.csv"
                             {:key-fn keyword
                              :parser-fn {:date [:string #(subs % 0 5)]}})]
    (is (= "Jan 1"
           (first (stocks :date))))))

(deftest one-hot-failing
  (let [str-ds (-> (ds/->dataset [{"a" 1 "b" "AA"}
                                  {"a" 2 "b" "AA"}
                                  {"a" 3 "b" "BB"}
                                  {"a" 4 "b" "BB"}])
                   (ds/categorical->one-hot ["b"]))
        kwd-ds (-> (ds/->dataset [{:a 1 :b "AA"}
                                  {:a 2 :b "AA"}
                                  {:a 3 :b "BB"}
                                  {:a 4 :b "BB"}])
                   (ds/categorical->one-hot [:b]))]
    (is (= #{"a" "b-AA" "b-BB"} (set (ds/column-names str-ds))))
    (is (= #{:a :b-AA :b-BB} (set (ds/column-names kwd-ds))))))


(deftest select-memory
  (let [original (ds/->dataset [{:a 0} {:a 1} {:a 2} {:a 3} {:a 4}])
        new-ds (ds/select-rows original (range 4))]
    (is (= (vec (range 4)) (vec (new-ds :a))))
    (is (thrown? Throwable (vec (:a (ds/select-rows new-ds 4)))))))


(deftest custom-sort-by-column
  (let [DS (-> (tech.v3.dataset/->dataset {:a [5 4 3 2 8 7 6]})
               (ds/sort-by-column :a compare))]
    (is (= (vec (sort [5 4 3 2 8 7 6]))
           (vec (DS :a))))))


(deftest set-missing-new-column
  (let [col (ds-col/new-column "abc" (repeat 10 1) nil [1 2 3])]
    (is (= [1 nil nil nil 1 1 1 1 1 1] (vec col)))))


(deftest join-on-date
  (let [A (ds/->dataset {:a [(java.time.LocalDate/of 2001 01 01)]
                         :b [11]})
        B (ds/->dataset {:a [(java.time.LocalDate/of 2001 01 01)]
                         :c [22]})]
    (ds-join/left-join :a A B)))


(deftest sample-repeatable-seed
  (let [ds (ds/->dataset "test/data/stocks.csv")]
    (is (= (vec (get (ds/sample ds 5 {:seed 20}) "symbol"))
           (vec (get (ds/sample ds 5 {:seed 20}) "symbol"))))))


(deftest sample-arities
  (let [ds (ds/->dataset "test/data/stocks.csv")]
    (is (= (dtype/ecount (get (ds/sample ds) "symbol"))
           (dtype/ecount (get (ds/sample ds 5) "symbol"))))))


(deftest string-table-addall
  (let [data ["one" "two" "three"]
        strt (str-table/make-string-table 0)]
    (.addAll strt data)
    (is (= (vec strt)
           data))))


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
