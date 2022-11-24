(ns tech.v3.dataset-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.tensor :as ds-tens]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.join :as ds-join]
            [tech.v3.datatype.rolling :as rolling]
            [tech.v3.dataset.test-utils :as test-utils]
            [tech.v3.dataset.rolling :as ds-roll]
            [tech.v3.dataset.column-filters :as cf]
            [tech.v3.dataset.print :as ds-print]
            ;;Loading multimethods required to load the files
            [tech.v3.libs.poi]
            [tech.v3.libs.fastexcel]
            [tech.v3.io :as tech-io]
            [taoensso.nippy :as nippy]
            [clojure.test :refer [deftest is]])
  (:import [java.util List HashSet UUID]
           [java.io File ByteArrayInputStream]
           [tech.v3 TMD]))

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
                      (assoc :a (ds-col/column-map (fn [lhs rhs]
                                                     (when (and lhs rhs)
                                                       (+ (double lhs)
                                                          (double rhs))))
                                                   nil
                                                   (ds :a) (ds :b))))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/finite? (ds :a)))))
    (is (= #{0 1}
           (set (ds/missing (ds :a))))))

  (let [ds (ds/bind-> (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}]) ds
             (assoc :a (ds-col/column-map (fn [lhs rhs]
                                            (if (and lhs rhs)
                                              (+ (double lhs)
                                                 (double rhs))
                                              Double/NaN))
                                          :float64
                                          (ds :a) (ds :b))))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/finite? (ds :a))))))
  ;; Never remove these tests.  Actual users are relying on this behavior to simplify
  ;; their processing chains.
  (let [ds (ds/bind-> (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}]) ds
                      (assoc :a (ds-col/column-map (fn [^double lhs ^double rhs]
                                                     (+ (double lhs)
                                                        (double rhs)))
                                                   {:missing-fn ds-col/union-missing-sets
                                                    :datatype :float64}
                                                   (ds :a) (ds :b))))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/finite? (ds :a))))))
  (let [ds (-> (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}])
               (ds/column-map-m :a [:a :b]
                                (when (and a b)
                                  (+ (double a) (double b)))))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/finite? (ds :a)))))
    (is (= #{0 1}
           (set (ds/missing (ds :a))))))
  (let [ds (-> (ds/->dataset [{:a.a 1} {:b 2.0} {:a.a 2 :b 3.0}])
               (ds/column-map-m :a [:a.a :b]
                                (when (and a-a b)
                                  (+ (double a-a) (double b)))))]
    (is (= :float64 (dtype/get-datatype (ds :a))))
    (is (= [false false true]
           (vec (dfn/finite? (ds :a)))))
    (is (= #{0 1}
           (set (ds/missing (ds :a)))))))


(deftest mean-object-column
  (let [ds (-> (ds/->dataset [])
               (ds/add-or-update-column :a (map (fn [arg] (* 2 arg)) (range 9))))]
    (is (= :int64 (dtype/get-datatype (ds :a))))
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
               (vec))))
    ))

(deftest select-by-index
  (let [ds (ds/->dataset {:V1 (take 9 (cycle [1 2]))
                          :V2 (range 1 10)
                          :V3 (take 9 (cycle [0.5 1.0 1.5]))
                          :V4 (take 9 (cycle [\A \B \C]))})]

    (is (= [1 \A]
           (-> (ds/select-by-index ds [0 3] [0 8])
               (ds/value-reader)
               (first)
               (vec))
           (-> (ds/select-by-index ds [-4 -1] [-9 -1])
               (ds/value-reader)
               (first)
               (vec))))

    (is (= [\C]
           (-> (ds/select-by-index ds 3 8)
               (ds/value-reader)
               (first)
               (vec))
           (-> (ds/select-by-index ds -1 -1)
               (ds/value-reader)
               (first)
               (vec))
           (-> (ds/select-by-index ds [3] [8])
               (ds/value-reader)
               (first)
               (vec))
           (-> (ds/select-by-index ds [-1] [-1])
               (ds/value-reader)
               (first)
               (vec))))

    (is (= [\A \B \C \A \B \C \A \B \C]
           (vec ((ds/select-columns-by-index ds 3) :V4))
           (vec ((ds/select-columns-by-index ds [3]) :V4))
           (vec ((ds/select-columns-by-index ds -1) :V4))
           (vec ((ds/select-columns-by-index ds [-1]) :V4))))

    (is (= [2 6 1.5 \C]
           (-> (ds/select-rows-by-index ds -4)
               (ds/value-reader)
               (first)
               (vec))
           (-> (ds/select-rows-by-index ds [-4])
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

(deftest positional-column-rename
  (let [DS (ds/->dataset
            (-> "id,a,a\n0,aa,bb\n1,cc,dd"
                .getBytes
                ByteArrayInputStream.)
            {:file-type :csv})
        new-cols-incorrect [:a1 :a2]
        new-cols-correct [:id :a1 :a2]]
    (is (= new-cols-correct
           (-> DS
               (ds/rename-columns new-cols-correct)
               ds/column-names)))
    (is (thrown? Throwable
                 (ds/rename-columns DS new-cols-incorrect)))
    (is (thrown? Throwable
                 (ds/rename-columns DS (set new-cols-correct))))))

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

(deftest dataset-equality
  (let [ds0 (ds/->dataset {:foo "foo" :bar "bar"}) ;;equal to 3
        ds1 (ds/->dataset {:foo "foo" :bar "bar" :baz "baz"})
        ds2 (ds/->dataset {:foo "foo" :bar "beer"})
        ds3 (ds/->dataset {:foo "foo" :bar "bar"}) ;;equal to 0
        datasets [ds0 ds1 ds2 ds3]
        hashmaps (mapv (fn [ds] (into {} ds)) datasets)
        mapify #(reduce (fn [^java.util.Map m [k v]]
                          (doto m (.put k v)))
                         (java.util.HashMap.) %)
        mutmaps (mapv mapify datasets)
        xs        (range (count datasets))
        dsresults (->> (for [i xs
                             j xs]
                       [i j (= (nth datasets i) (nth datasets j))])
                     (filter last)
                     (map (juxt first second))
                     set)
        hashresults (->> (for [i xs
                               j xs]
                           [i j (= (nth datasets i) (nth hashmaps j))])
                         (filter last)
                         (map (juxt first second))
                         set)
        mapresults (->> (for [i xs
                              j xs]
                          [i j (= (nth datasets i) (nth mutmaps j))])
                        (filter last)
                        (map (juxt first second))
                        set)
        expected  #{[0 0] [1 1] [2 2] [3 3] [3 0] [0 3]}]
    (is (= dsresults expected)
        "Datasets should obey map equivalence when compared to datasets.")
    (is (= hashresults expected)
        "Datasets should obey map equivalence when compared to IPersistentMap.")
    (is (= mapresults expected)
        "Datasets should obey map equivalence when compared to java.util.Map
         like HashMap.")))

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
    (is (= [nil nil nil 1.0 2.0 2.0 2.0 2.0 2.0 2.0 4.0 4.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :down) :a))))
    (is (= [555.0 555.0 555.0 1.0 2.0 2.0 2.0 2.0 2.0 2.0 4.0 4.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :all :down 555) :a))))
    (is (= [1.0 1.0 1.0 1.0 2.0 2.0 2.0 2.0 2.0 2.0 4.0 4.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :all :downup) :a))))
    (is (= [1.0 1.0 1.0 1.0 2.0 4.0 4.0 4.0 4.0 4.0 4.0 11.0 11.0 nil nil]
           (vec ((ds/replace-missing ds :up) :a))))
    (is (= [1.0 1.0 1.0 1.0 2.0 4.0 4.0 4.0 4.0 4.0 4.0 11.0 11.0 11.0 11.0]
           (vec ((ds/replace-missing ds :updown) :a))))
    (is (= [1.0 1.0 1.0 1.0 2.0 4.0 4.0 4.0 4.0 4.0 4.0 11.0 11.0 555.0 555.0]
           (vec ((ds/replace-missing ds :all :up 555) :a))))
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


(deftest replace-missing-selector-fn
  (let [ds (ds/->dataset {:a [nil nil 2 4]
                          :b [nil nil 4 6]
                          :c [nil nil "A" "B"]})
        ds-replaced (-> ds
                        (ds/replace-missing cf/numeric :value dfn/mean)
                        (ds/replace-missing cf/categorical :value "C"))]
    (is (= [3 3 2 4] (vec (ds-replaced :a))))
    (is (= [5 5 4 6] (vec (ds-replaced :b))))
    (is (= ["C" "C" "A" "B"] (vec (ds-replaced :c))))))


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


(deftest replace-missing-abb
  (let [dtds (ds/->dataset {:a [nil nil nil 1.0 2  nil nil nil
                                nil  nil 4   nil  11 nil nil]
                            :b [2   2   2 nil nil nil nil nil
                                nil 13   nil   3  4  5 5]})
        fds (ds/replace-missing dtds :abb)]
    (is (= 0 (dtype/ecount (ds/missing fds))))))


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


(deftest concat-copying-object-fail
  (let [ds1 (ds/->dataset {:a [["A" 1]["B" 1]]})
        ds2 (ds/->dataset {:a [["A" 2]["B" 2]]})
        dsc (ds/concat-copying ds1 ds2)]
    (is (= [["A" 1] ["B" 1] ["A" 2] ["B" 2]]
           (vec (dsc :a))))))


(deftest concat-inplace-desc-stats
  (let [ds (ds/->dataset [{"A" 1 "B" 2} {"A" 2 "B" 3}])]
    (is (dfn/equals [1.5 2.5]
                    (-> (ds/concat ds ds)
                        (ds/descriptive-stats)
                        (:mean))))))


(deftest replace-missing-regression-181
  []
  (let [ds (ds/->dataset {:a [nil nil 2 2]})]
    (is (=  [2 2 2 2]
            (-> (ds/replace-missing ds :all :value dfn/mean)
                :a
                vec)))))


(deftest replace-missing-regression-184
  (let [date-dtype (java.time.LocalDate/parse "2020-12-11")
        ds (ds/->dataset {:a [nil 2 nil nil 4 nil 6 nil]
                          :b [3. nil nil 6. nil 9. nil 12.]
                          :c [nil "A" nil nil "B" nil "C" nil]
                          :d ["A" nil nil "B" nil "C" nil "D"]
                          :e (dtype-dt/plus-temporal-amount
                              (dtype/make-container
                               :local-date
                               [nil date-dtype nil nil date-dtype nil date-dtype nil])
                              (dfn/* 10 (range 8))
                              :days)})
        ds' (ds/replace-missing ds :midpoint)]
    (is (= [2.0 2.0 3.0 3.0 4.0 5.0 6.0 6.0] (vec (ds' :a))))
    (is (= [3.0 4.5 4.5 6.0 7.5 9.0 10.5 12.0] (vec (ds' :b))))
    (is (= [nil "A" "A" "A" "B" "B" "C" "C"] (vec (ds' :c))))
    (is (= ["A" "A" "A" "B" "B" "C" "C" "D"] (vec (ds' :d))))
    (is (= ["2020-12-21" "2020-12-21" "2021-01-05" "2021-01-05" "2021-01-20"
            "2021-01-30" "2021-02-09" "2021-02-09"]
           (mapv str (:e ds'))))
    (let [ds (ds/->dataset {:a [nil 2 nil nil nil 4 nil 6 nil]
                            :b [3. nil nil nil 6. nil 9. nil 12.]
                            :c [nil "A" nil nil "B" nil nil "C" nil]
                            :d ["A" nil nil "B" nil nil "C" nil "D"]
                            :e (dtype-dt/plus-temporal-amount
                                (dtype/make-container
                                 :local-date
                                 [nil date-dtype nil nil nil date-dtype nil
                                  date-dtype nil])
                                (dfn/* 10 (range 9))
                                :days)})
          ds' (ds/replace-missing ds :nearest)
          ds'' (ds/replace-missing ds :mid)]
      (is (= [2 2 2 2 4 4 4 6 6] (vec (ds' :a))))
      (is (= [2 2 2 2 4 4 4 6 6] (vec (ds'' :a))))
      (is (= [3.0 3.0 3.0 6.0 6.0 6.0 9.0 9.0 12.0] (vec (ds' :b)))))))


(deftest column-to-double-regression-187
  (let [col1 (ds-col/new-column :col1 [1 2 3])]
    (is (dfn/equals [1 2 3]
                    (ds-col/to-double-array col1))))
  (let [col1 (ds-col/new-column :col1 (int-array [1 2 3]))]
    (is (dfn/equals (ds-col/to-double-array col1) [1 2 3]))))


(deftest boolean-csv-column-names
  (try
    (ds/write!
     (ds/->dataset {false [1]}) "test/out.csv")
    (is (= ["false"] (-> (ds/->dataset "test/out.csv")
                         (ds/column-names))))
    (finally (.delete (java.io.File. "test/out.csv")))))


(deftest to-double-array-returns-double-array
  (let [data (ds/->dataset [{:a 1.0 :b 2.0}
                            {:a 3.0}])]
    (is (instance? (Class/forName "[D") (ds-col/to-double-array (data :a))))
    (is (every? identity (dfn/eq [2.0 Double/NaN]
                                 (ds-col/to-double-array (data :b)))))))


(deftest write-with-nil-name
  (let [data (-> (ds/->dataset [{:a 1.0 :b 2.0}
                                {:a 3.0}])
                 (vary-meta assoc :name nil))]
    (try
      (ds/write! data "test/data/nil-name.csv")
      (finally
        (.delete (java.io.File. "test/data/nil-name.csv"))))))


(deftest create-dataset-scalars
  (let [data (ds/->dataset {:a [1 2 3 4]
                            :b "hey"
                            :c (range)
                            :d 1})]
    (is (= ["hey" "hey" "hey" "hey"]
           (vec (data :b))))
    (is (= [:int64 :string :int64 :int64]
           (mapv (comp :datatype meta) (vals data))))))


(deftest create-dataset-seq
  (let [data (ds/->dataset {:calendar-year '(2020 2021 2020 2021)
                            :setting '("A" "A" "B" "B")
                            :bigdata (cycle [1 2 3 4])})]
    (is (= 4 (ds/row-count data)))))


(deftest empty-dataset-on-select-nothing
  (let [dataset (ds/->dataset "test/data/stocks.csv")]
    (is (= 0 (ds/row-count (ds/select-columns dataset nil))))
    (is (= 0 (ds/row-count (ds/select-rows dataset nil))))
    (is (= (ds/column-count dataset)
           (ds/column-count (ds/select-rows dataset nil))))))


(deftest column-cast-test-cce-fail
  (let [ds (ds/->dataset {:col1 [1 2 3 "NaN"]} {:parser-fn :string})]
    (is (= [1.0 2.0 3.0]
           (->> (ds/column-cast ds :col1 [:float64 :relaxed?])
                (#(ds/column % :col1))
                (take 3)
                (vec))))))


(deftest desc-stats-ok
  (let [ds (ds/->dataset [])]
    (is '()
        (ds/brief ds))))


(deftest desc-stats-also-ok
  (let [ds (ds/->dataset {"col1" [] "col2" [1]})]
    (is '()
        (ds/brief ds))))


(deftest desc-stats-oob
  (let [ds (ds/->dataset {"col1" []})]
    (is '()
        (ds/brief ds))))


(deftest column-map-regression-1
  (let [testds (ds/->dataset [{:a 1.0 :b 2.0} {:a 3.0 :b 5.0} {:a 4.0 :b nil}])]
    ;;result scanned for both datatype and missing set
    (is (= (vec [3.0 6.0 nil])
           (:b2 (ds/column-map testds :b2 #(when % (inc %)) [:b]))))
    ;;result scanned for missing set only.  Result used in-place.
    (is (= (vec [3.0 6.0 nil])
           (:b2 (ds/column-map testds :b2 #(when % (inc %))
                               {:datatype :float64} [:b]))))
    ;;Nothing scanned at all.
    (is (= (vec [3.0 6.0 nil])
           (:b2 (ds/column-map testds :b2 #(inc %)
                               {:datatype :float64
                                :missing-fn ds-col/union-missing-sets} [:b]))))
    ;;Missing set scanning causes NPE at inc.
    (is (thrown? Throwable
                 (ds/column-map testds :b2 #(inc %)
                                {:datatype :float64}
                                [:b])))))


(deftest remove-columns-issue-242
  (is (= [:a "c" :d :e]
         (vec (-> (tech.v3.dataset/->dataset {:a [1] :b [2] "c" [3]
                                              :d [4] :e [5]})
                  (tech.v3.dataset/drop-columns [:b])
                  (ds/column-names))))))


(deftest column-cast-packed-date
  (let [x (ds/->dataset [{:a 0 :b "2020-03-05"} {:a 1 :b nil}])
        y (ds/column-cast x :b :packed-local-date)]
    (is (= (vec (.data (y :b)))
           (vec (y :b))))
    (is (nil? ((y :b) 1)))))


(deftest dataset->data-regression-249
  (let [src-ds (ds/concat (ds/->dataset {:x ["1"]
                                         :y ["2" "3"]})
                          (ds/->dataset {:x ["4"]
                                         :y ["5"]}))
        ds-data (ds/dataset->data src-ds)
        rehydrated (ds/data->dataset ds-data)]
    (is (= (vec (src-ds :x))
           (vec (rehydrated :x))))
    (is (= (ds/missing src-ds)
           (ds/missing rehydrated)))))


(deftest dataset->data-regression-250
  (let [src-ds (ds/->dataset {:x [1]
                              :y [[3 4]]})
        new-ds (-> (nippy/freeze src-ds)
                   (nippy/thaw))]
    (is (= (vec (src-ds :y))
           (vec (new-ds :y))))))


(deftest freeze-thaw-column
  (let [{:keys [date price symbol]}
        (ds/->dataset "test/data/stocks.csv" {:key-fn keyword})
        date-data (nippy/freeze date)
        symbol-data (nippy/freeze symbol)
        ndate (nippy/thaw date-data)
        nsym (nippy/thaw symbol-data)
        nds (ds/new-dataset [ndate nsym])]
    (is (= (vec date)
           (nds :date)))
    (is (= (vec symbol)
           (nds :symbol)))))


(deftest negative-index-on-columns-gets-last
  (let [ds (ds/->dataset "test/data/stocks.csv")
        last-idx (dec (ds/row-count ds))
        symbol (ds "symbol")]
    (is (= (symbol last-idx) (symbol -1)))))


;; This was a bad idea.  Concatenating, just the same as concatenating sequences of maps
;; should not require the same columns across all datasets.  That creates extremely
;; error prone code.
(deftest concat-doesnt-require-same-columns
  (let [ds (ds/concat-copying
            (ds/->dataset {:a (range 10)
                           :c (repeat 10 (dtype-dt/local-date))})
            (ds/->dataset {:b (range 10)}))]
    (is (= 20 (ds/row-count ds)))
    (is (= 10 (dtype/ecount (ds/missing (ds :a)))))
    (is (= 10 (dtype/ecount (ds/missing (ds :b)))))))


;;It is way too confusing for users to have to navigate pack/unpack code in any
;;normal situation.
(deftest filter-sort-columns-uses-unpacked-datatypes
  (let [stocks (ds/->dataset "test/data/stocks.csv")
        test-val (second (stocks "date"))]
    (is (not= 0 (ds/row-count (ds/filter-column stocks "date" #(= % test-val)))))
    ;;make sure sorting still works
    (is (= (ds/row-count stocks)
           (ds/row-count (ds/sort-by-column stocks "date"))))))


(deftest binary-ops-on-integer-missing-results-in-nan
  (let [src-ds (ds/->dataset {:a [1 2 nil 4]})
        dst-ds (assoc src-ds :b (dfn/+ (:a src-ds ) 1))]
    (is (= 1 (dtype/ecount (ds/missing (dst-ds :b)))))
    (is (= [2.0 3.0 nil 5.0]
           (vec (dst-ds :b))))))


(deftest sort-works-with-nan
  (let [ds (ds/->dataset {:a [1 nil 2 nil nil 4]} )
        ds-first (ds/sort-by-column ds :a nil {:nan-strategy :first})
        ds-last (ds/sort-by-column ds :a nil {:nan-strategy :last})]
    (is (= [nil nil nil 1 2 4] (vec (ds-first :a))))
    (is (= [1 2 4 nil nil nil] (vec (ds-last :a))))
    (is (thrown? Exception (ds/sort-by-column ds :a nil {:nan-strategy :exception})))))


(deftest concat-packed-date-with-date-results-in-local-date-or-packed-local-date
  (let [ds (ds/->dataset (repeat 10 {:a (dtype-dt/local-date)})
                         {:parser-fn {:a :local-date}})
        ds-packed (ds/->dataset {:a (repeat 10 (dtype-dt/local-date))}
                                {:parser-fn {:a :packed-local-date}})
        res-inp (ds/concat-inplace ds ds-packed)
        res-cp (ds/concat-copying ds ds-packed)]
    (is (#{:local-date :packed-local-date} (dtype/elemwise-datatype (res-inp :a))))
    (is (#{:local-date :packed-local-date} (dtype/elemwise-datatype (res-cp :a))))))


(deftest row-map-test
  (let [ds (ds/->dataset "test/data/stocks.csv")]
    (is (thrown? Exception (ds/row-map ds #(hash-map :price2 (* (% :price) (% :price))))))
    (is (dfn/equals (dfn/sq (ds "price"))
                    (-> (ds/row-map ds #(hash-map :price2 (* (% "price") (% "price"))))
                        (ds/column :price2))))))


(deftest extend-packed-date-with-empty
  (let [ds-a (ds/->dataset {:b (range 20)})
        ds (ds/->dataset (repeat 10 {:a (dtype-dt/local-date)})
                         {:parser-fn {:a :packed-local-date}})
        fin-ds (merge ds-a ds)]
    (is (not (nil? (.toString (fin-ds :a)))))))


(deftest desc-stats-date-col
  (let [src-ds  (tech.v3.dataset/->dataset
                 {:date-time-with-nil ["Jul 1, 2011" nil]}
                 {:parser-fn :local-date})
        {:keys [min mean max]} (tech.v3.dataset/descriptive-stats src-ds)
        val ((src-ds :date-time-with-nil) 0)]
    (is (every? #(= % val) [(min 0) (mean 0) (max 0)]))))


(deftest nth-col-neg-indexes
  (let [data ((ds/->dataset {:a (range 10)}) :a)]
    (is (thrown? Throwable (nth data 10)))
    (is (= :a (nth data 10 :a)))
    (is (thrown? Throwable (nth data -11)))
    (is (= :a (nth data -11 :a)))
    (is (= 0 (nth data -10 :a)))))


(deftest column-rolling-regression
  (is (every? identity (dfn/eq
                        [##NaN 2.0 2.5 3.5]
                        (rolling/fixed-rolling-window
                         ((ds/->dataset {:a [##NaN 2 3 4]}) :a)
                         2 dfn/mean))))
  (is (every? identity (dfn/eq
                        [##NaN 2.0 2.5 3.5]
                        (rolling/fixed-rolling-window
                         (ds-col/new-column [nil 2 3 4])
                         2 dfn/mean))))
  (is (every? identity (dfn/eq
                        [##NaN 2.0 2.5 3.5]
                        (rolling/fixed-rolling-window
                         (ds-col/new-column (double-array [##NaN 2 3 4]))
                         2 dfn/mean)))))


(deftest concat-nil-is-nil
  (is (= nil (apply ds/concat nil)))
  (is (= nil (apply ds/concat-copying nil)))
  (is (= nil (apply ds/concat-inplace nil))))


(deftest replace-missing-whacks-metadata-274
  (let [ds (-> (ds/->dataset {:a [0 nil 1 nil 2]})
               (ds/update-column :a (fn [a-col]
                                      (with-meta a-col {:a :b}))))
        dsm (ds/replace-missing-value ds [:a] 10)
        dsmm (ds/replace-missing ds [:a] :down)]
    (is (= {:a :b} (select-keys (meta (ds :a)) [:a])))
    (is (= {:a :b} (select-keys (meta (dsm :a)) [:a])))
    (is (= {:a :b} (select-keys (meta (dsmm :a)) [:a])))))


(deftest induction-test
  (let [induct-ds (-> (ds/->dataset {:a [0 1 2 3] :b [1 2 3 4]})
                      (ds/induction (fn [ds]
                                      {:sum-of-previous-row (dfn/sum (ds/rowvec-at ds -1))
                                       :sum-a (dfn/sum (ds :a))
                                       :sum-b (dfn/sum (ds :b))})))]
    (is (= [0.0 1.0 3.0 6.0]
           (induct-ds :sum-b)))

    (is (= [0.0 0.0 1.0 3.0]
           (induct-ds :sum-a)))

    (is (= [0.0 1.0 5.0 14.0]
           (induct-ds :sum-of-previous-row)))))


(deftest row-mapcat
  (let [ds (ds/->dataset {:rid (range 10)
                          :data (repeatedly 10 #(rand-int 3))})
        mds (ds/row-mapcat ds (fn [row]
                                (for [idx (range (row :data))]
                                  {:idx idx})))
        n-rows (long (dfn/sum (ds :data)))]
    (is (= n-rows (ds/row-count mds)))))


(deftest array-of-structs-all-dtypes
  (let [sdef (dt-struct/define-datatype! :alldtypes
               [{:name :i8 :datatype :int8}
                {:name :u8 :datatype :uint8}
                {:name :i16 :datatype :int16}
                {:name :u16 :datatype :uint16}
                {:name :i32 :datatype :int32}
                {:name :u32 :datatype :uint32}
                {:name :i64 :datatype :int64}
                {:name :u64 :datatype :uint64}
                {:name :f32 :datatype :float32}
                {:name :f64 :datatype :float64}])
        ary (dt-struct/new-array-of-structs :alldtypes 10)
        cmap (dt-struct/column-map ary)
        _ (doseq [col (vals cmap)]
            (dtype/copy! (range 10) col))
        ds (ds/->dataset cmap)
        props (sdef :data-layout)]
    (doseq [prop props]
      (let [col (ds/column ds (:name prop))
            cmeta (meta col)]
        (is (= (:datatype cmeta) (:datatype prop)) (str prop))
        (is (= (vec (cmap (:name prop)))
               (vec col))
            (str prop))))))


(deftest replace-missing-packed-local-date
  (let [date (dtype-dt/local-date)
        ds (-> (ds/->dataset {:a [date nil nil date nil]})
               (ds/replace-missing :all :value date))]
    (is (== 0 (dtype/ecount (ds/missing ds))))
    (is (= (vec (repeat 5 date))
           (vec (ds :a))))))


(deftest variable-rolling-window-doubles
  (let [ds (ds/->dataset {:a (double-array (range 100))
                          :b (range 100)})
        small-win (ds/head (ds-roll/rolling ds {:window-type :variable
                                                :window-size 10
                                                :column-name :a}
                                            {:b-mean (ds-roll/mean :b)}))
        big-win (ds/head (ds-roll/rolling ds  {:window-type :variable
                                               :window-size 20
                                               :column-name :a}
                                          {:b-mean (ds-roll/mean :b)}))]
    (is (dfn/equals [4.5 5.5 6.5 7.5 8.5] (vec (small-win :b-mean))))
    (is (dfn/equals [0.0 0.5 1.0 1.5 2.0]
                    (-> (ds-roll/rolling ds {:window-type :variable
                                             :window-size 10
                                             :column-name :a
                                             :relative-window-position :left}
                                         {:b-mean (ds-roll/mean :b)})
                        (ds/head)
                        (ds/column :b-mean)
                        (vec))))
    (is (dfn/equals [2.0 2.5 3.0 3.5 4.0]
                    (-> (ds-roll/rolling ds {:window-type :variable
                                             :window-size 10
                                             :column-name :a
                                             :relative-window-position :center}
                                         {:b-mean (ds-roll/mean :b)})
                        (ds/head)
                        (ds/column :b-mean)
                        (vec))))
    (is (dfn/equals [9.5 10.5 11.5 12.5 13.5] (vec (big-win :b-mean))))))



(deftest rolling-multi-column-reducer
  (let [ds (ds/->dataset {:a (range 100)
                          :b (range 100)})
        fin-ds (ds-roll/rolling ds 10 {:c {:column-name [:a :b]
                                           :reducer (fn [a b]
                                                      (+ (dfn/sum a) (dfn/sum b)))
                                           :datatype :int16}})]
    (is (= :int16 (dtype/elemwise-datatype (fin-ds :c))))
    (is (= [20 30 42 56 72]
           (vec (take 5 (fin-ds :c)))))))


(deftest unroll-single-column
  (is (= (vec (range 9))
         (-> (ds/->dataset {:a [[0 1 2 3] [4 5] [6 7 8]]})
             (ds/unroll-column :a)
             (ds/column :a)
             (vec)))))


(deftest construct-with-hashmap
  (let [hm (doto (java.util.HashMap.)
             (.put :a 1)
             (.put :b 2))
        ds (ds/->dataset [hm hm hm])]
    (is (= (vector 1 1 1)
           (vec (ds :a))))))


(deftest double-nan-missing
  (let [ds (ds/->dataset {:a [0.0 Double/NaN 2.0]
                          :b [0 nil 2]
                          :c [:a nil :b]})]
    (is (= [2.0]
           (-> (ds/filter-column ds :a identity)
               (ds/column :a)
               (vec))))
    (is (= [2.0]
           (-> (ds/filter-column ds :b identity)
               (ds/column :a)
               (vec))))
    (is (= [0.0 2.0]
           (-> ds
               (ds/filter-column :c identity)
               (ds/column :a)
               (vec))))))


(deftest issue-315
  (is (not (nil? (ds/concat (ds/drop-rows (ds/->dataset [{:a 1 :b 2}]) [0])
                            (ds/drop-rows (ds/->dataset [{:a 1 :c3 2}]) [0]))))))


(deftest issue-259
  (let [ds (ds/->dataset [{"a o" 1 "b o" 2} {"a o" 5 "b o" 3}]
                         {:key-fn #(keyword (clojure.string/replace % " " "-"))})]
    (is (= #{:b-o :a-o} (set (map (comp :name meta) (vals ds))))))
  (let [ds (ds/->dataset {"a o" [1 5] "b o" [2 3]}
                         {:key-fn #(keyword (clojure.string/replace % " " "-"))})]
    (is (= #{:b-o :a-o} (set (map (comp :name meta) (vals ds))))))
  (let [ds (ds/->dataset [{"Foo" 1 , "Bar" 2}]
                         {:key-fn #(keyword (.toLowerCase %))})]
    (is (= #{:foo :bar}
           (set (map (comp :name meta) (vals ds))))))
  (let [ds (ds/->dataset (java.io.ByteArrayInputStream. (.getBytes "Foo,Bar\n1,2"))
                         {:key-fn #(keyword (.toLowerCase %))
                          :file-type :csv})]
    (is (= #{:foo :bar}
           (set (map (comp :name meta) (vals ds)))))))


(deftest discrete-categorical-issue-322
  (let [ds (ds/->dataset "test/data/stocks.csv")]
    (is (thrown? Exception (ds/categorical->number ds ["symbol"] {"AAPL" 1
                                                                  "MSFT" 2.2
                                                                  "AMZN" 3
                                                                  "IBM" 4
                                                                  "GOOG" 5})))
    (is (= (set (range 1 6))
           (->> (-> (ds/categorical->number ds ["symbol"] {"AAPL" 1
                                                           "MSFT" 2
                                                           "AMZN" 3
                                                           "IBM" 4
                                                           "GOOG" 5})
                    (ds/column "symbol"))
                (map long)
                (set))))))

(deftest column-meta-roundtrip
  (is (= :v
         (->
          (ds-base/column->data (ds-col/new-column :a [0] {:k :v}))
          (ds-base/data->column)
          meta
          :k
          ))))

(deftest print-all-test
  (let [ds (ds/->dataset (for [i (range 1000)] {:a i}))]
    (is (= (meta (ds/print-all ds))
           (meta (ds-print/print-range ds :all))))))

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
