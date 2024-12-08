(ns tech.v3.dataset.join-test
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.join :as ds-join]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.test :refer [deftest is testing]])
  (:import [java.time LocalDate]))


(deftest simple-join-test
  (let [lhs (ds/->dataset {:a (range 10)
                           :b (range 10)})
        rhs (ds/->dataset {:a (->> (range 10)
                                   (mapcat (partial repeat 2))
                                   (vec))
                           :c (->> (range 10)
                                   (mapcat (partial repeat 2))
                                   (vec))})
        {:keys [inner rhs-missing]} (ds-join/hash-join :a lhs rhs)]
    (is (dfn/equals (inner :a) (inner :b)))
    (is (dfn/equals (inner :b) (inner :c)))
    (is (empty? (seq rhs-missing))))
  (let [lhs (ds/->dataset {:a (range 10)
                           :b (range 10)})
        rhs (ds/->dataset {:a (->> (range 15)
                                   (mapcat (partial repeat 2))
                                   (vec))
                           :c (->> (range 15)
                                   (mapcat (partial repeat 2))
                                   (vec))})
        {:keys [inner rhs-missing]} (ds-join/hash-join [:b :c] lhs rhs
                                                       {:rhs-missing? true})]
    (is (dfn/equals (inner :a) (inner :b)))
    (is (dfn/equals (inner :b) (inner :right.a)))
    (is (= [20 21 22 23 24 25 26 27 28 29] (vec rhs-missing))))
  (let [lhs (ds/->dataset {:a (range 15)
                           :b (range 15)})
        rhs (ds/->dataset {:a (->> (range 10)
                                   (mapcat (partial repeat 2))
                                   (vec))
                           :c (->> (range 10)
                                   (mapcat (partial repeat 2))
                                   (vec))})
        {:keys [inner lhs-missing]} (ds-join/hash-join :a lhs rhs
                                                       {:lhs-missing? true})]
    (is (dfn/equals (inner :a) (inner :b)))
    (is (dfn/equals (inner :b) (inner :c)))
    (is (= [10 11 12 13 14] (vec lhs-missing)))))

(defn lhs-customer-db
  []
  (ds/->dataset [{"CustomerID" 1,
                  "CustomerName" "Alfreds Futterkiste",
                  "ContactName" "Maria Anders",
                  "Address" "Obere Str. 57",
                  "City" "Berlin",
                  "PostalCode" 12209,
                  "Country" "Germany"}
                 {"CustomerID" 2,
                  "CustomerName" "Ana Trujillo Emparedados y helados",
                  "ContactName" "Ana Trujillo",
                  "Address" "Avda. de la Constitución 2222",
                  "City" "México D.F.",
                  "PostalCode" 5021,
                  "Country" "Mexico"}
                 {"CustomerID" 3,
                  "CustomerName" "Antonio Moreno Taquería",
                  "ContactName" "Antonio Moreno",
                  "Address" "Mataderos 2312",
                  "City" "México D.F.",
                  "PostalCode" 5023,
                  "Country" "Mexico"}]
                {:parser-fn {"PostalCode" :int16}}))

(defn rhs-customer-db
  []
  (ds/->dataset [{"OrderID" 10308,
                  "CustomerID" 2,
                  "EmployeeID" 7,
                  "OrderDate" "1996-09-18",
                  "ShipperID" 3}
                 {"OrderID" 10309,
                  "CustomerID" 37,
                  "EmployeeID" 3,
                  "OrderDate" "1996-09-19",
                  "ShipperID" 1}
                 {"OrderID" 10310,
                  "CustomerID" 77,
                  "EmployeeID" 8,
                  "OrderDate" "1996-09-20",
                  "ShipperID" 2}]
                {:parser-fn {"OrderID" :int16
                             "CustomerID" :int16
                             "EmployeeID" :int16
                             "ShipperID" :int16}}))


(deftest inner-join-test
  (let [lhs (lhs-customer-db)
        rhs (rhs-customer-db)
        join-data (ds-join/inner-join "CustomerID" lhs rhs)
        lhs-colname-map (:left-column-names (meta join-data))
        rhs-colname-map (:right-column-names (meta join-data))]
    (is (= (count lhs-colname-map)
           (ds/column-count lhs)))
    (is (= (count rhs-colname-map)
           (ds/column-count rhs)))))


;;sample from https://www.w3schools.com/sql/sql_join_left.asp
(deftest left-join-test
  (let [lhs (lhs-customer-db)
        rhs (rhs-customer-db)
        join-data (ds-join/left-join "CustomerID" lhs rhs)
        recs (ds/mapseq-reader join-data)
        empty-int?    #{-32768}
        empty-string? #{""}
        empty-val?    #(or (empty-int? %) (empty-string? %)
                           (nil? %))
        realized       (some #(when (= (get % "CustomerID") 2) %) recs)
        unrealized     (filter #(not= % realized) recs)
        lhs-colname-map (:left-column-names (meta join-data))
        rhs-colname-map (:right-column-names (meta join-data))]
    (is (every? (complement empty-val?) (vals realized))
        "Ana's record should be fully realized.")
    (is (every? identity
                (for [{:strs [OrderID OrderDate ShipperID]}
                      unrealized]
                  ;;We can't do order date because they are dates
                  (every? empty-val? [OrderID ShipperID])))
        "Everyone else should have missing entries from RHS.")
    (is (= (count lhs-colname-map)
           (ds/column-count lhs)))
    (is (= (count rhs-colname-map)
           (ds/column-count rhs)))))


(deftest right-join-test
  (let [lhs (lhs-customer-db)
        rhs (rhs-customer-db)
        join-data (ds-join/right-join "CustomerID" lhs rhs)
        lhs-colname-map (:left-column-names (meta join-data))
        rhs-colname-map (:right-column-names (meta join-data))]
    (is (= #{2 37 77} (set (join-data "right.CustomerID"))))
    (is (= #{"Ana Trujillo" nil} (set (join-data "ContactName"))))
    (is (= #{5021 nil} (set (map #(when % (int %)) (join-data "PostalCode")))))
    (is (= #{1 2} (set (ds-col/missing (join-data "ContactName")))))
    (is (= #{1 2} (set (ds-col/missing (join-data "PostalCode")))))
    (is (= (count lhs-colname-map)
           (ds/column-count lhs)))
    (is (= (count rhs-colname-map)
           (ds/column-count rhs)))))


(deftest duplicate-column-test
  (let [test-ds (ds/->dataset "test/data/ames-house-prices/train.csv"
                              {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                               :n-records 5
                               :parser-fn {:SalePrice :float32}})
        jt (ds-join/inner-join "1stFlrSF" test-ds test-ds)]
    (is (= (ds/column-count jt)
           (count (distinct (ds/column-names jt))))))
  (let [test-ds (ds/->dataset "test/data/ames-house-prices/train.csv"
                              {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                               :n-records 5
                               :parser-fn {:SalePrice :float32}})
        jt (ds-join/inner-join ["1stFlrSF" "2ndFlrSF"] test-ds test-ds)]
    (is (= (ds/column-count jt)
           (count (distinct (ds/column-names jt)))))))


(deftest join-tuple-cname
  (let [DS (ds/->dataset [{:a 11 [:a :b] 2}])
        lj (ds-join/left-join :a DS DS)
        rj (ds-join/right-join :a DS DS)
        ljt (ds-join/left-join [[:a :b][:a :b]] DS DS)]
    ;;no nil column names
    (is (every? identity (ds/column-names lj)))
    (is (every? identity (ds/column-names rj)))
    (is (every? identity (ds/column-names ljt)))))


(defn- drop-missing
  [ds]
  (ds/drop-rows ds (ds/missing ds)))


(deftest asof-lt
  (let [ds-a (ds/->dataset {:a (range 10)})
        ds-b (ds/->dataset {:a (dfn/* 2 (range 10))})
        ds-bm (ds/->dataset {:a (dfn/- (dfn/* 2 (range 10)) 5)})
        ds-bmm (ds/->dataset {:a (dfn/- (dfn/* 2 (range 10)) 14)})]
    (is (= [2 2 4 4 6 6 8 8 10 10]
           (vec ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :<}) :right.a))))
    (is (= [0 2 2 4 4 6 6 8 8 10]
           (vec ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :<=}) :right.a))))
    (is (= [1 3 3 5 5 7 7 9 9 11]
           (vec ((ds-join/left-join-asof :a ds-a ds-bm {:asof-op :<}) :right.a))))
    (is (= [2 2 4 4 nil nil nil nil nil nil]
           (vec ((ds-join/left-join-asof :a ds-a ds-bmm {:asof-op :<}) :right.a)))))

  (let [cur-date (dtype-dt/local-date)
        date-fn #(when %
                   (dtype-dt/plus-temporal-amount cur-date % :days))
        ds-a (ds/->dataset {:a (date-fn (range 10))})
        ds-b (ds/->dataset {:a (date-fn (dfn/* 2 (range 10)))})
        ds-bm (ds/->dataset {:a (date-fn (dfn/- (dfn/* 2 (range 10)) 5))})
        ds-bmm (ds/->dataset {:a (date-fn (dfn/- (dfn/* 2 (range 10)) 14))})]
    (is (= (vec (date-fn [2 2 4 4 6 6 8 8 10 10]))
           (vec (packing/unpack
                 ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :<}) :right.a)))))
    (is (= (date-fn [0 2 2 4 4 6 6 8 8 10])
           (vec (packing/unpack
                 ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :<=}) :right.a)))))
    (is (= (date-fn [1 3 3 5 5 7 7 9 9 11])
           (vec (packing/unpack
                 ((ds-join/left-join-asof :a ds-a ds-bm {:asof-op :<}) :right.a)))))
    (is (= (date-fn [2 2 4 4])
           (vec (packing/unpack
                 ((drop-missing (ds-join/left-join-asof
                                 :a ds-a ds-bmm {:asof-op :<}))
                  :right.a)))))))


(deftest asof-gt
  (let [ds-a (ds/->dataset {:a (range 10)})
        ds-b (ds/->dataset {:a (dfn/* 2 (range 10))})
        ds-bm (ds/->dataset {:a (dfn/- (dfn/* 2 (range 10)) 5)})
        ds-bmm (ds/->dataset {:a (dfn/- (dfn/* 2 (range 10)) 14)})]
    (is (= [nil 0 0 2 2 4 4 6 6 8]
           (vec ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :>}) :right.a))))
    (is (= [0 0 2 2 4 4 6 6 8 8]
           (vec ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :>=}) :right.a))))
    (is (= [-1 -1 1 1 3 3 5 5 7 7]
           (vec ((ds-join/left-join-asof :a ds-a ds-bm {:asof-op :>}) :right.a))))
    (is (= [-2 0 0 2 2 4 4 4 4 4]
           (vec ((ds-join/left-join-asof :a ds-a ds-bmm {:asof-op :>}) :right.a))))))


(deftest asof-nearest
  (let [ds-a (ds/->dataset {:a (range 10)})
        ds-b (ds/->dataset {:a (dfn/* 3 (range 10))})
        ds-bm (ds/->dataset {:a (dfn/- (dfn/* 3 (range 10)) 5)})
        ds-bmm (ds/->dataset {:a (dfn/- (dfn/* 3 (range 10)) 20)})]
    (is (= [0 0 3 3 3 6 6 6 9 9]
           (vec ((ds-join/left-join-asof :a ds-a ds-b {:asof-op :nearest})
                 :right.a))))
    (is (= [1 1 1 4 4 4 7 7 7 10]
           (vec ((ds-join/left-join-asof :a ds-a ds-bm {:asof-op :nearest})
                 :right.a))))
    (is (= [1 1 1 4 4 4 7 7 7 7]
           (vec ((ds-join/left-join-asof :a ds-a ds-bmm {:asof-op :nearest})
                 :right.a))))))


(deftest pd-merge
  (let [ds-a (ds/->dataset {:a [:a :b :b :a :c]
                            :b (range 5)
                            :c (range 5)})
        ds-b (ds/->dataset {:a [:a :b :a :b :d]
                            :b (range 5)
                            :c (range 6 11)})]
    (is (= [0 1 2 3 4 nil nil nil]
           (vec ((ds-join/pd-merge ds-a ds-b {:on [:a :b] :how :outer}) :c))))
    (is (= [6 7 nil nil nil]
           (vec ((ds-join/pd-merge ds-a ds-b {:on [:a :b] :how :left}) :right.c))))
    (is (= [0 1 nil nil nil]
           (vec ((ds-join/pd-merge ds-a ds-b {:on [:a :b] :how :right}) :left.c))))
    (is (= [6 7]
           (vec ((ds-join/pd-merge ds-a ds-b {:on [:a :b] :how :inner}) :right.c))))
    (is (= [6 7 8 9 10 6 7 8 9 10 6 7 8 9 10 6 7 8 9 10 6 7 8 9 10]
           (vec ((ds-join/pd-merge ds-a ds-b {:how :cross}) :right.c))))))


(deftest double-join
  (let [a (ds/->dataset [{:name "a" :a 1.0 :b 2.0}
                         {:name "b" :a 1.0 :b 2.0}
                         {:name "c" :a 1.0 :b 2.0}])
        b (ds/->dataset [{:name "a" :c 1.0}
                         {:name "b" :c 1.0}])]
    (is (= [1.0 1.0 nil]
           (vec ((ds-join/left-join :name a b) :c))))
    (is (= ["a" "b" nil]
           (vec ((ds-join/left-join :name a b) :right.name))))
    (is (= [2.0 2.0 2.0]
           (vec ((ds-join/left-join :name a b) :b))))
    (is (= [1.0 1.0 1.0]
           (vec ((ds-join/left-join :name a b) :a))))
    (is (= ["a" "b" "c"]
           (vec ((ds-join/left-join :name a b) :name))))
    (ds-join/left-join :name a b)))

(deftest eraderna-left-join
  (testing "Changing the type of int shouldn't break the join"
    (let [a (-> (ds/->dataset [{:y 2022}]))
          a' (-> a
                 (ds/column-cast :y :int16))
          b (ds/->dataset [{:y 2022 :s "2022"}
                           {:y 2023 :s "2023"}])]
      (is (=
           ((ds-join/left-join :y a b) :s)
           ((ds-join/left-join :y a' b) :s))))))


(deftest cross-join
  (let [res (ds-join/pd-merge
             (ds/->dataset {:a [1 2 3] :b [4 5 6]})
             (ds/->dataset {:c [:a :b :c] :d [:x :y :z]})
             {:how :cross})]
    (is (= [1 1 1 2 2 2 3 3 3]
           (res :a)))
    (is (= [:a :b :c :a :b :c :a :b :c]
           (res :c)))))


(deftest pd-merge-issue-302
  (let [res (ds-join/pd-merge (ds/->dataset {:id ["a" "b"]
                                             :x  [1 2]})
                              (ds/->dataset {:id ["c"]
                                             :y  [3]})
                              {:on [:id] :how :outer})]
    (is (= [nil nil 3] (vec (:y res))))))


(deftest left-join-dates
  (is (= [{:a (LocalDate/of 2022 12 20)
	   :b 4,
	   :right.a (LocalDate/of 2022 12 20)
	   :c 5}
	  {:a (LocalDate/of 2022 12 28)
	   :b 3}
	  {:a (LocalDate/of 2022 12 30)
	   :b 4}]
         (vec (ds/rows
               (tech.v3.dataset.join/left-join
                :a
                (ds/->dataset [{:a (LocalDate/of 2022 12 28) :b 3}
                               {:a (LocalDate/of 2022 12 30) :b 4}
                               {:a (LocalDate/of 2022 12 20) :b 4}])
                (ds/->dataset [{:a (LocalDate/of 2022 12 20) :c 5}
                               {:a (LocalDate/of 2022 10 20) :c 6}
                               {:a (LocalDate/of 2022 11 20) :c 7}])))))))

(deftest issue-361
  (let [ds1 (ds/->dataset {:a '(\1 \2 \3 \4 \5 \6 \7 \8 \9)})
        ds2 (ds/->dataset {:a '(\0 \9 \8 \7 \6 \5 \4 \3 \2)})
        jds (ds-join/left-join :a ds1 ds2)]
    (is (= 9 (ds/row-count jds)))
    (is (= 1 (dtype/ecount (ds/missing jds))))))


(deftest issue-377
  (let [j (ds-join/left-join :a
                             (ds/->dataset {:a [nil 2]
                                            :b [3 4]})
                             (ds/->dataset {:a [nil 4]
                                            :b [6 7]}))]
    (is (= [6 nil] (vec (j :right.b))))))


(deftest short-types
  (let [lds (ds/->dataset [{:i "foo" :y (short 2022)}])
        rds (ds/->dataset [{:i "foo" :y 2022 :s "2022"}
                           {:i "foo" :y 2023 :s "2023"}])
        jds (ds-join/pd-merge lds rds {:on [:i :y]})]
    (is (= {:i "foo" :y 2022 :s "2022"}
           (ds/row-at jds 0))))
  (is (= 1 (ds/row-count
            (ds-join/left-join :z
                               (ds/->dataset [{:z ["foo" (short 2022)]}])
                               (ds/->dataset [{:z ["foo" (long 2022)] :s "2022"}
                                              {:z ["foo" (long 2023)] :s "2023"}]))))))


(deftest issue-381
  (let [make-row (fn [] {:row 1})
        left (ds/->dataset (repeatedly 10000 make-row))
        right (ds/->dataset (repeatedly 1000 make-row))
        jds (ds-join/left-join :row left right)]
    (is (= (* 10000 1000) (ds/row-count jds)))))


(deftest pd-merge-error
  (let [ds1 (ds/->dataset {:customer ["A" "A" "A"]                           
                           :product ["A" "B" "C"]})
        ds2 (ds/->dataset {:product ["B" "C"]})
        mm (ds-join/pd-merge ds1 ds2 {:on :product :how :inner})]
    (is (= #{:product :customer}
           (set (ds/column-names mm))))))


(deftest pd-merge-issue-435
  (is (ds-join/pd-merge (ds/empty-dataset)
                        (ds/->dataset {:t [0 1] :x [:a :b]})
                        {:on :t :how :outer}))
  (is (ds-join/pd-merge (ds/->dataset {:t [0 1] :x [:a :b]})
                        (ds/empty-dataset)                        
                        {:on :t :how :outer})))
