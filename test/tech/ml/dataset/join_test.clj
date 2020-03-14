(ns tech.ml.dataset.join-test
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.base :as ds-base]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))


(deftest simple-join-test
  (let [lhs (ds/name-values-seq->dataset {:a (range 10)
                                          :b (range 10)})
        rhs (ds/name-values-seq->dataset {:a (->> (range 10)
                                                  (mapcat (partial repeat 2)))
                                          :c (->> (range 10)
                                                  (mapcat (partial repeat 2)))})
        {:keys [join-table rhs-missing]} (ds-base/join-by-column :a lhs rhs)]
    (is (dfn/equals (join-table :a) (join-table :b)))
    (is (dfn/equals (join-table :b) (join-table :c)))
    (is (empty? (seq rhs-missing))))
  (let [lhs (ds/name-values-seq->dataset {:a (range 10)
                                          :b (range 10)})
        rhs (ds/name-values-seq->dataset {:a (->> (range 15)
                                                  (mapcat (partial repeat 2)))
                                          :c (->> (range 15)
                                                  (mapcat (partial repeat 2)))})
        {:keys [join-table rhs-missing]} (ds-base/join-by-column :a lhs rhs
                                                                 {:rhs-missing? true})]
    (is (dfn/equals (join-table :a) (join-table :b)))
    (is (dfn/equals (join-table :b) (join-table :c)))
    (is (= [20 21 22 23 24 25 26 27 28 29] (vec rhs-missing))))
  (let [lhs (ds/name-values-seq->dataset {:a (range 15)
                                          :b (range 15)})
        rhs (ds/name-values-seq->dataset {:a (->> (range 10)
                                                  (mapcat (partial repeat 2)))
                                          :c (->> (range 10)
                                                  (mapcat (partial repeat 2)))})
        {:keys [join-table lhs-missing]} (ds-base/join-by-column :a lhs rhs
                                                                 {:lhs-missing? true})]
    (is (dfn/equals (join-table :a) (join-table :b)))
    (is (dfn/equals (join-table :b) (join-table :c)))
    (is (= [10 11 12 13 14] (vec lhs-missing)))))


;;sample from https://www.w3schools.com/sql/sql_join_left.asp
(deftest left-join-test
  (let [lhs (ds/->dataset [{"CustomerID" 1,
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
                            "Country" "Mexico"}])

        rhs (ds/->dataset [{"OrderID" 10308,
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
                            "ShipperID" 2}])
        join-data (ds-base/join-by-column "CustomerID" lhs rhs {:lhs-missing? true})
        joined (:lhs-outer-join join-data)
        recs   (ds/mapseq-reader joined)]
    (is (= 2 (count recs)))
    (is (= #{1 3} (set (map #(get % "CustomerID") recs))))))


(comment

  (def lhs-fields
    [:size :day :operatorid :notes :more-notes :even-more-notes :how-can-there-be-more])

  (defn customers []
    (for [i (range 100000)]
      (let [city   (str (rand-int 10))]
        {:address    (str "Address" i)
         :gender     (rand-nth ["m" "f" "n"])
         :address-id i
         :country-code "99"
         :first-name (str "customer_" i "first")
         :last-name  (str "customer_" i "last")
         :city       city
         :zip-code   (clojure.string/join (repeat 5 city))
         :email      (str "customer_" i "@the-net")
         :huge-field (str "this is a huge field containing a lot of dumb info for
       bloat which will make the file so much larger for our poor machine how
       unkind of us to do so in this day and age" i)})))

  (def rhs-fields
    [:operatorid
     :address
     :gender
     :address-id
     :country-code
     :first-name
     :last-name
     :city
     :zip-code
     :email])

  (defn random-lhs []
    (for [i (range 200000)]
      {:size           (rand-nth ["s" "m" "l"])
       :day            (str (rand-int 100000))
       :operatorid    (str "op" (rand-int 10000) "op")
       :notes          "THis is some bloated information we'll add in"
       :more-notes     "to make the table larger"
       :even-more-notes "Also this will make things big as well"
       :how-can-there-be-more "Yet another text field will add overhead jabroni"}))

  (defn random-rhs []
    (let [cs  (vec (customers))]
      (for [i (range 500000)]
        (let [c (rand-nth cs)]
          (assoc c :operatorid (str "op" (rand-int 10000) "op"))))))

  (with-open [w (clojure.java.io/writer "lhs.csv")]
    (.write w (str (clojure.string/join "," (map name lhs-fields)) "\n"))
    (run! (comp #(.write w (str % "\n"))
                (partial clojure.string/join ",")
                (apply juxt lhs-fields)) (random-lhs)))

  (with-open [w (clojure.java.io/writer "rhs.csv")]
    (.write w (str (clojure.string/join "," (map name rhs-fields)) "\n"))
    (run! (comp #(.write w (str % "\n"))
                (partial clojure.string/join ",")
                (apply juxt rhs-fields)) (random-rhs)))


  (def lhs (ds/->dataset "lhs.csv"))
  (def rhs (ds/->dataset "rhs.csv"))

  (defn run-join-test
    [op-space]
    (ds-base/join-by-column "operatorid" lhs rhs {:operation-space op-space})
    :ok)
  )
