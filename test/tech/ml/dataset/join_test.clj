(ns tech.ml.dataset.join-test
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.base :as ds-base]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))


(deftest simple-join-test
  (let [lhs (ds/name-values-seq->dataset {:a (range 10)
                                          :b (range 10)})
        rhs (ds/name-values-seq->dataset {:a (->> (range 10)
                                                  (mapcat (partial repeat 2))
                                                  (long-array))
                                          :c (->> (range 10)
                                                  (mapcat (partial repeat 2))
                                                  (long-array))})
        {:keys [join-table rhs-missing]} (ds-base/join-by-column :a lhs rhs)]
    (is (dfn/equals (join-table :a) (join-table :b)))
    (is (dfn/equals (join-table :b) (join-table :c)))
    (is (empty? (seq rhs-missing)))))




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



  (defn run-join-test
    []
    (let [lhs (ds/->dataset "lhs.csv")
          rhs (ds/->dataset "rhs.csv")]
      (System/gc)
      (time
       (ds-base/join-by-column "operatorid" lhs rhs))))
  )
