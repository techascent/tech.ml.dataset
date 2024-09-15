(ns tech.v3.dataset.categorical-test
  (:require [tech.v3.dataset.categorical :as ds-cat]
            [tech.v3.dataset.modelling :as ds-mod]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.column-filters :as cf]
            [clojure.test :refer [deftest is] :as t]
            [tech.v3.dataset :as ds]))


(deftest prediction
  (is (= [:no :yes]
           (->
            (ds/->dataset {:yes [0.3 0.5] :no [0.7 0.5]})
            (ds-mod/probability-distributions->label-column :val)
            (ds-cat/reverse-map-categorical-xforms)
            :val))))



(deftest prob-dist
  (let [prob
        (->
         (ds/->dataset {:yes [0.3 0.5] :no [0.7 0.5]})
         (ds-mod/probability-distributions->label-column :val)
         (ds-cat/reverse-map-categorical-xforms))]


    (is (= (:yes prob) [0.3 0.5]))
    (is (= (:no prob) [0.7 0.5]))
    (is (= (:val prob) [:no :yes]))))



(deftest cat-to-number
  (is (=
         (set
          (->
           (ds/->dataset {:x [:a :b] :y ["1" "0"]})
           (ds/categorical->number [:y])
           :y))
         (set [0 1]))))




(defn- cat->num [table-args]
  (->
   (ds/->dataset {:y [:a :b :c :d]})
   (ds/categorical->number  [:y] table-args)
   :y
   meta
   :categorical-map
   :lookup-table
   clojure.set/map-invert))


(deftest test-categorical->number []
  (is (= {5 :a, 2 :b, 0 :d, 1 :c}
           (cat->num  [[:a 5] [:b 2]])))
  (is (= {5 :a, 0 :b, 1 :d, 2 :c}
           (cat->num  [[:a 5] [:b 0]])))
  (is (= (cat->num  [])
           {0 :d, 1 :c, 2 :a, 3 :b}))
  (is (= (cat->num [[:not-present 1]])
           {1 :not-present, 0 :d, 2 :c, 3 :a, 4 :b}))
  (is (= (cat->num [[:a 1 :b 1]])
         {1 :a, 0 :d, 2 :c, 3 :b})))


(deftest cat-map-regression
  (is (every? #(Double/isFinite %)
              (-> (ds/->dataset "test/data/titanic.csv")
                  (ds/update-column "Survived"
                                    (fn [col]
                                      (let [val-map {0 :drowned
                                                     1 :survived}]
                                        (dtype/emap val-map :keyword col))))
                  (ds/categorical->number cf/categorical)
                  (ds/column "Survived")))))
(deftest categorical-assignments-are-integers
  (is (= #{0 1 2 3}
         (->
          (ds/->dataset {:x1 [1 2 4 5 6 5 6 7]
                         :x2 [5 6 6 7 8 2 4 6]
                         :y [:a :b :b :a :c :a :b :d]})
          (ds/categorical->number [:y])
          (get :y)
          distinct
          set))))


(defn- =-invert-cat [target-1 target-2
                         lookup-one lookup-two
                         result-datatype
                      expected-result
                      ]
  (let [ds (ds/->dataset {:target [target-1 target-2]})
        inverted
        (ds-cat/invert-categorical-map ds
                                       {:lookup-table {:one lookup-one
                                                       :two lookup-two},
                                        :src-column :target,
                                        :result-datatype result-datatype})
        inverted-target (-> inverted :target)]
    (= expected-result inverted-target)))
    ;(format "expected %s,  found: %s" expected-result) (seq inverted-target)))

(deftest invert-cat--works
  (is
   (=-invert-cat 1 2
                  1 2
                  :int
                  [:one :two]))
  ; TODO - should pass ?
  (is (=-invert-cat 1.0 2.0
                     1 2
                     :int
                     [:one :two]))
  
  ; TODO - should pass ?
  (is (=-invert-cat 1.99999 2.99999
                     1 2
                     :int
                     [:one :two]))
  
  ; TODO - should pass ?
  (is (=-invert-cat 1.2 1.3
                     1 2
                     :int
                     [:one :one])))

(deftest invert-cat--throws
  

(is (thrown? Exception
     (=-invert-cat 1.0 2.0
                   1.0 2.0
                   :float
                   [:one :two])
     ;; => Execution error at tech.v3.dataset.categorical/invert-categorical-map$fn (categorical.clj:177).
     ;;    Unable to find src value for numeric value 1.0
))

  (is (thrown? Exception
                (=-invert-cat 1 2
                               4 5
                               :int
                               [:one :two])))
;; => Execution error at tech.v3.dataset.categorical/invert-categorical-map$fn (categorical.clj:177).
;;    Unable to find src value for numeric value 1
  
  (is (thrown? Exception
       (=-invert-cat 1 2
                      1.0 2.0
                      :int
                      [:one :two]))))
;; => Execution error at tech.v3.dataset.categorical/invert-categorical-map$fn (categorical.clj:177).
;;    Unable to find src value for numeric value 1


(defn- is-roundtrip-ok [raw-model-prediction]
  (let [
        train-ds
        (->
         (ds/->dataset {:target [:a :b :c]})
         (ds/categorical->number [:target])
         )
        cat-map (-> train-ds :target meta :categorical-map)

        prediction-ds  
        (->
         (ds/->dataset {:target raw-model-prediction})
         (ds/assoc-metadata [:target] :categorical-map cat-map)
         (ds-cat/reverse-map-categorical-xforms))]
    (is (= [:c :a :b] (:target prediction-ds)))
    ))


(deftest round-trip 
;; only this should pass
  (is-roundtrip-ok [0 1 2])

;; currently these all pass, while I would like them to all fail
  (is-roundtrip-ok [0.0 1.2 2.2])
  (is-roundtrip-ok [0.9 1.9 2.9])
  (is-roundtrip-ok (float-array [0 1 2]))
  (is-roundtrip-ok (float-array [0 1.9 2.9]))
  (is-roundtrip-ok (double-array [0 1.5 2.2])))

