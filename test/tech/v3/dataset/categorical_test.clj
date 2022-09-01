(ns tech.v3.dataset.categorical-test
  (:require [tech.v3.dataset.categorical :as ds-cat]
            [tech.v3.dataset.modelling :as ds-mod]
            [clojure.test :as t]
            [tech.v3.dataset :as ds]))


(t/deftest prediction
  (t/is (= [:no :yes]
           (->
            (ds/->dataset {:yes [0.3 0.5] :no [0.7 0.5]})
            (ds-mod/probability-distributions->label-column :val)
            (ds-cat/reverse-map-categorical-xforms)
            :val))))



(t/deftest prob-dist
  (let [prob
        (->
         (ds/->dataset {:yes [0.3 0.5] :no [0.7 0.5]})
         (ds-mod/probability-distributions->label-column :val)
         (ds-cat/reverse-map-categorical-xforms))]


    (t/is (= (:yes prob) [0.3 0.5]))
    (t/is (= (:no prob) [0.7 0.5]))
    (t/is (= (:val prob) [:no :yes]))))



(t/is (=
       (->
        (ds/->dataset {:x [:a :b] :y ["1" "0"]})
        (ds/categorical->number [:y])
        :y)

       [0.0 1.0]))




(defn- cat->num [table-args]
  (->
            (ds/->dataset {:y [ :a :b :c :d]})
            (ds/categorical->number  [:y] table-args)
            :y
            meta
            :categorical-map
            :lookup-table
            clojure.set/map-invert))


(t/deftest test-categorical->number []
  (t/is (= {5 :a, 2 :b, 0 :c, 1 :d})
        (cat->num  [[:a 5] [:b 2]]))
  (t/is (= {5 :a, 0 :b, 1 :c, 2 :d}
           (cat->num  [[:a 5] [:b 0]])))
  (t/is (= (cat->num  [])
           {0 :c, 1 :b, 2 :d, 3 :a}))
  (t/is (=
         (cat->num [[:not-present 1]])
         {1 :not-present, 0 :c, 2 :b, 3 :d, 4 :a}))
  (t/is (=
         (cat->num [[:a 1 :b 1]])
         {1 :a, 0 :c, 2 :b, 3 :d})))

(comment
  (cat->num [[:a 3.1 :b 3.2]])

  (->
   (ds/->dataset {:y [ :a :b :c :d]})
   (ds/categorical->number  [:y] [[:a 3.1 :b 3.2]])
   ((fn [ds] (hash-map :ds ds
                      :y-lookup-table (-> ds :y meta :categorical-map :lookup-table))))))
 ;; (ds-cat/reverse-map-categorical-xforms)
