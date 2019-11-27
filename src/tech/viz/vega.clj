(ns tech.viz.vega
  "Vega vizualizations of datasets."
  (:require [clojure.data.json :as json]
            [tech.v2.datatype :as dtype]
            [tech.v2.tensor.color-gradients :as gradient]
            [tech.ml.dataset :as ds]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Functions for generating vega JS specs for visualization
(defn base-schema
  [options & {:as m}]
  (merge {:$schema "https://vega.github.io/schema/vega/v5.json"
          :autosize {:type "fit" :resize true :contains "padding"}
          :width 800 :height 450}
         options m))

(defn axis
  [& {:keys [domain grid]
      :or {domain false grid true}
      :as m}]
  (merge m {:domain domain :grid grid}))

(defn scale
  [& {:keys [nice round type zero]
      :or {nice true round true type "linear" zero true}
      :as m}]
  (merge m {:nice nice :round round :type type :zero zero}))

(defn scatterplot
  "Render a scatterplot to a vega datastructure"
  [ds x-col y-col & [options]]
  (base-schema
   options
   :axes [(axis :orient "bottom"
                :scale "x"
                :title x-col)
          (axis :orient "left"
                :scale "y"
                :title y-col)]
   :data [{:name "source"
           :values (ds/mapseq-reader (ds/select-columns ds [x-col y-col]))}]
   :marks [{:encode {:update {:fill {:value "#222"}
                              :stroke {:value "#222"}
                              :opacity {:value 0.5}
                              :shape {:value "circle"}
                              :x {:field x-col :scale "x"}
                              :y {:field y-col :scale "y"}}}
            :from {:data "source"}
            :type "symbol"}]
   :scales [(scale :domain {:data "source" :field x-col}
                   :name "x"
                   :range "width"
                   :zero false)
            (scale :domain {:data "source" :field y-col}
                   :name "y"
                   :range "height"
                   :zero false)]))


(defn scatterplot->str
  [ds x-col y-col & [options]]
  (->> (scatterplot ds x-col y-col options)
       (json/write-str)))


(defn histogram
  "Render a histograph to a vega datastructure"
  [ds col & [{:keys [bin-count] :as options}]]
  (let [raw-values (ds col)
        [minimum maximum] ((juxt #(apply min %)
                                 #(apply max %)) raw-values)
        bin-count (int (or bin-count
                           (Math/ceil (Math/log (ds/ds-row-count ds)))))
        bin-width (double (/ (- maximum minimum) bin-count))
        initial-values (->> (for [i (range bin-count)]
                              {:count 0
                               :left (+ minimum (* i bin-width))
                               :right (+ minimum (* (inc i) bin-width))})
                            (vec))
        values (->> raw-values
                    (reduce (fn [eax v]
                              (let [bin-index (min (int (quot (- v minimum)
                                                              bin-width))
                                                   (dec bin-count))]
                                (update-in eax [bin-index :count] inc)))
                            initial-values))
        color-tensors (-> (map :count values)
                          (vec)
                          (gradient/colorize :gray-yellow-tones)
                          (tech.v2.tensor/reshape [bin-count 3]))
        colors (->> color-tensors
                    (map dtype/->vector)
                    (map (fn [[b g r]] (format "#%02X%02X%02X" r g b))))
        values (map (fn [v c] (assoc v :color c)) values colors)]
    (base-schema options
     :axes [{:orient "bottom" :scale "xscale" :tickCount 5}
            {:orient "left" :scale "yscale" :tickCount 5}]
     :data [{:name "binned"
             :values values}]
     :marks [{:encode {:update
                       {:fill {:field :color}
                        :stroke {:value "#222"}
                        :x {:field :left :scale "xscale" :offset {:value 0.5}}
                        :x2 {:field :right :scale "xscale" :offset {:value 0.5}}
                        :y {:field :count :scale "yscale" :offset {:value 0.5}}
                        :y2 {:value 0 :scale "yscale" :offset {:value 0.5}}}}
              :from {:data "binned"}
              :type "rect"}]
     :scales [(scale :domain [minimum maximum]
                     :range "width"
                     :name "xscale"
                     :zero false
                     :nice false)
              (scale :domain {:data "binned" :field "count"}
                     :range "height"
                     :name "yscale")])))

(defn histogram->str
  ([ds col & [options]]
   (-> (histogram ds col options)
       (json/write-str))))


(defn time-series
  "Render a time series to a vega datastructure"
  [ds x-col y-col & [options]]
  (base-schema
   options
   :axes [{:orient "bottom" :scale "x"}
          {:orient "left" :scale "y"}]
   :data [{:name "table"
           :values (ds/mapseq-reader (ds/select-columns ds [x-col y-col]))}]
   :marks [{:encode {:enter {:stroke {:value "#222"}
                             :strokeWidth {:value 2}
                             :x {:field x-col :scale "x"}
                             :y {:field y-col :scale "y"}}}
            :from {:data "table"}
            :type "line"}]
   :scales [{:domain {:data "table" :field x-col}
             :name "x"
             :range "width"
             :type "utc"}
            (scale :domain {:data "table" :field y-col}
                   :name "y"
                   :range "height")]))

(defn time-series->str
  [ds x-col y-col & [options]]
  (->> (time-series ds x-col y-col options)
       (json/write-str)))

(comment

  (require '[tech.viz.desktop :refer [->clipboard]])

  (-> (ds/->dataset "https://data.cityofchicago.org/api/views/pfsx-4n4m/rows.csv?accessType=DOWNLOAD")
      (scatterplot->str "Longitude" "Total Passing Vehicle Volume")
      (->clipboard))

  ;;Now open https://vega.github.io/editor/ and paste.

  (-> (slurp "https://vega.github.io/vega/data/cars.json")
      (clojure.data.json/read-str :key-fn keyword)
      (ds/->dataset)
      (histogram->str :Displacement {:bin-count 15})
      (->clipboard))

  (let [ds (->> (ds/->dataset "https://vega.github.io/vega/data/stocks.csv")
                (ds/ds-filter (fn [{:strs [symbol]}] (= "MSFT" symbol))))
        sdf (java.text.SimpleDateFormat. "MMM dd yyyy")
        utc-ms (map #(.getTime (.parse sdf %)) (ds "date"))]
    (-> (ds/new-column ds "inst" utc-ms {:datatype :int64})
        (time-series->str "inst" "price")
        (->clipboard)))

  (let [ds (-> (ds/->dataset "https://vega.github.io/vega/data/seattle-temps.csv")
               (ds/select :all (range 1000)))
        sdf (java.text.SimpleDateFormat. "yyyy/MM/dd HH:mm")
        utc-ms (map #(.getTime (.parse sdf %)) (ds "date"))]
    (-> (ds/new-column ds "inst" utc-ms {:datatype :int64})
        (time-series->str "inst" "temp")
        (->clipboard)))

  ;; Then, paste into: https://vega.github.io/editor

  )
