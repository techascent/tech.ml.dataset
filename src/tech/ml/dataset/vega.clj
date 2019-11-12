(ns tech.ml.dataset.vega
  (:require [clojure.data.json :as json]
            [tech.ml.dataset :as ds]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Functions for generating vega JS specs for visualization
(defn- axis
  [& {:keys [domain grid]
      :or {domain false grid true}
      :as m}]
  (merge m {:domain domain :grid grid}))

(defn- scale
  [& {:keys [nice round type zero]
      :or {nice true round true type "linear" zero true}
      :as m}]
  (merge m {:nice nice :round round :type type :zero zero}))

(defn- scatterplot-schema
  [ds x-col y-col]
  {:$schema "https://vega.github.io/schema/vega/v5.json"
   :width 800
   :height 450
   :autosize {:type "fit" :resize true :contains "padding"}
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
            :name "marks"
            :type "symbol"}]
   :scales [(scale :domain {:data "source" :field x-col}
                   :name "x"
                   :range "width"
                   :zero false)
            (scale :domain {:data "source" :field y-col}
                   :name "y"
                   :range "height"
                   :zero false)]})

(defn scatterplot
  [ds x-col y-col]
  (->> (scatterplot-schema ds x-col y-col)
       (json/write-str)))

(comment

  (defn- clip
    [s]
    (-> (.getSystemClipboard (java.awt.Toolkit/getDefaultToolkit))
        (.setContents (java.awt.datatransfer.StringSelection. s) nil)))

  (-> (ds/->dataset "https://data.cityofchicago.org/api/views/pfsx-4n4m/rows.csv?accessType=DOWNLOAD")
      (scatterplot "Longitude" "Total Passing Vehicle Volume")
      (clip))

  ;; Then, paste into: https://vega.github.io/editor

  )
