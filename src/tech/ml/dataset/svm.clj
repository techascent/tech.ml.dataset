(ns tech.ml.dataset.svm
  (:require [clojure.string :as s]))


(def test-line "1 1:2.617300e+01 2:5.886700e+01 3:-1.894697e-01 4:1.251225e+02")


(defn- parse-line
  [line]
  (let [parse (s/split line #"\s+")
        label (Double/parseDouble (first parse))
        idx-val-pairs (->> (rest parse)
                           (map (fn [item]
                                  (let [[idx val] (s/split item #":")]
                                    [(Long/parseLong idx)
                                     (Double/parseDouble val)]))))]
    {:label label
     :features (->> idx-val-pairs
                    (into {}))
     :max-idx (apply max (map first idx-val-pairs))
     :min-idx (apply min (map first idx-val-pairs))}))


(defn parse-svm-file
  "Parse an svm file and generate a dataset of {:features :label}.  Always
  represents result as a dense matrix; no support for sparse datasets."
  [fname & {:keys [label-map]}]
  (let [f-data (slurp fname)
        [labels features min-idx max-idx]
        (->> (s/split f-data #"\n")
             (pmap parse-line)
             (reduce
              (fn [[labels features min-idx max-idx] next-line]
                (let [{label :label
                       line-feature :features
                       line-max-idx :max-idx
                       line-min-idx :min-idx
                       :as item} next-line]
                  [(conj labels (if label-map
                                  (if-let [label-value (get label-map label)]
                                    label-value
                                    (throw (ex-info (format "Failed to find label for value: %s" label)
                                                    {:label-map label-map})))
                                  label))
                   (conj features (->> line-feature
                                       (map (fn [[k v]]
                                              [(str k) v]))
                                       (into {})))
                   (if (and min-idx
                            (< min-idx line-min-idx))
                     min-idx
                     line-min-idx)
                   (if (and max-idx
                            (> max-idx line-max-idx))
                     max-idx
                     line-max-idx)]))
              [[] [] nil nil]))
        min-idx (long min-idx)
        max-idx (long max-idx)
        num-items (+ 1 (- max-idx min-idx))]
    {:min-idx min-idx
     :max-idx max-idx
     :feature-ecount (+ 1 (- max-idx min-idx))
     :dataset (map (fn [label features]
                     (assoc features :label label))
                   labels features)}))
