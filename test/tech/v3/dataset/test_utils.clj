(ns tech.v3.dataset.test-utils
  (:require [tech.v3.io :as io]
            [clojure.string :as s]
            [tech.v3.dataset :as ds]
            [camel-snake-kebab.core :refer [->kebab-case]]))

(defn load-mapseq-fruit-dataset
  []
  (let [fruit-ds (slurp (io/input-stream "test/data/fruit_data_with_colors.txt"))
        dataset (->> (s/split fruit-ds #"\n")
                     (mapv #(s/split % #"\s+")))
        ds-keys (->> (first dataset)
                     (mapv (comp keyword ->kebab-case)))]
    (->> (rest dataset)
         (map (fn [ds-line]
                (->> ds-line
                     (map (fn [ds-val]
                            (try
                              (Double/parseDouble ^String ds-val)
                              (catch Throwable e
                                (-> (->kebab-case ds-val)
                                    keyword)))))
                     (zipmap ds-keys))))
         (ds/->dataset))))

(def mapseq-fruit-dataset (memoize load-mapseq-fruit-dataset))
