(ns tech.v3.datatype.statistics)


(defmacro define-descriptive-stats
  []
  `(do
     ~@(->> [:skew :variance :standard-deviation :moment-3 :kurtosis :moment-4 :moment-2]
            (map (fn [tower-key]
                   (let [fn-symbol (symbol (name tower-key))]
                     `(defn ~fn-symbol
                        ([~'data ~'options]
                         (apply + ~'data ~'options)
                         ~'data)
                        ([~'data]
                         (apply + ~'data)))))))))
