(ns tech.v3.tensor-api)


(defn args-expansion
  [op-code-args]
  (if (symbol? op-code-args)
    [op-code-args [:a :b :c]]
       (->> op-code-args
            (mapcat (fn [argname]
                      [argname 1.0]))
            (vec))))


(defmacro typed-compute-tensor
  ([datatype advertised-datatype rank shape op-code-args op-code]
   (let [args-expansion (args-expansion op-code-args)]
     `(let ~args-expansion
        ~op-code)))
  ([advertised-datatype rank shape op-code-args op-code]
   (let [args-expansion (args-expansion op-code-args)]
     `(let ~args-expansion
        ~op-code)))
  ([_advertised-datatype _shape op-code-args op-code]
   (let [args-expansion (args-expansion op-code-args)]
     `(let ~args-expansion
        ~op-code))))
