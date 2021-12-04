(ns tech.v3.datatype.ffi)


(defmacro define-library!
  [lib-varname lib-fns _lib-symbols _error-checker]
  (let [fn-defs (second lib-fns)]
    `(do
       (def ~lib-varname :ok)
       ~@(map (fn [[fn-name fn-data]]
                (let [argvec (mapv first (:argtypes fn-data))]
                  `(defn ~(symbol (name fn-name))
                     ~argvec
                     (apply + ~argvec))))
              fn-defs))))
