(ns tech.v3.parallel.for)


(defmacro doiter
  [varname iterable & body]
  `(let [~varname ~iterable]
     ~@body))


(defmacro parallel-for
  [idx-var n-elems & body]
  `(let [~idx-var ~n-elems]
     ~@body))
