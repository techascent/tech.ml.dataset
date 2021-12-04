(ns tech.v3.datatype-api)


(defmacro make-reader
  ([datatype n-elems read-op]
   `(let [~'idx ~n-elems]
       ~read-op))
  ([reader-datatype adv-datatype n-elems read-op]
   `(let [~'idx ~n-elems]
      ~read-op)))
