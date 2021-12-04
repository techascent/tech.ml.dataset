(ns tech.v3.datatype.binary-pred)



(defmacro make-boolean-predicate
  [opname op]
  `(let [~'x 1
         ~'y 2]
     ~op))


(defmacro make-numeric-binary-predicate
  [opname scalar-op object-op]
  `(let [~'x 1
         ~'y 2]
     ~scalar-op
     ~object-op))
