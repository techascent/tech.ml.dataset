(ns tech.v3.datatype.binary-op)

(defmacro make-numeric-object-binary-op
  [opname scalar-op object-op identity-value]
  `(let [~'x 1
         ~'y 2]
     ~scalar-op
     ~object-op))

(defmacro make-float-double-binary-op
  ([opname scalar-op identity-value]
   `(let [~'x 1
          ~'y 2]
      ~scalar-op))
  ([opname scalar-op]
   `(let [~'x 1
          ~'y 2]
      ~scalar-op)))

(defmacro make-int-long-binary-op
  [opname scalar-op]
  `(let [~'x 1
         ~'y 2]
     ~scalar-op))
