(ns tech.v3.datatype.unary-op)


(defmacro make-double-unary-op
  [_opname opcode]
  `(let [~'x 1.0]
     ~opcode))


(defmacro make-numeric-object-unary-op
  [_opname opcode]
  `(let [~'x 1.0]
     ~opcode))


(defmacro make-float-double-unary-op
  [_opname opcode]
  `(let [~'x 1.0]
     ~opcode))


(defmacro make-numeric-unary-op
  [_opname opcode]
  `(let [~'x 1.0]
     ~opcode))


(defmacro make-long-unary-op
  [_opname opcode]
  `(let [~'x 1]
     ~opcode))


(defmacro make-all-datatype-unary-op
  [_opname opcode]
  `(let [~'x 1]
     ~opcode))
