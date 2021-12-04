(ns tech.v3.datatype.functional-api
  (:require [clojure.set :as set]))

(def init-binary-ops
  #{:tech.numerics/hypot
    :tech.numerics/bit-xor
    :tech.numerics/unsigned-bit-shift-right
    :tech.numerics/quot
    :tech.numerics/atan2
    :tech.numerics/*
    :tech.numerics/min
    :tech.numerics/-
    :tech.numerics/pow
    :tech.numerics/bit-test
    :tech.numerics/bit-and
    :tech.numerics/rem
    :tech.numerics/max
    :tech.numerics/bit-or
    :tech.numerics//
    :tech.numerics/bit-flip
    :tech.numerics/+
    :tech.numerics/bit-shift-left
    :tech.numerics/bit-clear
    :tech.numerics/ieee-remainder
    :tech.numerics/bit-shift-right
    :tech.numerics/bit-set
    :tech.numerics/bit-and-not})

(def init-unary-ops
  #{:tech.numerics/tanh
  :tech.numerics/sq
  :tech.numerics/expm1
  :tech.numerics/log10
  :tech.numerics/cos
  :tech.numerics/tan
  :tech.numerics/atan
  :tech.numerics/sqrt
  :tech.numerics/cosh
  :tech.numerics/get-significand
  :tech.numerics/-
  :tech.numerics/next-up
  :tech.numerics/cbrt
  :tech.numerics/next-down
  :tech.numerics/exp
  :tech.numerics/log1p
  :tech.numerics//
  :tech.numerics/asin
  :tech.numerics/sinh
  :tech.numerics/rint
  :tech.numerics/+
  :tech.numerics/bit-not
  :tech.numerics/signum
  :tech.numerics/abs
  :tech.numerics/ulp
  :tech.numerics/sin
  :tech.numerics/to-radians
  :tech.numerics/acos
  :tech.numerics/ceil
  :tech.numerics/to-degrees
  :tech.numerics/identity
  :tech.numerics/logistic
  :tech.numerics/log
  :tech.numerics/floor})


(defmacro implement-arithmetic-operations
  []
  (let [binary-ops init-binary-ops
        unary-ops init-unary-ops
        dual-ops (set/intersection binary-ops unary-ops)
        unary-ops (set/difference unary-ops dual-ops)]
    `(do
       ~@(->>
          unary-ops
          (map
           (fn [opname]
             (let [op-sym (symbol (name opname))]
               `(defn ~op-sym
                  ([~'x ~'options]
                   (apply + ~'options)
                   ~'x)
                  ([~'x]
                   ~'x))))))
       ~@(->>
          binary-ops
          (map
           (fn [opname]
             (let [op-sym (symbol (name opname))
                   dual-op? (dual-ops opname)]
               (if dual-op?
                 `(defn ~op-sym
                    ([~'x]
                     ~'x)
                    ([~'x ~'y]
                     [~'x ~'y])
                    ([~'x ~'y & ~'args]
                     [~'x ~'y ~'args]))
                 `(defn ~op-sym
                    ([~'x ~'y]
                     [~'x ~'y])
                    ([~'x ~'y & ~'args]
                     [~'x ~'y ~'args]))))))))))


(def init-unary-pred-ops
  [:tech.numerics/mathematical-integer?
   :tech.numerics/even?
   :tech.numerics/infinite?
   :tech.numerics/zero?
   :tech.numerics/not
   :tech.numerics/odd?
   :tech.numerics/finite?
   :tech.numerics/pos?
   :tech.numerics/nan?
   :tech.numerics/neg?])


(defmacro implement-unary-predicates
  []
  `(do
     ~@(->> init-unary-pred-ops
           (map (fn [pred-op]
                  (let [fn-symbol (symbol (name pred-op))]
                    `(defn ~fn-symbol
                       ([~'arg ~'_options]
                        ~'arg)
                       ([~'arg]
                        ~'arg))))))))


(def init-binary-pred-ops
  [:tech.numerics/and
   :tech.numerics/or
   :tech.numerics/eq
   :tech.numerics/not-eq])


(defmacro implement-binary-predicates
  []
  `(do
     ~@(->> init-binary-pred-ops
            (map (fn [pred-op]
                   (let [fn-symbol (symbol (name pred-op))]
                     `(defn ~fn-symbol
                        [~'lhs ~'rhs]
                        (apply + ~'lhs ~'rhs)
                        ~'lhs)))))))


(def init-binary-pred-comp-ops
  [:tech.numerics/>
   :tech.numerics/>=
   :tech.numerics/<
   :tech.numerics/<=])


(defmacro implement-compare-predicates
  []
  `(do
     ~@(->> init-binary-pred-comp-ops
            (map (fn [pred-op]
                   (let [fn-symbol (symbol (name pred-op))
                         k pred-op]
                     `(defn ~fn-symbol
                        ([~'lhs ~'rhs]
                         (apply + [~'lhs ~'rhs])
                         ~'lhs)
                        ([~'lhs ~'mid ~'rhs]
                         (apply + [~'lhs ~'mid ~'rhs])
                         ~'lhs))))))))
