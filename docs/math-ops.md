# Math Operators

The ETL language contains a minimal and extensible tensor language.  Using this you can do simple
column-level transformations and combinations.

The canonical file for math operations is [math_ops.clj](../src/tech/ml/dataset/etl/math_ops.clj).

## Existing Math Operators

### Dataset

* `col` - if given no arguments returns the current column defined by 'column-name' in
   environment.  Else returns the column indicated by the string passed in.

### Mathmatics

These are the functions available for operating on columns:

```clojure

user> (tech.compute.cpu.tensor-math/all-known-operators)
{:binary (:*
          :**
          :+
          :-
          :/
          :<
          :<=
          :>
          :>=
          :and
          :atan2
          :bit-and
          :bit-and-not
          :bit-clear
          :bit-flip
          :bit-or
          :bit-set
          :bit-shift-left
          :bit-shift-right
          :bit-test
          :bit-xor
          :eq
          :hypot
          :ieee-remainder
          :max
          :min
          :not-eq
          :or
          :pow
          :quot
          :rem
          :unsigned-bit-shift-right),
 :blas [:gemm],
 :lapack (:gesvd :getrf :getrs :potrf :potrs),
 :ternary [:select],
 :unary (:-
         :/
         :abs
         :acos
         :asin
         :atan
         :bit-not
         :cbrt
         :ceil
         :cos
         :cosh
         :exp
         :expm1
         :floor
         :log
         :log10
         :log1p
         :logistic
         :next-down
         :next-up
         :noop
         :not
         :rint
         :round
         :signum
         :sin
         :sinh
         :sq
         :sqrt
         :tan
         :tanh
         :to-degrees
         :to-radians
         :ulp),
 :unary-reduce (:*
                :+
                :-
                :/
                :<
                :<=
                :>
                :>=
                :and
                :atan2
                :bit-and
                :bit-and-not
                :bit-clear
                :bit-flip
                :bit-or
                :bit-set
                :bit-shift-left
                :bit-shift-right
                :bit-test
                :bit-xor
                :eq
                :hypot
                :ieee-remainder
                :magnitude
                :magnitude-squared
                :max
                :mean
                :min
                :not-eq
                :or
                :pow
                :quot
                :rem
                :sum
                :unsigned-bit-shift-right)}
```

These are the operators available in addition that are defined for columns:

* stats - `[mean
            variance
            median
            min
            max
            skew
            kurtosis
            geometric-mean
            sum-of-squares
            sum-of-logs
            quadratic-mean
            standard-deviation
            population-variance
            sum
            product
            quartile-1
            quartile-3]`


## Extensions

The math subsystem is based on the [tech.compute](https://github.com/techascent/tech.compute) [tensor](https://github.com/techascent/tech.compute/blob/master/docs/tensor.md) library.


To add a new operation, you must first determine if the operation is supported by the compute tensor's cpu backend.


Available operations are listed [here](https://github.com/techascent/tech.compute/blob/master/src/tech/compute/cpu/math_operands.clj).


### Adding op to compute.tensor

Adding a new operation that is not supported by the tensor subsystem is simple.

You can see an example of adding operations to the compute subsystem
[here](https://github.com/techascent/tech.compute/blob/master/test/tech/compute/cpu/tensor_test.clj#L185).
Basically you reify/proxy the appropriate interface and call the appropriate compute
tensor registration function with the keyword name of the operation.


### Adding op to etl/math-ops


Once your new operation is supported by the compute tensor subsystem, you can
add it with math-ops/register-math-op!.

The api of the math op is `(defn new-op [op-env & op-args] ...)`.

This takes an environment which is a map of at least `:dataset` and `:column-name` which
represent the source dataset and the result column that are being operated on.  Note
that the column may not exist.

There are wrapper functions for adding unary or binary operations that do the necessary
dispatch to operate either in scalar space or in tensor space.
