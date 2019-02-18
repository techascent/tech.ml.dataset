# Math Operators

The ETL language contains a minimal and extensible tensor language.  Using this you can do simple
column-level transformations and combinations.

The canonical file for math operations is [math_ops.clj](../src/tech/ml/dataset/etl/math_ops.clj).

## Existing Math Operators

### Dataset

* `col` - if given no arguments returns the current column defined by 'column-name' in
   environment.  Else returns the column indicated by the string passed in.

### Unary

Unary operators take one double argument and return one double value.

* math - `[log1p ceil floor sqrt abs -]`
  * `log1p` (log (+ 1 val))
  * `ceil`
  * `floor`
  * `sqrt`
  * `abs`
  * `-` (unary negation)

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


### Binary

Binary operators take 2 arguments and return one double value.
* basic `[+ - * /]`
* higher-order
  * `**` - (pow x y)


## Extensions

The math subsystem is based on the [tech.compute](https://github.com/techascent/tech.compute) [tensor](https://github.com/techascent/tech.compute/blob/master/docs/tensor.md) library.


To add a new operation, you must first determine if the operation is supported by the compute tensor's cpu backend.

Unary operations are listed [here](https://github.com/techascent/tech.compute/blob/3fbcc24cc5cf3460445f066355b95779568cdaaa/src/tech/compute/cpu/tensor_math/unary_op.clj#L32).

Binary operations are listed [here](https://github.com/techascent/tech.compute/blob/3fbcc24cc5cf3460445f066355b95779568cdaaa/src/tech/compute/cpu/tensor_math/binary_op_impls.clj#L36).


### Adding op to compute.tensor

Adding a new operation that is not supported by the tensor subsystem is simple.

You can see an example of adding operations to the compute subsystem [here](https://github.com/techascent/tech.ml.dataset/blob/master/src/tech/ml/dataset/compute_math_context.clj#L10).  Basically you reify/proxy the appropriate interface and
call the appropriate compute tensor registration function with the keyword name of the operation.


### Adding op to etl/math-ops


Once your new operation is supported by the compute tensor subsystem, you can
add it with math-ops/register-math-op!.

The api of the math op is `(defn new-op [op-env & op-args] ...)`.

This takes an environment which is a map of at least `:dataset` and `:column-name` which represent the source
dataset and the result column that are being operated on.  Note that the column may not exist.
