# Columns, Readers, and Datatypes


In `tech.ml.dataset`, columns are composed of three things:
[data, metadata, and the missing set](https://github.com/techascent/tech.ml.dataset/blob/0ec64ac6bf5c5536491202b1abd3da7abbec1109/src/tech/ml/dataset/impl/column.clj#L141).
The column's datatype is the datatype of the `data` member.  The data member can
be anything convertible to a tech.v2.datatype reader of the appropriate type.


Readers are a simple abstraction of typed random access read-only memory.  You can
create a reader by reifying the appropriately typed interface from `tech.v2.datatype`
but the datatype library has [quick paths](https://github.com/techascent/tech.datatype/blob/5b4745f728a2773ae542fac9613ffd1c482b9750/src/tech/v2/datatype.clj#L458) to creating
these:

```clojure
user> (require '[tech.v2.datatype :as dtype])
nil
user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :float32 5 (* 2 idx))
[0.0 2.0 4.0 6.0 8.0]
```

A reader has a three methods - `getDatatype` (optional), `lsize`, and `read`.  `read`
is typed to the datatype so for instance in the example above, read returns a primitive
float object.  `lsize` returns a long.  Unlike a the similar method `get` in java
lists, the `read` method takes a long.  This allows us to use read methods on storage
mechanism capable of addressing more than 2 (signed int) or 4 (unsigned int) billion
addresses.


The dataset system in general is smart enough to create columns out of readers in most
situations.   So for instance if you have a dataset and you want a column of a
particular type, you can add-or-update-column and pass in a reader that implements what
you want:

```clojure
user> (def stocks (ds/->dataset "test/data/stocks.csv"))
#'user/stocks
user> (ds/head stocks)
test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------+------------+-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
user> (ds/head (ds/add-or-update-column stocks "id"
                                        (dtype/make-reader :int64
                                                           (ds/row-count stocks)
                                                           idx)))
test/data/stocks.csv [5 4]:

| symbol |       date | price | id |
|--------+------------+-------+----|
|   MSFT | 2000-01-01 | 39.81 |  0 |
|   MSFT | 2000-02-01 | 36.35 |  1 |
|   MSFT | 2000-03-01 | 43.22 |  2 |
|   MSFT | 2000-04-01 | 28.37 |  3 |
|   MSFT | 2000-05-01 | 25.45 |  4 |
```


There are many different datatypes currently used in the datatype system -
the primitive numeric types:
* `:boolean` - convert to and from 0 (false) or 1 (true) when used as a number.
* `:int8`,`:uint8` - signed/unsigned bytes.
* `:int16`,`:uint16` - signed/unsigned shorts.
* `:int32`,`:uint32` - signed/unsigned ints.
* `:int64` - signed longs (haven't figured out unsigned longs really yet).
* `:float32`, `float64` - floats, doubles respectively.


There are more types that can be represented by primitives (they 'alias' the primitive
type) but we will leave that for another article.

Outside of the primitive types (and types aliased to primitive types), we have an
infinite object types.  Any datatype the system doesn't understand it will treat as
type :object during generic options.


One very important aspect to note is that columns marked as `:object` datatypes will
use the **Clojure numerics stack** during mathematical operations.  This is
important because Clojure numerics stack, unlike the java or scala numerics stacks,
actively promotes values to the next appropriate size and is thus less error prone
to use if you aren't absolutely certain of your value range how it interacts with
your arithmetic pathways.


You can create a reader of any type you like and you can use any datastructure you
like as a datatype.  From the docstring on make-reader:
```clojure

-------------------------
tech.v2.datatype/make-reader
([datatype n-elems read-op])
Macro
  Make a reader.  Datatype must be a compile time visible object.
  read-op has 'idx' in scope which is the index to read from.  Returns a
  reader of the appropriate type for the passed in datatype.  Results are unchecked
  casted to the appropriate datatype.  It is up to *you* to ensure this is the result
  you want or throw an exception.

user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :boolean 5 idx)
[true true true true true]
user> (dtype/make-reader :boolean 5 (== idx 0))
[true false false false false]
user> (dtype/make-reader :float32 5 (* idx 2))
 [0.0 2.0 4.0 6.0 8.0]
user> (dtype/make-reader :any-datatype-you-wish 5 (* idx 2))
[0 2 4 6 8]
user> (dtype/get-datatype *1)
:any-datatype-you-wish
user> (dtype/make-reader [:a :b] 5 (* idx 2))
[0 2 4 6 8]
user> (dtype/get-datatype *1)
[:a :b]
```

This means you can make a column and mark it as any datatype that you like if you
are ok with object semantics.


Often times it is useful to perform an operation on one or more columns and produce
a new column.  Almost anything you think of that will result in a countable thing
will work fine (mapv, etc).  If your operation results in a primitive element per
index, it will often be very efficient to:
1.  Use a let statement to type all the inputs to your operation.
2.  Create a reader of the appropriate type that does the operation.


```clojure
user> (ds/head
       (ds/add-or-update-column
        stocks
        "price-lag"
        ;;This enables fast, typed random access to the data in the column
        (let [price-data (dtype/->typed-reader
                          (stocks "price") :float32)]
          (dtype/make-reader :float32 (.lsize price-data)
                             (.read price-data (max 0 (dec idx)))))))

test/data/stocks.csv [5 4]:

| symbol |       date | price | price-lag |
|--------+------------+-------+-----------|
|   MSFT | 2000-01-01 | 39.81 |     39.81 |
|   MSFT | 2000-02-01 | 36.35 |     39.81 |
|   MSFT | 2000-03-01 | 43.22 |     36.35 |
|   MSFT | 2000-04-01 | 28.37 |     43.22 |
|   MSFT | 2000-05-01 | 25.45 |     28.37 |
```

These columns will still work fine with the arithmetic subsystems:

```clojure
user> (require '[tech.v2.datatype.functional :as dfn])
user> (def stocks-lag
       (ds/add-or-update-column
        stocks
        "price-lag"
        ;;This enables fast, typed random access to the data in the column
        (let [price-data (dtype/->typed-reader
                          (stocks "price") :float32)]
          (dtype/make-reader :float32 (.lsize price-data)
                             (.read price-data (max 0 (dec idx)))))))

#'user/stocks-lag
user> (ds/head
       (ds/add-or-update-column
        stocks-lag
        "price-lag-diff"
        (dfn/- (stocks-lag "price")
               (stocks-lag "price-lag"))))
test/data/stocks.csv [5 5]:

| symbol |       date | price | price-lag | price-lag-diff |
|--------+------------+-------+-----------+----------------|
|   MSFT | 2000-01-01 | 39.81 |     39.81 |          0.000 |
|   MSFT | 2000-02-01 | 36.35 |     39.81 |         -3.460 |
|   MSFT | 2000-03-01 | 43.22 |     36.35 |          6.870 |
|   MSFT | 2000-04-01 | 28.37 |     43.22 |         -14.85 |
|   MSFT | 2000-05-01 | 25.45 |     28.37 |         -2.920 |
```

All these operations are intrinsically lazy, so values are only calculated when
requested.  This is usually fine but in some cases it may be desired to force
the calculation of a particular column completely (like in the instance where
the calculation is particularly expensive).  One way to force the column
efficiently is to clone it:

```clojure
user> (ds/head (ds/update-column stocks-lag "price-lag" dtype/clone))
test/data/stocks.csv [5 4]:

| symbol |       date | price | price-lag |
|--------+------------+-------+-----------|
|   MSFT | 2000-01-01 | 39.81 |     39.81 |
|   MSFT | 2000-02-01 | 36.35 |     39.81 |
|   MSFT | 2000-03-01 | 43.22 |     36.35 |
|   MSFT | 2000-04-01 | 28.37 |     43.22 |
|   MSFT | 2000-05-01 | 25.45 |     28.37 |
```

If we now get the actual type of the column's data member, we can see that it is
a concrete type.

```clojure
user> (import '[tech.ml.dataset.impl.column Column])
tech.ml.dataset.impl.column.Column
user> (def concrete (ds/update-column stocks-lag "price-lag" dtype/clone))
#'user/concrete
user> (type (.data ^Column (concrete "price-lag")))
[F
```



This ability - lazily define a column via interface implementation and still
efficiently operate on that column - that separates the implementation of
the `tech.ml.dataset` library from other libraries in this field.  This is likely
to have an interesting and different set of advantages and disadvantages that will
present themselves over time.  The dataset library is very loosely bound to the
underlying data representation allowing it to represent data that is much larger
than can fit in memory and allowing dynamic column definitions to be defined at
program runtime as equations and extensions derived from other sources of data.
