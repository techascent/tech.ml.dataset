# Columns, Readers, and Datatypes


In `tech.ml.dataset`, columns are composed of three things:
[data, metadata, and the missing set](https://github.com/techascent/tech.ml.dataset/blob/7c8c7514e0e35995050c1e326122a1826cc18273/src/tech/v3/dataset/impl/column.clj#L140).
The column's datatype is the datatype of the `data` member.  The data member can
be anything convertible to a tech.v2.datatype reader of the appropriate type.


Buffers are a [simple abstraction](https://github.com/cnuernber/dtype-next/blob/152f09f925041d41782e05009bbf84d7d6cfdbc6/java/tech/v3/datatype/Buffer.java) of typed random access read-only
memory that implement all the interfaces required to both efficient and easy to use.
You can create a buffer by reifying the appropriately typed interface from
`tech.v3.datatype` but the datatype library has
[quick paths](https://github.com/cnuernber/dtype-next/blob/152f09f925041d41782e05009bbf84d7d6cfdbc6/src/tech/v3/datatype.clj#L102) to creating these:

```clojure
user> (require '[tech.v3.datatype :as dtype])
nil
user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :float32 5 (* 2 idx))
[0.0 2.0 4.0 6.0 8.0]
```



A read-only buffer only needs three methods - `elemwiseDatatype` (optional), `lsize`, and
`read[X]`.  `read[X]` is typed to the datatype so for instance in the example above,
readFloat returns a primitive float object.  `lsize` returns a long.  Unlike a the
similar method `get` in java lists, the `read[X]` methods takes a long.  This allows us
to use read methods on storage mechanism capable of addressing more than 2 (signed int)
or 4 (unsigned int) billion addresses.


Another way to create a reader is to do a 'map' type translation from one or more other
readers.  This is provided in two ways:

* [`dtype/emap`](https://github.com/cnuernber/dtype-next/blob/152f09f925041d41782e05009bbf84d7d6cfdbc6/src/tech/v3/datatype/emap.clj#L97) - Missing set ignorant mapping into a typed representation.
* [`tech.v3.dataset.column/column-map`](https://github.com/techascent/tech.ml.dataset/blob/7c8c7514e0e35995050c1e326122a1826cc18273/src/tech/v3/dataset/column.clj#L174) - Missing set aware mapping into a typed representation.


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
use the Clojure numerics stack during mathematical operations.  This is
important because Clojure number tower, similar to the APL number tower,
actively promotes values to the next appropriate size and is thus less error prone
to use if you aren't absolutely certain of your value range how it interacts with
your arithmetic pathways.


```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (def stocks (ds/->dataset "test/data/stocks.csv"))
#'user/stocks
user> (require '[tech.v3.datatype.functional :as dfn])
nil
user> (def stocks-lag
        (assoc stocks "price-lag"
               (let [price-data (dtype/->reader (stocks "price"))]
                 (dtype/make-reader :float64 (.lsize price-data)
                                    (.readDouble price-data
                                                 (max 0 (dec idx)))))))

#'user/stocks-lag
user> (ds/head (assoc stocks-lag "price-lag-diff" (dfn/- (stocks-lag "price")
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
user> (-> (ds/update-column stocks-lag "price-lag" dtype/clone)
          (get "price-lag")
          (dtype/as-concrete-buffer))
#array-buffer<float64>[560]
[39.81, 39.81, 36.35, 43.22, 28.37, 25.45, 32.54, 28.40, 28.40, 24.53, 28.02, 23.34, 17.65, 24.84, 24.00, 22.25, 27.56, 28.14, 29.70, 26.93, ...]
```


This ability - lazily define a column via interface implementation and still
efficiently operate on that column - separates the implementation of
the `tech.ml.dataset` library from other libraries in this field.  This is likely
to have an interesting and different set of advantages and disadvantages that will
present themselves over time.  The dataset library is very loosely bound to the
underlying data representation allowing it to represent data that is much larger
than can fit in memory and allowing dynamic column definitions to be defined at
program runtime as equations and extensions derived from other sources of data.
