# tech.ml.dataset Walkthrough


Let's take a moment to walkthrough the `tech.ml.dataset system`.  This system was built
over the course of a few months in order to make working with columnar data easier
in the same manner as one would work with DataFrames in R or Pandas in Python.  While
it takes design inspiration from these sources it does not strive to be a copy in any
way but rather an extension to the core Clojure language that is built for good
performance when processing datasets of realistic sizes which in our case means
millions of rows and tens of columns.


The current backing store of the `tech.ml.dataset` abstraction is a java project
named `tablesaw`.  We tested out quite a few different storage mechanisms and
`tablesaw` came out ahead in memory usage, performance, and features such as
support for strings, dates, and missing values.


## High Level Design


Logically, a dataset is a map of column name to column data.  Column data is typed
so for instance you may have a column of 16 bit integers or 64 bit floating point
numbers.  Column names may be keywords or strings but the column values must be
strings or some subset of the java primitives due to the current underlying
implementations.  Data is stored contiguously in jvm arrays while missing values are
indicated with bitsets.  Unlike Pandas, there is currently no Object column type.


Given this definition, the intention is to allow more or less normal flows familiar
to most Clojure programmers:

1.  Dataset creation in the form of csv,tsv (and gzipped varieties of these), maps of
    column name to column values, and arbitrary sequences of maps.
1.  Pretty printing of datasets and, to a lesser extent, columns.  Simple selection of
    a given column and various functions describing the details of a column.
1.  Access to the values in a column including eliding or erroring on missing values.
1.  Select subrect of dataset defined by a sequence of columns and some sequence of
    indexes.
1.  `ds-sort-by`, `ds-filter`, `ds-group-by` are modified operations that operate on a
    logical sequence of maps and an arbitrary function but return a new dataset.
1.  Efficient elementwise operations such as linear combinations of columns.
1.  Statistical and ml-based analysis of some subset of columns either on their own
    or as they relate to another `target` column.
1.  Conversion of the dataset to sequences of maps, sequences of persistent vectors, and
    rowwise sequences of java arrays of a chosen primitive datatype.


## Dataset Creation


#### ->dataset ->>dataset

Dataset creation can happen in many ways.  For data in csv, tsv, or sequence of maps
format there are two functions that differ in where the data is passed in, `->dataset`
and `->>dataset`.  These functions several arguments:

*  A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
   ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
   is csv or tsv and then `tablesaw` has column datatype detection mechanisms which
   can be overridden.
*  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.

```clojure
user> (require '[tech.ml.dataset :as ds])
nil
user> (require '[tech.ml.dataset.column :as ds-col])
nil
user> (ds/->dataset [{:a 1 :b 2} {:a 2 :c 3}])
_unnamed [2 3]:

| :a |     :b |     :c |
|----+--------+--------|
|  1 |      2 | -32768 |
|  2 | -32768 |      3 |
```

#### name-value-seq->dataset

Given a map of name->column data produce a new dataset.  If column data is untyped
(like a persistent vector) then the column datatype is either string or double,
dependent upon the first entry of the column data sequence.

```clojure

user> (ds/name-values-seq->dataset {:age [1 2 3 4 5]
                                    :name ["a" "b" "c" "d" "e"]})
_unnamed [5 2]:

|  :age | :name |
|-------+-------|
| 1.000 |     a |
| 2.000 |     b |
| 3.000 |     c |
| 4.000 |     d |
| 5.000 |     e |
```

## Basic Dataset Manipulation

Dataset are logically maps when treated like functions and sequences of columns when
treated like sequences.

```clojure
user> (def new-ds (ds/->dataset [{:a 1 :b 2} {:a 2 :c 3}]))
#'user/new-ds
user> (first new-ds)
#tablesaw-column<int16>[2]
:a
[1, 2, ]
user> (new-ds :c)
#tablesaw-column<int16>[2]
:c
[-32768, 3, ]
[]
user> (ds-col/missing (new-ds :b))
[1]
user> (ds-col/missing (new-ds :c))
[0]
user> (first new-ds)
#tablesaw-column<int16>[2]
:a
[1, 2, ]
```

It is safe to print out very large columns.  The system will only print out the first
20 or values.  In this way it can be useful to get a feel for the data in a particular
column.


## Access To Column Values


Columns are convertible (at least) to tech.datatype readers. These derive from
java.util.List and as such allow efficient iteration and bulk copy to other
datastructures.

```clojure
user> (ds/name-values-seq->dataset {:age [1 2 3 4 5]
                                    :name ["a" "b" "c" "d" "e"]})
_unnamed [5 2]:

|  :age | :name |
|-------+-------|
| 1.000 |     a |
| 2.000 |     b |
| 3.000 |     c |
| 4.000 |     d |
| 5.000 |     e |
user> (def nameage *1)
#'user/nameage
user> (require '[tech.v2.datatype :as dtype])
nil
user> (dtype/->reader (nameage :age))
[1.0 2.0 3.0 4.0 5.0]
user> (dtype/->reader (nameage :name))
["a" "b" "c" "d" "e"]
user> (dtype/->array-copy (nameage :age))
[1.0, 2.0, 3.0, 4.0, 5.0]
user> (type *1)
[D
user> (def col-reader (dtype/->reader (nameage :age)))
#'user/col-reader
user> (col-reader 0)
1.0
user> (col-reader 1)
2.0
user> (col-reader 2)
3.0
```

In the same vein, you can access entire rows of the dataset as a reader that converts
the data either into a persistent vector in the same column-order as the dataset or
a sequence of maps with each entry named.  This type of conversion does not include
any mapping to or from labelled values so as such represented the dataset as it is
stored in memory:

```clojure
user> (ds/value-reader nameage)
[[1.0 "a"] [2.0 "b"] [3.0 "c"] [4.0 "d"] [5.0 "e"]]
user> (ds/mapseq-reader nameage)
[{:age 1.0, :name "a"} {:age 2.0, :name "b"} {:age 3.0, :name "c"} {:age 4.0, :name "d"} {:age 5.0, :name "e"}]
```

## Subrect Selection


The dataset system offers two methods to select subrects of information from the
dataset.  This results in a new dataset.

```clojure
(def ames-ds (ds/->dataset "file://data/ames-house-prices/train.csv.gz"))
#'user/ames-ds
user> (ds/column-names ames-ds)
("Id"
 "MSSubClass"
 "MSZoning"
 "LotFrontage"
 ...)
user> (ames-ds "KitchenQual")
#tablesaw-column<string>[1460]
KitchenQual
[Gd, TA, Gd, Gd, Gd, TA, Gd, TA, TA, TA, TA, Ex, TA, Gd, TA, TA, TA, TA, Gd, TA, ...]
user> (ames-ds "SalePrice")
#tablesaw-column<int32>[1460]
SalePrice
[208500, 181500, 223500, 140000, 250000, 143000, 307000, 200000, 129900, 118000, 129500, 345000, 144000, 279500, 157000, 132000, 149000, 90000, 159000, 139000, ...]

user> (ds/select ames-ds ["KitchenQual" "SalePrice"] [1 3 5 7 9])
 [5 2]:

| KitchenQual | SalePrice |
|-------------+-----------|
|          TA |    181500 |
|          Gd |    140000 |
|          TA |    143000 |
|          TA |    200000 |
|          TA |    118000 |

user> (ds/select-columns ames-ds ["KitchenQual" "SalePrice"])
 [1460 2]:

| KitchenQual | SalePrice |
|-------------+-----------|
|          Gd |    208500 |
|          TA |    181500 |
|          Gd |    223500 |
|          Gd |    140000 |
|          Gd |    250000 |
|          TA |    143000 |
|          Gd |    307000 |
|          TA |    200000 |
|          TA |    129900 |
...
```

## Add, Remove, Update

Adding or updating columns requires either a fully constructed column
`(dtype/make-container :tablesaw-column :float32 elem-seq)` or a reader
that has a type compatible with tablesaw's column system.  For this
reason you may be errors if you pass a persistent vector in to the
add-or-update method without first given it a datatype via
`(dtype/->reader [1 2 3 4] :float32)`.

```clojure
user> (require '[tech.v2.datatype.functional :as dfn])
nil
;;Log doesn't work if the incoming value isn't a float32 or a float64.  SalePrice is
;;of datatype :int32 so we convert it before going into log.
user> (ds/update-column small-ames "SalePrice" #(-> (dtype/->reader % :float64)
                                                    dfn/log))
 [5 2]:

| KitchenQual | SalePrice |
|-------------+-----------|
|          TA |    12.109 |
|          Gd |    11.849 |
|          TA |    11.871 |
|          TA |    12.206 |
|          TA |    11.678 |

user> (ds/add-or-update-column small-ames "Range" (float-array (range 5)))
 [5 3]:

| KitchenQual | SalePrice | Range |
|-------------+-----------+-------|
|          TA |    181500 | 0.000 |
|          Gd |    140000 | 1.000 |
|          TA |    143000 | 2.000 |
|          TA |    200000 | 3.000 |
|          TA |    118000 | 4.000 |

user> (ds/remove-column small-ames "KitchenQual")
 [5 1]:

| SalePrice |
|-----------|
|    181500 |
|    140000 |
|    143000 |
|    200000 |
|    118000 |
```

## Sort-by, Filter, Group-by

These are prefixed by `ds` to differentiate them from the base clojure versions but
they do conceptually the same thing with the exception that they return new datasets
as opposed to sequences.  The predicate/key-fn used by these functions are passed
sequences of maps but if you know you want to filter/sort-by/group-by a single
column it is more efficient to use the `-column` versions of these functions.

In the case of `ds-group-by-column` you then get a map of column value to
dataset container rows that had that column value.

```clojure

user> (-> (ds/ds-filter #(< 30000 (get % "SalePrice")) ames-ds)
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
 [5 2]:

| SalePrice | KitchenQual |
|-----------+-------------|
|    208500 |          Gd |
|    181500 |          TA |
|    223500 |          Gd |
|    140000 |          Gd |
|    250000 |          Gd |
user> (-> (ds/ds-sort-by #(get % "SalePrice") ames-ds)
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
 [5 2]:

| SalePrice | KitchenQual |
|-----------+-------------|
|     34900 |          TA |
|     35311 |          TA |
|     37900 |          TA |
|     39300 |          Fa |
|     40000 |          TA |
user> (def group-map (->> (ds/select ames-ds ["SalePrice" "KitchenQual"] (range 20))
                          (ds/ds-group-by #(get % "KitchenQual"))))
#'user/group-map
user> (keys group-map)
("Gd" "TA" "Ex")
user> (first group-map)
["Gd"  [7 2]:

| SalePrice | KitchenQual |
|-----------+-------------|
|    208500 |          Gd |
|    223500 |          Gd |
|    140000 |          Gd |
|    250000 |          Gd |
|    307000 |          Gd |
|    279500 |          Gd |
|    159000 |          Gd |
]
user> (def group-map (->> (ds/select ames-ds ["SalePrice" "KitchenQual"] (range 20))
                          (ds/ds-group-by-column "KitchenQual")))

#'user/group-map
user> (keys group-map)
("Gd" "TA" "Ex")
user> (first group-map)
["Gd" Gd [7 2]:

| SalePrice | KitchenQual |
|-----------+-------------|
|    208500 |          Gd |
|    223500 |          Gd |
|    140000 |          Gd |
|    250000 |          Gd |
|    307000 |          Gd |
|    279500 |          Gd |
|    159000 |          Gd |
]
```

Combining a `ds-group-by` variant with `descriptive-stats` can quickly help break down
a dataset as it relates to a categorical value:

```clojure

user> (->> (ds/select-columns ames-ds ["SalePrice" "KitchenQual" "BsmtFinSF1" "GarageArea"])
           (ds/ds-group-by-column "KitchenQual")
           (map (fn [[k v-ds]]
                  (-> (ds/descriptive-stats v-ds)
                      (ds/set-dataset-name k)))))
(Gd [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |      :mean | :mode |      :min |       :max | :standard-deviation | :skew |
|-------------+-----------+----------+------------+------------+-------+-----------+------------+---------------------+-------|
|  BsmtFinSF1 |     int32 |      586 |          0 |    456.469 |       |     0.000 |   1810.000 |             455.209 | 0.597 |
|  GarageArea |     int32 |      586 |          0 |    549.101 |       |     0.000 |   1069.000 |             174.387 | 0.227 |
| KitchenQual |    string |      586 |          0 |        NaN |    Gd |       NaN |        NaN |                 NaN |   NaN |
|   SalePrice |     int32 |      586 |          0 | 212116.031 |       | 79000.000 | 625000.000 |           64020.176 | 1.189 |
 TA [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |      :mean | :mode |      :min |       :max | :standard-deviation | :skew |
|-------------+-----------+----------+------------+------------+-------+-----------+------------+---------------------+-------|
|  BsmtFinSF1 |     int32 |      735 |          0 |    394.337 |       |     0.000 |   1880.000 |             360.215 | 0.628 |
|  GarageArea |     int32 |      735 |          0 |    394.241 |       |     0.000 |   1356.000 |             187.557 | 0.175 |
| KitchenQual |    string |      735 |          0 |        NaN |    TA |       NaN |        NaN |                 NaN |   NaN |
|   SalePrice |     int32 |      735 |          0 | 139962.516 |       | 34900.000 | 375000.000 |           38896.281 | 0.999 |
 Ex [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |      :mean | :mode |      :min |       :max | :standard-deviation |  :skew |
|-------------+-----------+----------+------------+------------+-------+-----------+------------+---------------------+--------|
|  BsmtFinSF1 |     int32 |      100 |          0 |    850.610 |       |     0.000 |   5644.000 |             799.383 |  2.144 |
|  GarageArea |     int32 |      100 |          0 |    706.430 |       |     0.000 |   1418.000 |             236.293 | -0.187 |
| KitchenQual |    string |      100 |          0 |        NaN |    Ex |       NaN |        NaN |                 NaN |    NaN |
|   SalePrice |     int32 |      100 |          0 | 328554.656 |       | 86000.000 | 755000.000 |          120862.945 |  0.937 |
 Fa [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |      :mean | :mode |      :min |       :max | :standard-deviation | :skew |
|-------------+-----------+----------+------------+------------+-------+-----------+------------+---------------------+-------|
|  BsmtFinSF1 |     int32 |       39 |          0 |    136.513 |       |     0.000 |    932.000 |             209.117 | 1.975 |
|  GarageArea |     int32 |       39 |          0 |    214.564 |       |     0.000 |    672.000 |             201.934 | 0.423 |
| KitchenQual |    string |       39 |          0 |        NaN |    Fa |       NaN |        NaN |                 NaN |   NaN |
|   SalePrice |     int32 |       39 |          0 | 105565.203 |       | 39300.000 | 200000.000 |           36004.254 | 0.242 |
)
```

## Elementwise Operations

Anything convertible to a such persisent vectors or anything deriving from
both `java.util.List` and `java.util.RandomAccess` can be converted to a reader of
any datatype.  Columns are exactly this so we can add a new column to the dataset
that is a linear combination of other columns using add-or-update-column:

```clojure
user> (require '[tech.v2.datatype.functional :as dfn])
nil
user> (def updated-ames
        (ds/add-or-update-column ames-ds
                                 "TotalBath"
                                 (dfn/+ (ames-ds "BsmtFullBath")
                                        (dfn/* 0.5 (ames-ds "BsmtHalfBath"))
                                        (ames-ds "FullBath")
                                        (dfn/* 0.5 (ames-ds "HalfBath")))))

#'user/updated-ames
user> (updated-ames "TotalBath")
#tablesaw-column<float64>[1460]
TotalBath
[3.500, 2.500, 3.500, 2.000, 3.500, 2.500, 3.000, 3.500, 2.000, 2.000, 2.000, 4.000, 2.000, 2.000, 2.500, 1.000, 2.000, 2.000, 2.500, 1.000, ...]
```

We can also create do totally dynamic operation to create a new column by implementing
the appropriate reader interface from the datatype library:
```clojure
user> (import '[tech.v2.datatype ObjectReader])
tech.v2.datatype.ObjectReader
user> (require '[tech.v2.datatype.typecast :as typecast])
nil
user> (def named-baths
        (ds/add-or-update-column
         updated-ames
         "NamedBaths"
         ;;Type out total baths so we know the datatype we are dealing with
         (let [total-baths (typecast/datatype->reader
                            :float64 (updated-ames "TotalBath"))]
           (reify ObjectReader
             (getDatatype [rdr] :string)
             (lsize [rdr] (.lsize total-baths))
             (read [rdr idx]
               (let [tbaths (.read total-baths idx)]
                 (cond
                   (< tbaths 1.0)
                   "almost none"
                   (< tbaths 2.0)
                   "somewhat doable"
                   (< tbaths 3.0)
                   "getting somewhere"
                   :else
                   "living in style")))))))


#'user/named-baths
user> (named-baths "NamedBaths")
#tablesaw-column<string>[1460]
NamedBaths
[living in style, getting somewhere, living in style, getting somewhere, living in style, getting somewhere, living in style, living in style, getting somewhere, getting somewhere, getting somewhere, living in style, getting somewhere, getting somewhere, getting somewhere, somewhat doable, getting somewhere, getting somewhere, getting somewhere, somewhat doable, ...]

;; Here we see that the higher level houses all have more bathrooms

user> (def sorted-named-baths (ds/ds-sort-by-column "SalePrice" >  named-baths))
#'user/sorted-named-baths
user> (sorted-named-baths "NamedBaths")
#tablesaw-column<string>[1460]
NamedBaths
[living in style, living in style, living in style, living in style, living in style, living in style, living in style, living in style, living in style, living in style, getting somewhere, living in style, living in style, living in style, living in style, living in style, living in style, living in style, living in style, living in style, ...]

user> (->> (sorted-named-baths "NamedBaths")
           (dtype/->reader)
           (take-last 10))
("somewhat doable"
 "getting somewhere"
 "somewhat doable"
 "somewhat doable"
 "somewhat doable"
 "somewhat doable"
 "somewhat doable"
 "somewhat doable"
 "somewhat doable"
 "somewhat doable")
```
