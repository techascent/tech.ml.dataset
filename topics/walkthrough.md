# tech.ml.dataset Walkthrough


Let's take a moment to walkthrough the `tech.ml.dataset system`.  This system was built
over the course of a few months in order to make working with columnar data easier
in the same manner as one would work with `data.table` in R or `pandas` in Python.  While
it takes design inspiration from these sources it does not strive to be a copy in any
way but rather an extension to the core Clojure language that is built for good
performance when processing datasets of realistic sizes which in our case means
millions of rows and tens of columns.


## High Level Design


Logically, a dataset is a map of column name to column data.  Column data is typed
so for instance you may have a column of 16 bit integers or 64 bit floating point
numbers.  Column names may be any java object and column values may be of the
tech.datatype primitive, datetime, or objects.  Data is stored contiguously in jvm
arrays while missing values are indicated with bitsets.


Given this definition, the intention is to allow more or less normal flows familiar
to most Clojure programmers:

1.  Dataset creation in the form of csv,tsv (and gzipped varieties of these), maps of
    column name to column values, and arbitrary sequences of maps.
1.  Pretty printing of datasets and, to a lesser extent, columns.  Simple selection of
    a given column and various functions describing the details of a column.
1.  Access to the values in a column including eliding or erroring on missing values.
1.  Select subrect of dataset defined by a sequence of columns and some sequence of
    indexes.
1.  `sort-by`, `filter`, `group-by` are modified operations that operate on a
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
   is csv or tsv and then has some extensive engineering put into column datatype
   detection mechanisms which can be overridden.
*  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.

```clojure
user> (require '[tech.ml.dataset :as ds])
nil
user> (require '[tech.ml.dataset.column :as ds-col])
nil
user> (require '[tech.v2.datatype :as dtype])
nil
user> (ds/->dataset [{:a 1 :b 2} {:a 2 :c 3}])
_unnamed [2 3]:

| :a | :b | :c |
|----|----|----|
|  1 |  2 |    |
|  2 |    |  3 |
```

#### CSV/TSV/MAPSEQ/XLS/XLSX Parsing Options
It is important to note that there are many options for parsing files.
A few important ones are column whitelist/blacklists, num records,
and ways to specify exactly how to parse the string data:

* https://cljdoc.org/d/techascent/tech.ml.dataset/3.04/api/tech.ml.dataset#-%3Edataset

```clojure

user> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5})
data/ames-house-prices/train.csv [4 3]:

| SalePrice | 1stFlrSF | 2ndFlrSF |
|-----------|----------|----------|
|    208500 |      856 |      854 |
|    181500 |     1262 |        0 |
|    223500 |      920 |      866 |
|    140000 |      961 |      756 |
|    250000 |     1145 |     1053 |
user> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5
                     :parser-fn :float32})
data/ames-house-prices/train.csv [4 3]:

| SalePrice | 1stFlrSF | 2ndFlrSF |
|-----------|----------|----------|
|  208500.0 |    856.0 |    854.0 |
|  181500.0 |   1262.0 |      0.0 |
|  223500.0 |    920.0 |    866.0 |
|  140000.0 |    961.0 |    756.0 |
|  250000.0 |   1145.0 |   1053.0 |

user> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5
                     :parser-fn {"SalePrice" :float32}})
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [4 3]:

| SalePrice | 1stFlrSF | 2ndFlrSF |
|-----------|----------|----------|
|  208500.0 |      856 |      854 |
|  181500.0 |     1262 |        0 |
|  223500.0 |      920 |      866 |
|  140000.0 |      961 |      756 |
|  250000.0 |     1145 |     1053 |
```

You can also supply a tuple of `[datatype parse-fn]` if you have a specific
datatype and parse function you want to use.  For datetime types `parse-fn`
can additionally be a DateTimeFormat format string or a DateTimeFormat object:

```clojure
user> (def data (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx")))
#'user/data
user> data
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx [5 8]:

| column-0 | First Name | Last Name | Gender |       Country |  Age |       Date |     Id |
|----------|------------|-----------|--------|---------------|------|------------|--------|
|      1.0 |      Dulce |     Abril | Female | United States | 32.0 | 15/10/2017 | 1562.0 |
|      2.0 |       Mara | Hashimoto | Female | Great Britain | 25.0 | 16/08/2016 | 1582.0 |
|      3.0 |     Philip |      Gent |   Male |        France | 36.0 | 21/05/2015 | 2587.0 |
|      4.0 |   Kathleen |    Hanner | Female | United States | 25.0 | 15/10/2017 | 3549.0 |
|      5.0 |    Nereida |   Magwood | Female | United States | 58.0 | 16/08/2016 | 2468.0 |
nil
user> ;; Note the Date actually didn't parse out because it is dd/MM/yyyy format:
user> (dtype/get-datatype (data "Date"))
:string

user> (def data (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx"
                                       {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]}})))

#'user/data
user> data
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx [5 8]:

| column-0 | First Name | Last Name | Gender |       Country |  Age |       Date |     Id |
|----------|------------|-----------|--------|---------------|------|------------|--------|
|      1.0 |      Dulce |     Abril | Female | United States | 32.0 | 2017-10-15 | 1562.0 |
|      2.0 |       Mara | Hashimoto | Female | Great Britain | 25.0 | 2016-08-16 | 1582.0 |
|      3.0 |     Philip |      Gent |   Male |        France | 36.0 | 2015-05-21 | 2587.0 |
|      4.0 |   Kathleen |    Hanner | Female | United States | 25.0 | 2017-10-15 | 3549.0 |
|      5.0 |    Nereida |   Magwood | Female | United States | 58.0 | 2016-08-16 | 2468.0 |
user> (dtype/get-datatype (data "Date"))
:local-date


user> (def data (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx"
                                       {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]
                                                    "Id" :int32
                                                    0 :int32
                                                    "Age" :int16}})))
#'user/data
user> data
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx [5 8]:

| column-0 | First Name | Last Name | Gender |       Country | Age |       Date |   Id |
|----------|------------|-----------|--------|---------------|-----|------------|------|
|        1 |      Dulce |     Abril | Female | United States |  32 | 2017-10-15 | 1562 |
|        2 |       Mara | Hashimoto | Female | Great Britain |  25 | 2016-08-16 | 1582 |
|        3 |     Philip |      Gent |   Male |        France |  36 | 2015-05-21 | 2587 |
|        4 |   Kathleen |    Hanner | Female | United States |  25 | 2017-10-15 | 3549 |
|        5 |    Nereida |   Magwood | Female | United States |  58 | 2016-08-16 | 2468 |

```

A reference to what is possible is in
[parse-test](../test/tech/ml/dataset/parse_test.clj).


#### name-value-seq->dataset

Given a map of name->column data produce a new dataset.  If column data is untyped
(like a persistent vector) then the column datatype is either string or double,
dependent upon the first entry of the column data sequence.

If the column data is one of the object numeric primitive types, so
`Float` as opposed to `float`, then missing elements will be marked as
missing and the default empty-value will be used in the primitive storage.

```clojure

user> (ds/name-values-seq->dataset {:age [1 2 3 4 5]
                                    :name ["a" "b" "c" "d" "e"]})
_unnamed [5 2]:

| :age | :name |
|------|-------|
|    1 |     a |
|    2 |     b |
|    3 |     c |
|    4 |     d |
|    5 |     e |
```

## Printing


Printing out datasets comes in several flavors.  Datasets support multiline printing:
```clojure
user> (require '[tech.v2.tensor :as dtt])
nil
user> (def test-tens (dtt/->tensor (partition 3 (range 9))))
#'user/test-tens
user> (ds/->dataset [{:a 1 :b test-tens}{:a 2 :b test-tens}])
_unnamed [2 2]:
| :a |                            :b |
|----|-------------------------------|
|  1 | #tech.v2.tensor<float64>[3 3] |
|    | [[0.000 1.000 2.000]          |
|    |  [3.000 4.000 5.000]          |
|    |  [6.000 7.000 8.000]]         |
|  2 | #tech.v2.tensor<float64>[3 3] |
|    | [[0.000 1.000 2.000]          |
|    |  [3.000 4.000 5.000]          |
|    |  [6.000 7.000 8.000]]         |
```

You can provide options to control printing via the metadata of the dataset:
```clojure
user> (def tens-ds *1)
#'user/tens-ds
user> (with-meta tens-ds
        (assoc (meta tens-ds)
               :print-line-policy :single))
_unnamed [2 2]:
| :a |                            :b |
|----|-------------------------------|
|  1 | #tech.v2.tensor<float64>[3 3] |
|  2 | #tech.v2.tensor<float64>[3 3] |
```

This is especially useful when dealing with new datasets that may have large amounts
of per-column data:
```clojure
user> (require '[tech.io :as io])
nil
user> (def events-ds (-> (io/get-json "https://api.github.com/events"
                              :key-fn keyword)
                 (ds/->dataset)))
#'user/events-ds
user> (ds/head (with-meta events-ds
                 (assoc (meta events-ds)
                        :print-line-policy :single
                        :print-column-max-width 25)))
_unnamed [5 8]:
|         :id |       :type |         :actor |                     :repo |              :payload | :public |          :created_at |           :org |
|-------------|-------------|----------------|---------------------------|-----------------------|---------|----------------------|----------------|
| 12416500733 | CreateEvent |  {:id 1391351, |            {:id 62506473, |   {:ref "mix-target", |    true | 2020-05-22T18:21:21Z |                |
| 12416500729 |   PushEvent | {:id 10810283, |           {:id 266179290, | {:push_id 5115363028, |    true | 2020-05-22T18:21:21Z |                |
| 12416500724 |   PushEvent |  {:id 1036482, |            {:id 65323404, | {:push_id 5115363022, |    true | 2020-05-22T18:21:21Z | {:id 12449437, |
| 12416500717 |   ForkEvent | {:id 56911385, |           {:id 249431040, |              {:forkee |    true | 2020-05-22T18:21:21Z |                |
| 12416500714 | IssuesEvent | {:id 63518697, | {:id 247704958, :name "18 |    {:action "closed", |    true | 2020-05-22T18:21:21Z |  {:id 6233994, |
```

The full list of possible options is provided in the documentation for [dataset-data->str](https://github.com/techascent/tech.ml.dataset/blob/0ec98572dae64355ca1ab69b9209db17a810cad8/src/tech/ml/dataset/print.clj#L78).


## Basic Dataset Manipulation

Dataset are implementations of `clojure.lang.IPersistentMap`.  They strictly
respect column ordering, however, unlike persistent maps.

```clojure
user> (def new-ds (ds/->dataset [{:a 1 :b 2} {:a 2 :c 3}]))
#'user/new-ds
user> (first new-ds)
[:a #tech.ml.dataset.column<int64>[2]
:a
[1, 2, ]]
user> (new-ds :c)
#tech.ml.dataset.column<int64>[2]
:c
[, 3, ]
user> (ds-col/missing (new-ds :b))
#{1}
user> (ds-col/missing (new-ds :c))
#{0}
```

It is safe to print out very large columns.  The system will only print out the first
20 or values.  In this way it can be useful to get a feel for the data in a particular
column.


## Access To Column Values


Columns implement `clojure.lang.Indexed` (provides nth) and also implement
`clojure.lang.IFn` in the same manner as persistent vectors.

```clojure
user> (ds/->dataset {:age [1 2 3 4 5]
                     :name ["a" "b" "c" "d" "e"]})
_unnamed [5 2]:

| :age | :name |
|------|-------|
|    1 |     a |
|    2 |     b |
|    3 |     c |
|    4 |     d |
|    5 |     e |
user> (def nameage *1)
#'user/nameage
user> (require '[tech.v2.datatype :as dtype])
nil
user> (nth (nameage :age) 0)
1
user> (dtype/->reader (:age nameage))
[1 2 3 4 5]
user> (dtype/->reader (nameage :name))
["a" "b" "c" "d" "e"]
user> (dtype/->array-copy (nameage :age))
[1, 2, 3, 4, 5]
user> (type *1)
[J
user> (def name-col (nameage :age))
#'user/name-col
user> (name-col 0)
1
user> (name-col 1)
2
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
(def ames-ds (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz"))
#'user/ames-ds
user> (ds/column-names ames-ds)
("Id"
 "MSSubClass"
 "MSZoning"
 "LotFrontage"
 ...)
user> (ames-ds "KitchenQual")
#tech.ml.dataset.column<string>[1460]
KitchenQual
[Gd, TA, Gd, Gd, Gd, TA, Gd, TA, TA, TA, TA, Ex, TA, Gd, TA, TA, TA, TA, Gd, TA, ...]
user> (ames-ds "SalePrice")
#tech.ml.dataset.column<int32>[1460]
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

```clojure
user> (require '[tech.v2.datatype.functional :as dfn])
nil
user> (def small-ames (ds/head (ds/select-columns ames-ds ["KitchenQual" "SalePrice"])))
#'user/small-ames
user> small-ames
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| KitchenQual | SalePrice |
|-------------|-----------|
|          Gd |    208500 |
|          TA |    181500 |
|          Gd |    223500 |
|          Gd |    140000 |
|          Gd |    250000 |

user> (assoc small-ames "SalePriceLog" (dfn/log (small-ames "SalePrice")))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 3]:

| KitchenQual | SalePrice | SalePriceLog |
|-------------|-----------|--------------|
|          Gd |    208500 |  12.24769432 |
|          TA |    181500 |  12.10901093 |
|          Gd |    223500 |  12.31716669 |
|          Gd |    140000 |  11.84939770 |
|          Gd |    250000 |  12.42921620 |


user> (assoc small-ames "Range" (range) "Constant-Col" :a)
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 4]:

| KitchenQual | SalePrice | Range | Constant-Col |
|-------------|-----------|-------|--------------|
|          Gd |    208500 |     0 |           :a |
|          TA |    181500 |     1 |           :a |
|          Gd |    223500 |     2 |           :a |
|          Gd |    140000 |     3 |           :a |
|          Gd |    250000 |     4 |           :a |

user> (dissoc small-ames "KitchenQual")
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 1]:

| SalePrice |
|-----------|
|    208500 |
|    181500 |
|    223500 |
|    140000 |
|    250000 |
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

user> (-> (ds/filter #(< 30000 (get % "SalePrice")) ames-ds)
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
 [5 2]:

| SalePrice | KitchenQual |
|-----------+-------------|
|    208500 |          Gd |
|    181500 |          TA |
|    223500 |          Gd |
|    140000 |          Gd |
|    250000 |          Gd |

user> (-> (ds/sort-by-column "SalePrice" ames-ds)
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| SalePrice | KitchenQual |
|-----------|-------------|
|     34900 |          TA |
|     35311 |          TA |
|     37900 |          TA |
|     39300 |          Fa |
|     40000 |          TA |
user> (def group-map (->> (ds/select ames-ds ["SalePrice" "KitchenQual"] (range 20))
                          (ds/group-by #(get % "KitchenQual"))))
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
                          (ds/group-by-column "KitchenQual")))

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

Combining a `group-by` variant with `descriptive-stats` can quickly help break down
a dataset as it relates to a categorical value:

```clojure

user> (->> (ds/select-columns ames-ds ["SalePrice" "KitchenQual" "BsmtFinSF1" "GarageArea"])
           (ds/group-by-column "KitchenQual")
           (map (fn [[k v-ds]]
                  (-> (ds/descriptive-stats v-ds)
                      (ds/set-dataset-name k)))))
(Ex [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |    :min |     :mean | :mode |     :max | :standard-deviation |       :skew |
|-------------|-----------|----------|------------|---------|-----------|-------|----------|---------------------|-------------|
|  BsmtFinSF1 |    :int16 |      100 |          0 |     0.0 |    850.61 |       |   5644.0 |         799.3833216 |  2.14350280 |
|  GarageArea |    :int16 |      100 |          0 |     0.0 |    706.43 |       |   1418.0 |         236.2931861 | -0.18707598 |
| KitchenQual |   :string |      100 |          0 |         |           |    Ex |          |                     |             |
|   SalePrice |    :int32 |      100 |          0 | 86000.0 | 328554.67 |       | 755000.0 |      120862.9425733 |  0.93681387 |
 Fa [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |    :min |           :mean | :mode |     :max | :standard-deviation |      :skew |
|-------------|-----------|----------|------------|---------|-----------------|-------|----------|---------------------|------------|
|  BsmtFinSF1 |    :int16 |       39 |          0 |     0.0 |    136.51282051 |       |    932.0 |        209.11654668 | 1.97463203 |
|  GarageArea |    :int16 |       39 |          0 |     0.0 |    214.56410256 |       |    672.0 |        201.93443371 | 0.42348196 |
| KitchenQual |   :string |       39 |          0 |         |                 |    Fa |          |                     |            |
|   SalePrice |    :int32 |       39 |          0 | 39300.0 | 105565.20512821 |       | 200000.0 |      36004.25403680 | 0.24228279 |
 TA [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |    :min |           :mean | :mode |     :max | :standard-deviation |      :skew |
|-------------|-----------|----------|------------|---------|-----------------|-------|----------|---------------------|------------|
|  BsmtFinSF1 |    :int16 |      735 |          0 |     0.0 |    394.33741497 |       |   1880.0 |        360.21459000 | 0.62751158 |
|  GarageArea |    :int16 |      735 |          0 |     0.0 |    394.24081633 |       |   1356.0 |        187.55679385 | 0.17455203 |
| KitchenQual |   :string |      735 |          0 |         |                 |    TA |          |                     |            |
|   SalePrice |    :int32 |      735 |          0 | 34900.0 | 139962.51156463 |       | 375000.0 |      38896.28033636 | 0.99865115 |
 Gd [4 10]:

|   :col-name | :datatype | :n-valid | :n-missing |    :min |           :mean | :mode |     :max | :standard-deviation |      :skew |
|-------------|-----------|----------|------------|---------|-----------------|-------|----------|---------------------|------------|
|  BsmtFinSF1 |    :int16 |      586 |          0 |     0.0 |    456.46928328 |       |   1810.0 |        455.20910936 | 0.59724411 |
|  GarageArea |    :int16 |      586 |          0 |     0.0 |    549.10068259 |       |   1069.0 |        174.38742143 | 0.22683853 |
| KitchenQual |   :string |      586 |          0 |         |                 |    Gd |          |                     |            |
|   SalePrice |    :int32 |      586 |          0 | 79000.0 | 212116.02389079 |       | 625000.0 |      64020.17670212 | 1.18880409 |
)
```

#### Descriptive Stats And GroupBy And DateTime Types

This is best illustrated by an example:

```clojure
user> (def stocks (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"))
#'user/stocks
user> (ds/select stocks :all (range 5))
test/data/stocks.csv [5 3]:

| symbol |       date |  price |
|--------+------------+--------|
|   MSFT | 2000-01-01 | 39.810 |
|   MSFT | 2000-02-01 | 36.350 |
|   MSFT | 2000-03-01 | 43.220 |
|   MSFT | 2000-04-01 | 28.370 |
|   MSFT | 2000-05-01 | 25.450 |
user> (->> (ds/group-by-column "symbol" stocks)
           (map (fn [[k v]] (ds/descriptive-stats v))))
(MSFT: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |      :mean | :mode |       :min |       :max | :standard-deviation | :skew |
|-----------+--------------------+----------+------------+------------+-------+------------+------------+---------------------+-------|
|      date | :packed-local-date |      123 |          0 | 2005-01-30 |       | 1999-12-31 | 2010-02-28 |                 NaN |   NaN |
|     price |           :float32 |      123 |          0 |     24.737 |       |     15.810 |     43.220 |               4.304 | 1.166 |
|    symbol |            :string |      123 |          0 |            |  MSFT |            |            |                 NaN |   NaN |
 GOOG: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |      :mean | :mode |       :min |       :max | :standard-deviation |  :skew |
|-----------+--------------------+----------+------------+------------+-------+------------+------------+---------------------+--------|
|      date | :packed-local-date |       68 |          0 | 2007-05-17 |       | 2004-08-01 | 2010-02-28 |                 NaN |    NaN |
|     price |           :float32 |       68 |          0 |    415.870 |       |    102.370 |    707.000 |             135.070 | -0.228 |
|    symbol |            :string |       68 |          0 |            |  GOOG |            |            |                 NaN |    NaN |
 AAPL: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |      :mean | :mode |       :min |       :max | :standard-deviation | :skew |
|-----------+--------------------+----------+------------+------------+-------+------------+------------+---------------------+-------|
|      date | :packed-local-date |      123 |          0 | 2005-01-30 |       | 1999-12-31 | 2010-02-28 |                 NaN |   NaN |
|     price |           :float32 |      123 |          0 |     64.730 |       |      7.070 |    223.020 |              63.124 | 0.932 |
|    symbol |            :string |      123 |          0 |            |  AAPL |            |            |                 NaN |   NaN |
 IBM: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |      :mean | :mode |       :min |       :max | :standard-deviation | :skew |
|-----------+--------------------+----------+------------+------------+-------+------------+------------+---------------------+-------|
|      date | :packed-local-date |      123 |          0 | 2005-01-30 |       | 1999-12-31 | 2010-02-28 |                 NaN |   NaN |
|     price |           :float32 |      123 |          0 |     91.261 |       |     53.010 |    130.320 |              16.513 | 0.444 |
|    symbol |            :string |      123 |          0 |            |   IBM |            |            |                 NaN |   NaN |
 AMZN: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |      :mean | :mode |       :min |       :max | :standard-deviation | :skew |
|-----------+--------------------+----------+------------+------------+-------+------------+------------+---------------------+-------|
|      date | :packed-local-date |      123 |          0 | 2005-01-30 |       | 1999-12-31 | 2010-02-28 |                 NaN |   NaN |
|     price |           :float32 |      123 |          0 |     47.987 |       |      5.970 |    135.910 |              28.891 | 0.982 |
|    symbol |            :string |      123 |          0 |            |  AMZN |            |            |                 NaN |   NaN |
)
```

## Elementwise Operations

Anything convertible to a reader such as persisent vectors or anything deriving from
both `java.util.List` and `java.util.RandomAccess` can be converted to a reader of
any datatype.  Columns are exactly this so we can add a new column to the dataset
that is a linear combination of other columns using add-or-update-column:

```clojure
user> (def updated-ames
        (ds/add-or-update-column ames-ds
                                 "TotalBath"
                                 (dfn/+ (ames-ds "BsmtFullBath")
                                        (dfn/* 0.5 (ames-ds "BsmtHalfBath"))
                                        (ames-ds "FullBath")
                                        (dfn/* 0.5 (ames-ds "HalfBath")))))

#'user/updated-ames

user> (ds/head 10 (ds/select-columns updated-ames ["BsmtFullBath" "BsmtHalfBath" "FullBath" "HalfBath" "TotalBath"]))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [10 5]:

| BsmtFullBath | BsmtHalfBath | FullBath | HalfBath | TotalBath |
|--------------|--------------|----------|----------|-----------|
|            1 |            0 |        2 |        1 |       3.5 |
|            0 |            1 |        2 |        0 |       2.5 |
|            1 |            0 |        2 |        1 |       3.5 |
|            1 |            0 |        1 |        0 |       2.0 |
|            1 |            0 |        2 |        1 |       3.5 |
|            1 |            0 |        1 |        1 |       2.5 |
|            1 |            0 |        2 |        0 |       3.0 |
|            1 |            0 |        2 |        1 |       3.5 |
|            0 |            0 |        2 |        0 |       2.0 |
|            1 |            0 |        1 |        0 |       2.0 |
```

We can also implement a completely dynamic operation to create a new column by
implementing the appropriate reader interface from the datatype library:
```clojure
user> (def named-baths
        (assoc
         updated-ames
         "NamedBath"
         (let [total-baths (updated-ames "TotalBath")]
           (dtype/object-reader
            (count total-baths)
            (fn [idx]
               (let [tbaths (double (total-baths idx))]
                 (cond
                   (< tbaths 1.0)
                   "almost none"
                   (< tbaths 2.0)
                   "somewhat doable"
                   (< tbaths 3.0)
                   "getting somewhere"
                   :else
                   "living in style")))
            :string))))

#'user/named-baths
user> (ds/head (ds/select-columns named-baths ["TotalBath" "NamedBath"]))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| TotalBath |         NamedBath |
|-----------|-------------------|
|       3.5 |   living in style |
|       2.5 | getting somewhere |
|       3.5 |   living in style |
|       2.0 | getting somewhere |
|       3.5 |   living in style |
;; Here we see that the higher level houses all have more bathrooms

user> (ds/head (ds/select-columns sorted-named-baths ["TotalBath" "NamedBath" "SalePrice"]))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 3]:

| TotalBath |       NamedBath | SalePrice |
|-----------|-----------------|-----------|
|       4.0 | living in style |    755000 |
|       4.5 | living in style |    745000 |
|       4.5 | living in style |    625000 |
|       3.5 | living in style |    611657 |
|       3.5 | living in style |    582933 |
user> (ds/tail (ds/select-columns sorted-named-baths ["TotalBath" "NamedBath" "SalePrice"]))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 3]:

| TotalBath |       NamedBath | SalePrice |
|-----------|-----------------|-----------|
|       1.0 | somewhat doable |     40000 |
|       1.0 | somewhat doable |     39300 |
|       1.0 | somewhat doable |     37900 |
|       1.0 | somewhat doable |     35311 |
|       1.0 | somewhat doable |     34900 |
```

## DateTime Types

Support for reading datetime types and manipulating them.  Please checkout the
`tech.datatype` [datetime documentation](https://github.com/techascent/tech.datatype/blob/master/docs/datetime.md) for using this feature.


```clojure
(def stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword}))
#'user/stocks
user> (ds/head stocks)
test/data/stocks.csv [5 3]:

| :symbol |      :date | :price |
|---------|------------|--------|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
user> (dtype/get-datatype (stocks :date))
:packed-local-date

user> (require '[tech.v2.datatype.datetime.operations :as dtype-dt-ops])
nil

user> (ds/head (ds/update-column stocks :date dtype-dt-ops/get-epoch-milliseconds))
test/data/stocks.csv [5 3]:

| :symbol |                :date | :price |
|---------+----------------------+--------|
|    MSFT | 2000-01-01T00:00:00Z |  39.81 |
|    MSFT | 2000-02-01T00:00:00Z |  36.35 |
|    MSFT | 2000-03-01T00:00:00Z |  43.22 |
|    MSFT | 2000-04-01T00:00:00Z |  28.37 |
|    MSFT | 2000-05-01T00:00:00Z |  25.45 |


;;How about the yearly averages by symbol of the stocks
user> (require '[tech.v2.datatype.functional :as dfn])
nil

user> (->> (ds/add-or-update-column stocks :years (dtype-dt-ops/get-years (stocks :date)))
           (ds/group-by (juxt :symbol :years))
           (vals)
           ;;stream is a sequence of datasets at this point.
           (map (fn [ds]
                  {:symbol (first (ds :symbol))
                   :years (first (ds :years))
                   :avg-price (dfn/mean (ds :price))}))
           (sort-by (juxt :symbol :years))
           (ds/->>dataset)
           (ds/head 10))
_unnamed [10 3]:

| :symbol | :years |   :avg-price |
|---------|--------|--------------|
|    AAPL |   2000 |  21.74833333 |
|    AAPL |   2001 |  10.17583333 |
|    AAPL |   2002 |   9.40833333 |
|    AAPL |   2003 |   9.34750000 |
|    AAPL |   2004 |  18.72333333 |
|    AAPL |   2005 |  48.17166667 |
|    AAPL |   2006 |  72.04333333 |
|    AAPL |   2007 | 133.35333333 |
|    AAPL |   2008 | 138.48083333 |
|    AAPL |   2009 | 150.39333333 |
```

## Joins

We now have experimental support for joins.  This is a left-hash-join algorithm so
the current algorithm is -
1.  `(tech.ml.dataset.functional/arggroup-by-int (lhs lhs-colname))` - this returns
	a map of column-value->index-int32-array-list.
2.  Run through `(rhs rhs-colname)` finding values in the group-by hash map and
    building out left and right hand side final indexes.
3.  Using appropriate final indexes, select columns from left and right hand sides.

Colname can be value in which case both datasets must contain that column or it
may be a tuple in which case it will be destructured like:
`(let [[lhs-colname rhs-colname] colname] ...)`

```clojure
user> (def test-ds
        (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5}))
#'user/test-ds
user> test-ds
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [4 3]:

| SalePrice | 1stFlrSF | 2ndFlrSF |
|-----------+----------+----------|
|    208500 |      856 |      854 |
|    181500 |     1262 |        0 |
|    223500 |      920 |      866 |
|    140000 |      961 |      756 |

user> (ds/inner-join "1stFlrSF"
                     (ds/set-dataset-name test-ds "left")
                     (ds/set-dataset-name test-ds "right"))
inner-join [4 5]:

| 1stFlrSF | SalePrice | 2ndFlrSF | right.SalePrice | right.2ndFlrSF |
|----------+-----------+----------+-----------------+----------------|
|      856 |    208500 |      854 |          208500 |            854 |
|     1262 |    181500 |        0 |          181500 |              0 |
|      920 |    223500 |      866 |          223500 |            866 |
|      961 |    140000 |      756 |          140000 |            756 |

user> (ds/right-join ["1stFlrSF" "2ndFlrSF"]
                     (ds/set-dataset-name test-ds "left")
                     (ds/set-dataset-name test-ds "right"))
right-outer-join [5 6]:

| 1stFlrSF | SalePrice | 2ndFlrSF | right.2ndFlrSF | right.SalePrice | right.1stFlrSF |
|----------|-----------|----------|----------------|-----------------|----------------|
|          |           |          |            854 |          208500 |            856 |
|          |           |          |              0 |          181500 |           1262 |
|          |           |          |            866 |          223500 |            920 |
|          |           |          |            756 |          140000 |            961 |
|          |           |          |           1053 |          250000 |           1145 |


user> (ds/left-join ["1stFlrSF" "2ndFlrSF"]
                     (ds/set-dataset-name test-ds "left")
                     (ds/set-dataset-name test-ds "right"))
left-outer-join [5 6]:

| 1stFlrSF | SalePrice | 2ndFlrSF | right.2ndFlrSF | right.SalePrice | right.1stFlrSF |
|----------|-----------|----------|----------------|-----------------|----------------|
|      961 |    140000 |      756 |                |                 |                |
|      920 |    223500 |      866 |                |                 |                |
|      856 |    208500 |      854 |                |                 |                |
|     1145 |    250000 |     1053 |                |                 |                |
|     1262 |    181500 |        0 |                |                 |                |
```


## XLS, XLSX files

We use apache poi [directly](../src/tech/libs/poi/parse.clj) to generate datasets
from xls and xlsx files.  This feature is, like joins, very new.

```clojure
user> (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"))
Sheet1 [5 8]:

|     0 | First Name | Last Name | Gender |       Country |   Age |       Date |   Id |
|-------+------------+-----------+--------+---------------+-------+------------+------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.00 | 15/10/2017 | 1562 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.00 | 16/08/2016 | 1582 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.00 | 21/05/2015 | 2587 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.00 | 15/10/2017 | 3549 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.00 | 16/08/2016 | 2468 |
```

In this example, we actually failed to parse the date because it is in an international
format (day-month-year) and the dataset system automatically defaults to the American
(month-day-year).  In this case you actually have 2 options.  You can reload the entire
file and specify a datatype and DateTimeFormatter format for the column:

```clojure

user> (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"
                             {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]}}))
Sheet1 [5 8]:
|     0 | First Name | Last Name | Gender |       Country |   Age |       Date |   Id |
|-------|------------|-----------|--------|---------------|-------|------------|------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.00 | 2017-10-15 | 1562 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.00 | 2016-08-16 | 1582 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.00 | 2015-05-21 | 2587 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.00 | 2017-10-15 | 3549 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.00 | 2016-08-16 | 2468 |
```


Or you can reparse just that column using the above parse syntax:

```clojure
user> (require '[tech.ml.dataset.column :as ds-col])
nil
user> (def unparsed (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"))
#'user/unparsed
user> (ds/head (ds/update-column unparsed "Date"
                                 (partial ds-col/parse-column [:local-date "dd/MM/yyyy"])))
Sheet1 [5 8]:

|     0 | First Name | Last Name | Gender |       Country |   Age |       Date |   Id |
|-------+------------+-----------+--------+---------------+-------+------------+------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.00 | 2017-10-15 | 1562 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.00 | 2016-08-16 | 1582 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.00 | 2015-05-21 | 2587 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.00 | 2017-10-15 | 3549 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.00 | 2016-08-16 | 2468 |
```


## Writing A Dataset Out

These forms are supported for writing out a dataset:
```clojure
(ds/write-csv! test-ds "test.csv")
(ds/write-csv! test-ds "test.tsv")
(ds/write-csv! test-ds "test.tsv.gz")
(ds/write-csv! test-ds out-stream)
```

If you want to use your own serialization system, then converting the dataset to
a sequence of maps presents a slow but effective way forward:

- pure csv
```clojure
(tech.io/mapseq->csv! "file://test.csv" (dataset/mapseq-reader test-ds ))
```

- tsv
```clojure
   (io/mapseq->csv! "file://test.tsv"
                    (dataset/mapseq-reader test-ds )
                    :separator \tab)
```

- gzipped tsv
```clojure
user> (with-open [outs (io/gzip-output-stream! "file://test.tsv.gz")]
        (io/mapseq->csv!
         (dataset/mapseq-reader test-ds )
         :separator \tab))
```

We also have support for [nippy](nippy-serialization-rocks.md) in which case
datasets work just like any other datastructure.  This format allows some level of
compression but about 10X-100X the loading performance of anything else.

```clojure
user> (require '[taoensso.nippy :as nippy])
nil
user> (def stocks-data (nippy/freeze stocks))
#'user/stocks-data
user> (type stocks-data)
[B
user> (ds/head (nippy/thaw stocks-data))
test/data/stocks.csv [5 3]:

| :symbol |      :date | :price |
|---------|------------|--------|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
```
