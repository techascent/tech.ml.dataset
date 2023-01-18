# tech.ml.dataset Walkthrough

`tech.ml.dataset` (TMD) is a Clojure library designed to ease working with tabular data, similar to `data.table` in R or Python's Pandas. TMD takes inspiration from the design of those tools, but does not aim to copy their functionality. Instead, TMD is a building block that increases Clojure's already considerable prowess for data processing tasks.

## High Level Design

Logically, a dataset is a map of column name to column data. Column data is typed (e.g., a column of 16 bit integers, or a column of 64 bit floating point numbers), similar to a database. Column names may be any Java object - keywords and strings are typical - and column values may be any Java primitive type, or type supported by `tech.datatype`, datetimes, or arbitrary objects. Column data is stored contiguously in JVM arrays, and missing values are indicated with bitsets.

The outline of the walkthrough follows typical Clojure workflows, showing how those ideas are expanded by TMD:

1. Dataset creation from typical formats (csv, tsv, etc...), or sequences of maps, or maps of column name to column values.
1. REPL-friendly printing of datasets
1. Access to dataset values, including eliding or erroring on missing values.
1. Selecting rows or columns of a dataset, or both at once.
1. Functions familiar from `clojure.core` (e.g., `sort-by`, `filter`, `group-by`, etc...) that operate on datasets.
1. Efficient elementwise operations and their use in adding or updating columns derived from data in the dataset.
1. Statistical analysis of dataset data.
1. Getting data back out of datasets for downstream consumers (via file/stream export with broad format support, or as sequences of maps or vectors or Java arrays).

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
user> (require '[tech.v3.dataset :as ds])
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

* https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var--.3Edataset

```clojure

user> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5})
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 3]:

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
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 3]:

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
user> (require '[tech.v3.libs.fastexcel])
nil
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

user> ;; Note the Date actually didn't parse out because it is dd/MM/yyyy format:
user> (meta (data "Date"))
{:categorical? true, :name "Date", :datatype :string, :n-elems 5}


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
user> (meta (data "Date"))
{:name "Date", :datatype :local-date, :n-elems 5}
user> (nth (data "Date") 0)
#object[java.time.LocalDate 0x6c88bf34 "2017-10-15"]


user> (def data (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLSX_1000.xlsx"
                                       {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]
                                                    "Id" :int32
                                                    "column-0" :int32
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

#### Map Of Columns Format

Given a map of name->column data produce a new dataset.  If column data is untyped
(like a persistent vector) then the column datatype is either string or double,
dependent upon the first entry of the column data sequence.

If the column data is one of the object numeric primitive types, so
`Float` as opposed to `float`, then missing elements will be marked as
missing and the default empty-value will be used in the primitive storage.

```clojure

user> (ds/->dataset {:age [1 2 3 4 5] :name ["a" "b" "c" "d" "e"]})
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
user> (require '[tech.v3.tensor :as dtt])
nil
user> (def test-tens (dtt/->tensor (partition 3 (range 9))))
#'user/test-tens
user> (ds/->dataset [{:a 1 :b test-tens}{:a 2 :b test-tens}])
_unnamed [2 2]:

| :a |                           :b |
|----|------------------------------|
|  1 | #tech.v3.tensor<object>[3 3] |
|    | [[0 1 2]                     |
|    |  [3 4 5]                     |
|    |  [6 7 8]]                    |
|  2 | #tech.v3.tensor<object>[3 3] |
|    | [[0 1 2]                     |
|    |  [3 4 5]                     |
|    |  [6 7 8]]                    |
```

You can provide options to control printing via the metadata of the dataset:
```clojure
user> (def tens-ds *1)
#'user/tens-ds
user> (with-meta tens-ds
        (assoc (meta tens-ds)
               :print-line-policy :single))
_unnamed [2 2]:

| :a |                           :b |
|----|------------------------------|
|  1 | #tech.v3.tensor<object>[3 3] |
|  2 | #tech.v3.tensor<object>[3 3] |
```

This is especially useful when dealing with new datasets that may have large amounts
of per-column data:
```clojure
user> (def events-ds (-> (ds/->dataset "https://api.github.com/events"
                                       {:key-fn keyword
                                        :file-type :json})
                         (vary-meta assoc :print-line-policy :single
                                    :print-column-max-width 25)))
#'user/events-ds
user> (ds/head events-ds)
https://api.github.com/events [5 8]:

|         :id |                  :type |         :actor |           :repo |              :payload | :public |          :created_at |           :org |
|-------------|------------------------|----------------|-----------------|-----------------------|---------|----------------------|----------------|
| 13911736787 |              PushEvent | {:id 29139614, | {:id 253259114, | {:push_id 5888739305, |    true | 2020-10-20T17:49:36Z |                |
| 13911736794 |            IssuesEvent | {:id 47793873, | {:id 240806054, |    {:action "opened", |    true | 2020-10-20T17:49:36Z | {:id 61098177, |
| 13911736759 |              PushEvent | {:id 71535163, | {:id 304746399, | {:push_id 5888739282, |    true | 2020-10-20T17:49:36Z |                |
| 13911736795 | PullRequestReviewEvent | {:id 47063667, | {:id 305218173, |   {:action "created", |    true | 2020-10-20T17:49:36Z |                |
| 13911736760 |              PushEvent | {:id 22623307, | {:id 287289752, | {:push_id 5888739280, |    true | 2020-10-20T17:49:36Z |                |
```

The full list of possible options is provided in the documentation for [dataset-data->str](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.print.html).


## Basic Dataset Manipulation

Dataset are implementations of `clojure.lang.IPersistentMap`.  They strictly
respect column ordering, however, unlike persistent maps.

```clojure

user> (def new-ds (ds/->dataset [{:a 1 :b 2} {:a 2 :c 3}]))
#'user/new-ds
user> new-ds
_unnamed [2 3]:

| :a | :b | :c |
|----|----|----|
|  1 |  2 |    |
|  2 |    |  3 |
user> (first new-ds)
[:a #tech.v3.dataset.column<int64>[2]
:a
[1, 2, ]]
user> (new-ds :c)
#tech.v3.dataset.column<int64>[2]
:c
[, 3, ]
user> (ds/missing (new-ds :b))
{1}
user> (ds/missing (new-ds :c))
{0}
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
user> (require '[tech.v3.datatype :as dtype])
nil
user> (dtype/->array-copy (nameage :age))
[1, 2, 3, 4, 5]
user> (type *1)
[J
user> (def namecol (nameage :age))
#'user/namecol
user> (namecol 0)
1
user> (namecol 1)
2
```

In the same vein, you can access entire rows of the dataset as a reader that converts
the data either into a persistent vector in the same column-order as the dataset or
a sequence of maps with each entry named.  This type of conversion does not include
any mapping to or from labelled values so as such represented the dataset as it is
stored in memory:

```clojure
user> (ds/rowvecs nameage)
[[1 "a"] [2 "b"] [3 "c"] [4 "d"] [5 "e"]]
user> (ds/rows nameage)
[{:name "a", :age 1} {:name "b", :age 2} {:name "c", :age 3} {:name "d", :age 4} {:name "e", :age 5}]
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
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| KitchenQual | SalePrice |
|-------------|-----------|
|          TA |    181500 |
|          Gd |    140000 |
|          TA |    143000 |
|          TA |    200000 |
|          TA |    118000 |

user> (ds/head (ds/select-columns ames-ds ["KitchenQual" "SalePrice"]))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| KitchenQual | SalePrice |
|-------------|-----------|
|          Gd |    208500 |
|          TA |    181500 |
|          Gd |    223500 |
|          Gd |    140000 |
|          Gd |    250000 |
```

## Add, Remove, Update

```clojure
user> (require '[tech.v3.datatype.functional :as dfn])
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

These functions do conceptually the same thing but the dataset is the *first* argument
so when we build large pipelines of dataset functionaly we don't have to switch the
argument orders.  They have per-column versions that are more efficient than the
whole-dataset versions.

The whole-dataset version pass in each row as a map so it is conceptually similar to doing
something like `(->> (ds/mapseq-reader ds) (clojure.core/filter pred))`.

```clojure
user> (-> ames-ds
          (ds/filter-column "SalePrice" #(< 30000 %))
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| SalePrice | KitchenQual |
|-----------|-------------|
|    208500 |          Gd |
|    181500 |          TA |
|    223500 |          Gd |
|    140000 |          Gd |
|    250000 |          Gd |
user> ;;Using full dataset version of filter
user> (-> ames-ds
          (ds/filter #(< 30000 (get % "SalePrice")))
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| SalePrice | KitchenQual |
|-----------|-------------|
|    208500 |          Gd |
|    181500 |          TA |
|    223500 |          Gd |
|    140000 |          Gd |
|    250000 |          Gd |


user> (-> (ds/sort-by-column ames-ds "SalePrice")
          (ds/select ["SalePrice" "KitchenQual"] (range 5)))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 2]:

| SalePrice | KitchenQual |
|-----------|-------------|
|     34900 |          TA |
|     35311 |          TA |
|     37900 |          TA |
|     39300 |          Fa |
|     40000 |          TA |


user> (def group-map (-> (ds/select ames-ds ["SalePrice" "KitchenQual"] (range 20))
                         (ds/group-by-column "KitchenQual")))
#'user/group-map
user> (keys group-map)
("Ex" "TA" "Gd")
user> (first group-map)
["Ex" Ex [1 2]:

| SalePrice | KitchenQual |
|-----------|-------------|
|    345000 |          Ex |
]
```

Combining a `group-by` variant with `descriptive-stats` can quickly help break down
a dataset as it relates to a categorical value:

```clojure
user> (as-> (ds/select-columns ames-ds ["SalePrice" "KitchenQual" "BsmtFinSF1" "GarageArea"]) ds
        (ds/group-by-column ds "KitchenQual")
        (map (fn [[k v-ds]]
               (-> (ds/descriptive-stats v-ds)
                   (ds/set-dataset-name k))) ds))
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
|   SalePrice |    :int32 |      586 |          0 | 79000.0 | 212116.02389078 |       | 625000.0 |      64020.17670212 | 1.18880409 |
)
```


#### Rowwise Operations

Datasets have efficient parallelized mechanisms of presenting data for rowwise map and mapcat
operations.  The maps passed into the mapping functions are maps that lazily read
only the required data from the underlying dataset.  The returned maps will be
scanned to gather datatype and missing information.  Columns derived from the mapping
operation will overwrite columns in the original dataset.

The mapping operations are run in parallel using a primitive named `pmap-ds` and the resulting
datasets can either be returned in a sequence or combined into a single larger dataset.

* [rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rows)
* [rowvecs](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rowvecs)
* [row-map](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-map)
* [row-mapcat](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-mapcat)

#### Descriptive Stats And GroupBy And DateTime Types

This is best illustrated by an example:

```clojure
user> (def stocks (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"))
#'user/stocks
user> (ds/select-rows stocks (range 5))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------|------------|-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |

user> (->> (ds/group-by-column stocks "symbol")
           (map (fn [[k v]] (ds/descriptive-stats v))))
(MSFT: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |      :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|------------|
|      date | :packed-local-date |      123 |          0 | 2000-01-01 | 2005-01-30 |       | 2010-03-01 |      9.37554538E+10 | 0.00025335 |
|     price |           :float64 |      123 |          0 |      15.81 |      24.74 |       |      43.22 |      4.30395786E+00 | 1.16559225 |
|    symbol |            :string |      123 |          0 |            |            |  MSFT |            |                     |            |
 GOOG: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |       :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|-------------|
|      date | :packed-local-date |       68 |          0 | 2004-08-01 | 2007-05-17 |       | 2010-03-01 |      5.20003989E+10 |  0.00094625 |
|     price |           :float64 |       68 |          0 |      102.4 |      415.9 |       |      707.0 |      1.35069851E+02 | -0.22776524 |
|    symbol |            :string |       68 |          0 |            |            |  GOOG |            |                     |             |
 AAPL: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |      :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|------------|
|      date | :packed-local-date |      123 |          0 | 2000-01-01 | 2005-01-30 |       | 2010-03-01 |      9.37554538E+10 | 0.00025335 |
|     price |           :float64 |      123 |          0 |      7.070 |      64.73 |       |      223.0 |      6.31237823E+01 | 0.93215285 |
|    symbol |            :string |      123 |          0 |            |            |  AAPL |            |                     |            |
 IBM: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |      :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|------------|
|      date | :packed-local-date |      123 |          0 | 2000-01-01 | 2005-01-30 |       | 2010-03-01 |      9.37554538E+10 | 0.00025335 |
|     price |           :float64 |      123 |          0 |      53.01 |      91.26 |       |      130.3 |      1.65133647E+01 | 0.44446266 |
|    symbol |            :string |      123 |          0 |            |            |   IBM |            |                     |            |
 AMZN: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |      :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|------------|
|      date | :packed-local-date |      123 |          0 | 2000-01-01 | 2005-01-30 |       | 2010-03-01 |      9.37554538E+10 | 0.00025335 |
|     price |           :float64 |      123 |          0 |      5.970 |      47.99 |       |      135.9 |      2.88913206E+01 | 0.98217538 |
|    symbol |            :string |      123 |          0 |            |            |  AMZN |            |                     |            |
)
```

## Elementwise Operations

The datatype system includes a mathematical abstraction that is designed to work with
things like columns.  Using this we can create new columns that are lazily evaluated
linear combinations of other columns.

```clojure
user> (def updated-ames
        (assoc ames-ds
               "TotalBath"
               (dfn/+ (ames-ds "BsmtFullBath")
                      (dfn/* 0.5 (ames-ds "BsmtHalfBath"))
                      (ames-ds "FullBath")
                      (dfn/* 0.5 (ames-ds "HalfBath")))))
#'user/updated-ames

user> (ds/head (ds/select-columns updated-ames ["BsmtFullBath" "BsmtHalfBath" "FullBath" "HalfBath" "TotalBath"]))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 5]:

| BsmtFullBath | BsmtHalfBath | FullBath | HalfBath | TotalBath |
|--------------|--------------|----------|----------|-----------|
|            1 |            0 |        2 |        1 |       3.5 |
|            0 |            1 |        2 |        0 |       2.5 |
|            1 |            0 |        2 |        1 |       3.5 |
|            1 |            0 |        1 |        0 |       2.0 |
|            1 |            0 |        2 |        1 |       3.5 |
```

The datatype library contains a typed elementwise-map function named 'emap'
that allows us to do define arbitrary conversions from columns into columns:

```clojure
user> (def named-baths (assoc updated-ames "NamedBath" (dtype/emap #(let [tbaths (double %)]
                                                                      (cond
                                                                        (< tbaths 1.0)
                                                                        "almost none"
                                                                        (< tbaths 2.0)
                                                                        "somewhat doable"
                                                                        (< tbaths 3.0)
                                                                        "getting somewhere"
                                                                        :else
                                                                        "living in style"))
                                                                   :string
                                                                   (updated-ames "TotalBath"))))
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
user> (def sorted-named-baths (-> (ds/select-columns named-baths ["TotalBath" "NamedBath" "SalePrice"])
                                  (ds/sort-by-column "SalePrice" >)))
#'user/sorted-named-baths
user> (ds/head sorted-named-baths)
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/ames-train.csv.gz [5 3]:

| TotalBath |       NamedBath | SalePrice |
|-----------|-----------------|-----------|
|       4.0 | living in style |    755000 |
|       4.5 | living in style |    745000 |
|       4.5 | living in style |    625000 |
|       3.5 | living in style |    611657 |
|       3.5 | living in style |    582933 |
user> (ds/tail sorted-named-baths)
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
`dtype-next` [datetime documentation](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html)
right now before moving on :-).


```clojure
user> (def stocks (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                                {:key-fn keyword}))
#'user/stocks
user> (ds/head stocks)
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

| :symbol |      :date | :price |
|---------|------------|--------|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
user> (meta (stocks :date))
{:name :date, :datatype :packed-local-date, :n-elems 560}
user> (require '[tech.v3.datatype.datetime :as dtype-dt])
nil
user> (ds/head (ds/update-column stocks :date dtype-dt/datetime->milliseconds))
https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

| :symbol |        :date | :price |
|---------|--------------|--------|
|    MSFT | 946684800000 |  39.81 |
|    MSFT | 949363200000 |  36.35 |
|    MSFT | 951868800000 |  43.22 |
|    MSFT | 954547200000 |  28.37 |
|    MSFT | 957139200000 |  25.45 |
user> (require '[tech.v3.datatype.functional :as dfn])
nil
user> (as-> (assoc stocks :years (dtype-dt/long-temporal-field :years (stocks :date))) stocks
        (ds/group-by stocks (juxt :symbol :years))
        (vals stocks)
        ;;stream is a sequence of datasets at this point.
        (map (fn [ds]
               {:symbol (first (ds :symbol))
                :years (first (ds :years))
                :avg-price (dfn/mean (ds :price))})
             stocks)
        (sort-by (juxt :symbol :years) stocks)
        (ds/->>dataset stocks)
        (ds/head stocks 10))
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
## Writing A Dataset Out

These forms are supported for writing out a dataset:

```clojure
(ds/write! test-ds "test.csv")
(ds/write! test-ds "test.tsv")
(ds/write! test-ds "test.tsv.gz")
(ds/write! test-ds "test.nippy")
(ds/write! test-ds out-stream)
```

We have good support for [nippy](nippy-serialization-rocks.md) in which case
datasets work just like any other datastructure.  This format allows some level of
compression but about 10X-100X the loading performance of gzipped csv/tsv.  In addition,
you can write out heterogeneous datastructures that contain datasets and other things
such as the result of a group-by:

```clojure
user> (require '[taoensso.nippy :as nippy])
nil
user> (def byte-data (nippy/freeze (ds/group-by stocks :symbol)))
#'user/byte-data
user> (type byte-data)
[B
user> (keys (nippy/thaw byte-data))
("MSFT" "GOOG" "AAPL" "IBM" "AMZN")
user> (first (nippy/thaw byte-data))
["MSFT"
 https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [123 3]:

| :symbol |      :date | :price |
|---------|------------|--------|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
```

Also see the [tech.v3.libs.arrow](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html) and
[tech.v3.libs.parquet](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html) namespaces.


If you made it this far, check out the [quick reference](https://techascent.github.io/tech.ml.dataset/quick-reference.html).

## Mini Walkthrough

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
;; We support many file formats
user> (def csv-data (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"))
#'user/csv-data
user> (ds/head csv-data)
test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------|------------|-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |

;; tech.v3.libs.poi registers xls, tech.v3.libs.fastexcel registers xlsx.  If you want
;; to use poi for everything use workbook->datasets in the tech.v3.libs.poi namespace.
user> (require '[tech.v3.libs.poi])
nil
user> (def xls-data (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"))
#'user/xls-data
user> (ds/head xls-data)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/file_example_XLS_1000.xls [5 8]:

| column-0 | First Name | Last Name | Gender |       Country |  Age |       Date |     Id |
|----------|------------|-----------|--------|---------------|------|------------|--------|
|      1.0 |      Dulce |     Abril | Female | United States | 32.0 | 15/10/2017 | 1562.0 |
|      2.0 |       Mara | Hashimoto | Female | Great Britain | 25.0 | 16/08/2016 | 1582.0 |
|      3.0 |     Philip |      Gent |   Male |        France | 36.0 | 21/05/2015 | 2587.0 |
|      4.0 |   Kathleen |    Hanner | Female | United States | 25.0 | 15/10/2017 | 3549.0 |
|      5.0 |    Nereida |   Magwood | Female | United States | 58.0 | 16/08/2016 | 2468.0 |

;;And you have fine grained control over parsing

user> (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"
                             {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]}}))
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/file_example_XLS_1000.xls [5 8]:

| column-0 | First Name | Last Name | Gender |       Country |  Age |       Date |     Id |
|----------|------------|-----------|--------|---------------|------|------------|--------|
|      1.0 |      Dulce |     Abril | Female | United States | 32.0 | 2017-10-15 | 1562.0 |
|      2.0 |       Mara | Hashimoto | Female | Great Britain | 25.0 | 2016-08-16 | 1582.0 |
|      3.0 |     Philip |      Gent |   Male |        France | 36.0 | 2015-05-21 | 2587.0 |
|      4.0 |   Kathleen |    Hanner | Female | United States | 25.0 | 2017-10-15 | 3549.0 |
|      5.0 |    Nereida |   Magwood | Female | United States | 58.0 | 2016-08-16 | 2468.0 |
user>


;;Loading from the web is no problem
user>
user> (def airports (ds/->dataset "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
                                  {:header-row? false :file-type :csv}))
#'user/airports
user> (ds/head airports)
https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat [5 14]:

| column-0 |                                    column-1 |     column-2 |         column-3 | column-4 | column-5 |    column-6 |     column-7 | column-8 | column-9 | column-10 |            column-11 | column-12 |   column-13 |
|----------|---------------------------------------------|--------------|------------------|----------|----------|-------------|--------------|----------|----------|-----------|----------------------|-----------|-------------|
|        1 |                              Goroka Airport |       Goroka | Papua New Guinea |      GKA |     AYGA | -6.08168983 | 145.39199829 |     5282 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        2 |                              Madang Airport |       Madang | Papua New Guinea |      MAG |     AYMD | -5.20707989 | 145.78900147 |       20 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        3 |                Mount Hagen Kagamuga Airport |  Mount Hagen | Papua New Guinea |      HGU |     AYMH | -5.82678986 | 144.29600525 |     5388 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        4 |                              Nadzab Airport |       Nadzab | Papua New Guinea |      LAE |     AYNZ | -6.56980300 | 146.72597700 |      239 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        5 | Port Moresby Jacksons International Airport | Port Moresby | Papua New Guinea |      POM |     AYPY | -9.44338036 | 147.22000122 |      146 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |

;;At any point you can get a sequence of maps back.  We implement a special version
;;of Clojure's APersistentMap that is much more efficient than even records and shares
;;the backing store with the dataset.

user> (take 2 (ds/mapseq-reader csv-data))
({"date" #object[java.time.LocalDate 0x4a998af0 "2000-01-01"],
  "symbol" "MSFT",
  "price" 39.81}
 {"date" #object[java.time.LocalDate 0x6d8c0bcd "2000-02-01"],
  "symbol" "MSFT",
  "price" 36.35})

;;Datasets are comprised of named columns, and provide a Clojure hashmap-compatible
;;collection.  Datasets allow reading and updating column data associated with a column name,
;;and provide a sequential view of [column-name column] entries.

;;You can look up columns via `get`, keyword lookup, and invoking the dataset as a function on
;;a key (a column name). `keys` and `vals` retrieve respective sequences of column names and columns.
;;The functions `assoc` and `dissoc` work to define new associations to conveniently
;;add, update, or remove columns, with add/update semantics defined by`tech.v3.dataset/add-or-update-column`.

;;Column data is stored in primitive arrays (even most datetimes!) and strings are stored
;;in string tables.  You can load really large datasets with this thing!

;;Columns themselves are sequences of their entries.
user> (csv-data "symbol")
#tech.v3.dataset.column<string>[560]
symbol
[MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, ...]
user> (xls-data "Gender")
#tech.v3.dataset.column<string>[1000]
Gender
[Female, Female, Male, Female, Female, Male, Female, Female, Female, Female, Female, Male, Female, Male, Female, Female, Female, Female, Female, Female, ...]
user> (take 5 (xls-data "Gender"))
("Female" "Female" "Male" "Female" "Female")


;;Datasets and columns implement the clojure metadata interfaces (`meta`, `with-meta`, `vary-meta`)

;;You can access a sequence of columns of a dataset with `ds/columns`, or `vals` like a map,
;;and access the metadata with `meta`:

user> (->> csv-data
           vals  ;synonymous with ds/columns
           (map (fn [column]
                  (meta column))))
({:categorical? true, :name "symbol", :size 560, :datatype :string}
 {:name "date", :size 560, :datatype :packed-local-date}
 {:name "price", :size 560, :datatype :float32})

;;We can similarly destructure datasets like normal clojure
;;maps:

user> (for [[k column] csv-data]
        [k (meta column)])
(["symbol" {:categorical? true, :name "symbol", :size 560, :datatype :string}]
 ["date" {:name "date", :size 560, :datatype :packed-local-date}]
 ["price" {:name "price", :size 560, :datatype :float64}])

user> (let [{:strs [symbol date]} csv-data]
        [symbol (meta date)])
[#tech.v3.dataset.column<string>[560]
symbol
[MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, ...]
 {:name "date", :size 560, :datatype :packed-local-date}]

;;We can get a brief description of the dataset:

user> (ds/brief csv-data)
({:min #object[java.time.LocalDate 0x5b2ea1d5 "2000-01-01"],
  :n-missing 0,
  :col-name "date",
  :mean #object[java.time.LocalDate 0x729b7395 "2005-05-12"],
  :datatype :packed-local-date,
  :quartile-3 #object[java.time.LocalDate 0x6c75fa43 "2007-11-23"],
  :n-valid 560,
  :quartile-1 #object[java.time.LocalDate 0x13d9aabe "2002-11-08"],
  :max #object[java.time.LocalDate 0x493bf7ef "2010-03-01"]}
 {:min 5.97,
  :n-missing 0,
  :col-name "price",
  :mean 100.7342857142857,
  :datatype :float64,
  :skew 2.4130946430619233,
  :standard-deviation 132.55477114107083,
  :quartile-3 100.88,
  :n-valid 560,
  :quartile-1 24.169999999999998,
  :max 707.0}
 {:mode "MSFT",
  :values ["MSFT" "AMZN" "IBM" "AAPL" "GOOG"],
  :n-values 5,
  :n-valid 560,
  :col-name "symbol",
  :n-missing 0,
  :datatype :string,
  :histogram (["MSFT" 123] ["AMZN" 123] ["IBM" 123] ["AAPL" 123] ["GOOG" 68])})

;;Another view of that brief:

user> (ds/descriptive-stats csv-data)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |      :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|------------|
|      date | :packed-local-date |      560 |          0 | 2000-01-01 | 2005-05-12 |       | 2010-03-01 |                     |            |
|     price |           :float64 |      560 |          0 |      5.970 |      100.7 |       |      707.0 |        132.55477114 | 2.41309464 |
|    symbol |            :string |      560 |          0 |            |            |  MSFT |            |                     |            |


;;There are analogues of the clojure.core functions that apply to dataset:
;;filter, group-by, sort-by.  These are all implemented efficiently.

;;You can add/remove/update columns, or use the map idioms of `assoc` and `dissoc`

user> (-> csv-data
          (assoc "always-ten" 10) ;scalar values are expanded as needed
          (assoc "random"   (repeatedly (ds/row-count csv-data) #(rand-int 100)))
          ds/head)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv [5 5]:

| symbol |       date | price | always-ten | random |
|--------|------------|-------|------------|--------|
|   MSFT | 2000-01-01 | 39.81 |         10 |     47 |
|   MSFT | 2000-02-01 | 36.35 |         10 |     35 |
|   MSFT | 2000-03-01 | 43.22 |         10 |     54 |
|   MSFT | 2000-04-01 | 28.37 |         10 |      6 |
|   MSFT | 2000-05-01 | 25.45 |         10 |     52 |

user> (-> csv-data
          (dissoc "price")
          ds/head)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv [5 2]:

| symbol |       date |
|--------|------------|
|   MSFT | 2000-01-01 |
|   MSFT | 2000-02-01 |
|   MSFT | 2000-03-01 |
|   MSFT | 2000-04-01 |
|   MSFT | 2000-05-01 |


;;since `conj` works as with clojure maps and sequences of map-entries or pairs,
;;you can use idioms like `reduce conj` or `into` to construct new datasets on the
;;fly with familiar clojure idioms:

user> (let [new-cols [["always-ten" 10] ["new-price" (map inc (csv-data "price"))]]
            new-data (into (dissoc csv-data "price") new-cols)]
            (ds/head new-data))
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv [5 4]:

| symbol |       date | always-ten | new-price |
|--------|------------|------------|-----------|
|   MSFT | 2000-01-01 |         10 |     40.81 |
|   MSFT | 2000-02-01 |         10 |     37.35 |
|   MSFT | 2000-03-01 |         10 |     44.22 |
|   MSFT | 2000-04-01 |         10 |     29.37 |
|   MSFT | 2000-05-01 |         10 |     26.45 |

;;You can write out the result back to csv, tsv, and gzipped variations of those.

;;Joins (left, right, inner) are all implemented.

;;Columnwise arithmetic manipulations (+,-, and many more) are provided via the
;;tech.v2.datatype.functional namespace.

;;Datetime columns can be operated on - plus,minus, get-years, get-days, and
;;many more - uniformly via the tech.v2.datatype.datetime.operations namespace.

;;There is much more.  Please checkout the walkthough and try it out!
```
