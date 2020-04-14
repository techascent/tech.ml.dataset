# tech.ml.dataset Walkthrough


Let's take a moment to walkthrough the `tech.ml.dataset system`.  This system was built
over the course of a few months in order to make working with columnar data easier
in the same manner as one would work with DataFrames in R or Pandas in Python.  While
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
user> (ds/->dataset [{:a 1 :b 2} {:a 2 :c 3}])
_unnamed [2 3]:

| :a |     :b |     :c |
|----+--------+--------|
|  1 |      2 | -32768 |
|  2 | -32768 |      3 |
```

#### CSV/TSV/MAPSEQ/XLS/XLSX Parsing Options
It is important to note that there are several options for parsing files.
A few important ones are column whitelist/blacklists, num records,
and ways to specify exactly how to parse the string data:

```clojure


user> (doc ds/->dataset)
-------------------------
tech.ml.dataset/->dataset
([dataset {:keys [table-name], :as options}] [dataset])
  Create a dataset from either csv/tsv/xls/xlsx or a sequence of maps.
   *  A `String` or `InputStream` will be interpreted as a file (or gzipped file if it
   ends with .gz) of tsv or csv data.  The system will attempt to autodetect if this
   is csv or tsv and then engineering around detecting datatypes all of which can
   be overridden.
   *  A sequence of maps may be passed in in which case the first N maps are scanned in
   order to derive the column datatypes before the actual columns are created.
  Options:
  :table-name - set the name of the dataset.
  :column-whitelist - either sequence of string column names or sequence of column
       indices of columns to whitelist.
  :column-blacklist - either sequence of string column names or sequence of column
       indices of columns to blacklist.
  :num-rows - Number of rows to read
  :header-row? - Defaults to true, indicates the first row is a header.
  :separator - Add a character separator to the list of separators to auto-detect.
  :csv-parser - Implementation of univocity's AbstractParser to use.  If not provided
       a default permissive parser is used.  This way you parse anything that univocity
       supports (so flat files and such).
  :skip-bad-rows? - For really bad files, some rows will not have the right column
      counts for all rows.  This skips rows that fail this test.
  :parser-fn -
   - keyword - all columns parsed to this datatype
   - ifn? - called with two arguments: (parser-fn column-name-or-idx column-data)
          - Return value must be implement tech.ml.dataset.parser.PColumnParser in
            which case that is used or can return nil in which case the default
            column parser is used.
   - tuple - pair of [datatype parse-fn] in which case container of type [datatype] will be created.
           parse-fn can be one of:
        :relaxed? - data will be parsed such that parse failures of the standard parse functions do not stop
             the parsing process.  :unparsed-values and :unparsed-indexes are available in the metadata of the
             column that tell you the values that failed to parse and their respective indexes.
        fn? - function from str-> one of #{:missing :parse-failure value}.  Exceptions here always kill the parse
             process.
        string? - for datetime types, this will turned into a DateTimeFormatter via DateTimeFormatter/ofPattern.
        DateTimeFormatter - use with the appropriate temporal parse static function to parse the value.
   - map - the header-name-or-idx is used to lookup value.  If not nil, then
           value can be any of the above options.  Else the default column parser is used.
  :parser-scan-len - Length of initial column data used for parser-fn's datatype
       detection routine. Defaults to 100.

  Returns a new dataset
nil

user> (ds/->dataset "data/ames-house-prices/train.csv"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5})
data/ames-house-prices/train.csv [4 3]:

| SalePrice | 1stFlrSF | 2ndFlrSF |
|-----------+----------+----------|
|    208500 |      856 |      854 |
|    181500 |     1262 |        0 |
|    223500 |      920 |      866 |
|    140000 |      961 |      756 |
user> (ds/->dataset "data/ames-house-prices/train.csv"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5
                     :parser-fn :float32})
data/ames-house-prices/train.csv [4 3]:

|  SalePrice | 1stFlrSF | 2ndFlrSF |
|------------+----------+----------|
| 208500.000 |  856.000 |  854.000 |
| 181500.000 | 1262.000 |    0.000 |
| 223500.000 |  920.000 |  866.000 |
| 140000.000 |  961.000 |  756.000 |
user> (ds/->dataset "data/ames-house-prices/train.csv"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5
                     :parser-fn {"SalePrice" :float32}})
data/ames-house-prices/train.csv [4 3]:

|  SalePrice | 1stFlrSF | 2ndFlrSF |
|------------+----------+----------|
| 208500.000 |      856 |      854 |
| 181500.000 |     1262 |        0 |
| 223500.000 |      920 |      866 |
| 140000.000 |      961 |      756 |
```

You can also supply a tuple of `[datatype parse-fn]` if you have a specific
datatype and parse function you want to use.  For datetime types `parse-fn`
can additionally be a DateTimeFormat format string or a DateTimeFormat object:

```clojure
nil
user> (def data (ds/select (ds/->dataset "test/data/file_example_XLSX_1000.xlsx")
                           :all (range 5)))

#'user/data
user> data
Sheet1 [5 8]:

|     0 | First Name | Last Name | Gender |       Country |    Age |       Date |       Id |
|-------+------------+-----------+--------+---------------+--------+------------+----------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.000 | 15/10/2017 | 1562.000 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.000 | 16/08/2016 | 1582.000 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.000 | 21/05/2015 | 2587.000 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.000 | 15/10/2017 | 3549.000 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.000 | 16/08/2016 | 2468.000 |
user> ;; Note the Date actually didn't parse out because it is dd/MM/yyyy format:
user> (dtype/get-datatype (data "Date"))
:string
user> (def data (ds/select (ds/->dataset "test/data/file_example_XLSX_1000.xlsx"
                                         {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]}})
                           :all (range 5)))

#'user/data
user> data
Sheet1 [5 8]:

|     0 | First Name | Last Name | Gender |       Country |    Age |       Date |       Id |
|-------+------------+-----------+--------+---------------+--------+------------+----------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.000 | 2017-10-15 | 1562.000 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.000 | 2016-08-16 | 1582.000 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.000 | 2015-05-21 | 2587.000 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.000 | 2017-10-15 | 3549.000 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.000 | 2016-08-16 | 2468.000 |
user> (dtype/get-datatype (data "Date"))
:local-date

user> (def data (ds/select (ds/->dataset "test/data/file_example_XLSX_1000.xlsx"
                                         {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]
                                                      "Id" :int32
                                                      0 :int32
                                                      "Age" :int16}})
                           :all (range 5)))
#'user/data
user> data
Sheet1 [5 8]:

| 0 | First Name | Last Name | Gender |       Country | Age |       Date |   Id |
|---+------------+-----------+--------+---------------+-----+------------+------|
| 1 |      Dulce |     Abril | Female | United States |  32 | 2017-10-15 | 1562 |
| 2 |       Mara | Hashimoto | Female | Great Britain |  25 | 2016-08-16 | 1582 |
| 3 |     Philip |      Gent |   Male |        France |  36 | 2015-05-21 | 2587 |
| 4 |   Kathleen |    Hanner | Female | United States |  25 | 2017-10-15 | 3549 |
| 5 |    Nereida |   Magwood | Female | United States |  58 | 2016-08-16 | 2468 |
```

A reference to what is possible is in
[parse-test](../test/tech/ml/dataset/parse_test.clj).


#### name-value-seq->dataset

Given a map of name->column data produce a new dataset.  If column data is untyped
(like a persistent vector) then the column datatype is either string or double,
dependent upon the first entry of the column data sequence.

Of the column data is one of the object numeric primitive types, so
`Float` as opposed to `float`, then missing elements will be marked as
missing and the default empty-value will be used in the primitive storage.

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
#tech.ml.dataset.column<int16>[2]
:a
[1, 2, ]
user> (new-ds :c)
#tech.ml.dataset.column<int16>[2]
:c
[-32768, 3, ]
[]
user> (ds-col/missing (new-ds :b))
[1]
user> (ds-col/missing (new-ds :c))
[0]
user> (first new-ds)
#tech.ml.dataset.column<int16>[2]
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
user> (-> (ds/sort-by #(get % "SalePrice") ames-ds)
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

#### Descriptive Stats And GroupBy And DateTime Types

This is best illustrated by an example:

```clojure
user> (def stocks (ds/->dataset "test/data/stocks.csv"))
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
#tech.ml.dataset.column<float64>[1460]
TotalBath
[3.500, 2.500, 3.500, 2.000, 3.500, 2.500, 3.000, 3.500, 2.000, 2.000, 2.000, 4.000, 2.000, 2.000, 2.500, 1.000, 2.000, 2.000, 2.500, 1.000, ...]
```

We can also implement a completely dynamic operation to create a new column by
implementing the appropriate reader interface from the datatype library:
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
		     ;;Since this is an object reader, we have to specify
			 ;;string as the datatype.
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
#tech.ml.dataset.column<string>[1460]
NamedBaths
[living in style, getting somewhere, living in style, getting somewhere, living in style, getting somewhere, living in style, living in style, getting somewhere, getting somewhere, getting somewhere, living in style, getting somewhere, getting somewhere, getting somewhere, somewhat doable, getting somewhere, getting somewhere, getting somewhere, somewhat doable, ...]

;; Here we see that the higher level houses all have more bathrooms

user> (def sorted-named-baths (ds/ds-sort-by-column "SalePrice" >  named-baths))
#'user/sorted-named-baths
user> (sorted-named-baths "NamedBaths")
#tech.ml.dataset.column<string>[1460]
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

## DateTime Types

Brand new.  Experimental.  all that stuff.

Support for reading datetime types and manipulating them.  Please checkout the
`tech.datatype` [datetime documentation](https://github.com/techascent/tech.datatype/blob/master/docs/datetime.md) for using this feature.


```clojure
user> (def stock-ds (ds/->dataset "test/data/stocks.csv"))
#'user/stock-ds
user> stock-ds
test/data/stocks.csv [560 3]:

| symbol |       date |  price |
|--------+------------+--------|
|   MSFT | 2000-01-01 | 39.810 |
|   MSFT | 2000-02-01 | 36.350 |
|   MSFT | 2000-03-01 | 43.220 |
|   MSFT | 2000-04-01 | 28.370 |
|   MSFT | 2000-05-01 | 25.450 |
|   MSFT | 2000-06-01 | 32.540 |
|   MSFT | 2000-07-01 | 28.400 |
|   MSFT | 2000-08-01 | 28.400 |
|   MSFT | 2000-09-01 | 24.530 |
|   MSFT | 2000-10-01 | 28.020 |
|   MSFT | 2000-11-01 | 23.340 |
|   MSFT | 2000-12-01 | 17.650 |
|   MSFT | 2001-01-01 | 24.840 |
|   MSFT | 2001-02-01 | 24.000 |
|   MSFT | 2001-03-01 | 22.250 |
|   MSFT | 2001-04-01 | 27.560 |
|   MSFT | 2001-05-01 | 28.140 |
|   MSFT | 2001-06-01 | 29.700 |
|   MSFT | 2001-07-01 | 26.930 |
|   MSFT | 2001-08-01 | 23.210 |
|   MSFT | 2001-09-01 | 20.820 |
|   MSFT | 2001-10-01 | 23.650 |
|   MSFT | 2001-11-01 | 26.120 |
|   MSFT | 2001-12-01 | 26.950 |
|   MSFT | 2002-01-01 | 25.920 |
user> (dtype/get-datatype (stock-ds "date"))
:packed-local-date

user> (require '[tech.v2.datatype.datetime.operations :as dtype-dt-ops])
nil
user> (ds/update-column stock-ds "date" dtype-dt-ops/get-epoch-milliseconds)
test/data/stocks.csv [560 3]:

| symbol |                 date |  price |
|--------+----------------------+--------|
|   MSFT | 2000-01-01T06:00:00Z | 39.810 |
|   MSFT | 2000-02-01T06:00:00Z | 36.350 |
|   MSFT | 2000-03-01T06:00:00Z | 43.220 |
|   MSFT | 2000-04-01T06:00:00Z | 28.370 |
|   MSFT | 2000-05-01T06:00:00Z | 25.450 |
|   MSFT | 2000-06-01T06:00:00Z | 32.540 |
|   MSFT | 2000-07-01T06:00:00Z | 28.400 |
|   MSFT | 2000-08-01T06:00:00Z | 28.400 |
|   MSFT | 2000-09-01T06:00:00Z | 24.530 |
|   MSFT | 2000-10-01T06:00:00Z | 28.020 |
|   MSFT | 2000-11-01T06:00:00Z | 23.340 |
|   MSFT | 2000-12-01T06:00:00Z | 17.650 |
|   MSFT | 2001-01-01T06:00:00Z | 24.840 |
|   MSFT | 2001-02-01T06:00:00Z | 24.000 |
|   MSFT | 2001-03-01T06:00:00Z | 22.250 |
|   MSFT | 2001-04-01T06:00:00Z | 27.560 |
|   MSFT | 2001-05-01T06:00:00Z | 28.140 |
|   MSFT | 2001-06-01T06:00:00Z | 29.700 |
|   MSFT | 2001-07-01T06:00:00Z | 26.930 |
|   MSFT | 2001-08-01T06:00:00Z | 23.210 |
|   MSFT | 2001-09-01T06:00:00Z | 20.820 |
|   MSFT | 2001-10-01T06:00:00Z | 23.650 |
|   MSFT | 2001-11-01T06:00:00Z | 26.120 |
|   MSFT | 2001-12-01T06:00:00Z | 26.950 |
|   MSFT | 2002-01-01T06:00:00Z | 25.920 |
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
        (ds/->dataset "data/ames-house-prices/train.csv"
                    {:column-whitelist ["SalePrice" "1stFlrSF" "2ndFlrSF"]
                     :n-records 5}))
#'user/test-ds
user> test-ds
data/ames-house-prices/train.csv [4 3]:

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
(ds-join/right-join ["1stFlrSF" "2ndFlrSF"]
                         (ds/set-dataset-name test-ds "left")
                         (ds/set-dataset-name test-ds "right"))
right-outer-join [4 5]:

| 2ndFlrSF | SalePrice | 1stFlrSF | left.SalePrice | left.2ndFlrSF |
|----------+-----------+----------+----------------+---------------|
|      854 |    208500 |      856 |    -2147483648 |        -32768 |
|        0 |    181500 |     1262 |    -2147483648 |        -32768 |
|      866 |    223500 |      920 |    -2147483648 |        -32768 |
|      756 |    140000 |      961 |    -2147483648 |        -32768 |

user> (ds-join/left-join ["1stFlrSF" "2ndFlrSF"]
(ds/set-dataset-name test-ds "left")
(ds/set-dataset-name test-ds "right"))
left-outer-join [4 6]:

| 1stFlrSF | SalePrice | 2ndFlrSF | right.2ndFlrSF | right.SalePrice | right.1stFlrSF |
|----------+-----------+----------+----------------+-----------------+----------------|
|      961 |    140000 |      756 |         -32768 |     -2147483648 |         -32768 |
|      920 |    223500 |      866 |         -32768 |     -2147483648 |         -32768 |
|      856 |    208500 |      854 |         -32768 |     -2147483648 |         -32768 |
|     1262 |    181500 |        0 |         -32768 |     -2147483648 |         -32768 |
```


## XLS, XLSX files

We use apache poi [directly](../src/tech/libs/poi/parse.clj) to generate datasets
from xls and xlsx files.  This feature is, like joins, very new.

```clojure
user> (first (poi-parse/workbook->datasets "test/data/file_example_XLS_1000.xls"))
Sheet1 [1000 8]:

|      0 | First Name | Last Name | Gender |       Country |    Age |       Date |       Id |
|--------+------------+-----------+--------+---------------+--------+------------+----------|
|  1.000 |      Dulce |     Abril | Female | United States | 32.000 | 15/10/2017 | 1562.000 |
|  2.000 |       Mara | Hashimoto | Female | Great Britain | 25.000 | 16/08/2016 | 1582.000 |
|  3.000 |     Philip |      Gent |   Male |        France | 36.000 | 21/05/2015 | 2587.000 |
|  4.000 |   Kathleen |    Hanner | Female | United States | 25.000 | 15/10/2017 | 3549.000 |
|  5.000 |    Nereida |   Magwood | Female | United States | 58.000 | 16/08/2016 | 2468.000 |
|  6.000 |     Gaston |     Brumm |   Male | United States | 24.000 | 21/05/2015 | 2554.000 |
|  7.000 |       Etta |      Hurn | Female | Great Britain | 56.000 | 15/10/2017 | 3598.000 |
|  8.000 |    Earlean |    Melgar | Female | United States | 27.000 | 16/08/2016 | 2456.000 |
|  9.000 |   Vincenza |   Weiland | Female | United States | 40.000 | 21/05/2015 | 6548.000 |
| 10.000 |     Fallon |   Winward | Female | Great Britain | 28.000 | 16/08/2016 | 5486.000 |
| 11.000 |    Arcelia |    Bouska | Female | Great Britain | 39.000 | 21/05/2015 | 1258.000 |
| 12.000 |   Franklyn |    Unknow |   Male |        France | 38.000 | 15/10/2017 | 2579.000 |
| 13.000 |    Sherron |  Ascencio | Female | Great Britain | 32.000 | 16/08/2016 | 3256.000 |
| 14.000 |     Marcel | Zabriskie |   Male | Great Britain | 26.000 | 21/05/2015 | 2587.000 |
| 15.000 |       Kina |  Hazelton | Female | Great Britain | 31.000 | 16/08/2016 | 3259.000 |
| 16.000 |   Shavonne |       Pia | Female |        France | 24.000 | 21/05/2015 | 1546.000 |
| 17.000 |     Shavon |    Benito | Female |        France | 39.000 | 15/10/2017 | 3579.000 |
| 18.000 |   Lauralee |   Perrine | Female | Great Britain | 28.000 | 16/08/2016 | 6597.000 |
| 19.000 |     Loreta |    Curren | Female |        France | 26.000 | 21/05/2015 | 9654.000 |
| 20.000 |     Teresa |    Strawn | Female |        France | 46.000 | 21/05/2015 | 3569.000 |
| 21.000 |    Belinda |   Partain | Female | United States | 37.000 | 15/10/2017 | 2564.000 |
| 22.000 |      Holly |      Eudy | Female | United States | 52.000 | 16/08/2016 | 8561.000 |
| 23.000 |       Many |    Cuccia | Female | Great Britain | 46.000 | 21/05/2015 | 5489.000 |
| 24.000 |     Libbie |     Dalby | Female |        France | 42.000 | 21/05/2015 | 5489.000 |
| 25.000 |     Lester |   Prothro |   Male |        France | 21.000 | 15/10/2017 | 6574.000 |
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
