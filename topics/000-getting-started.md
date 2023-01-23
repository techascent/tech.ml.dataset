# tech.ml.dataset Getting Started

## What kind of data?

TMD processes _tabular_ data, that is, data logically arranged in rows and columns. Similar to a spreadsheet (but handling much larger datasets) or a database (but much more convenient), TMD accelerates exploring, cleaning, and processing data tables. TMD inherits Clojure's data-orientation and flexible dynamic typing, without compromising on being _functional_; thereby extending the language's reach to new problems and domains.

```clojure
> (ds/->dataset "lucy.csv")
lucy.csv [3 3]:

|  name | age | likes |
|-------|----:|-------|
|  fred |  42 | pizza |
| ethel |  42 | sushi |
| sally |  21 | opera |
```

## Reading and writing datasets

TMD can read datasets from many common formats (e.g., csv, tsv, xls, xlsx, json, parquet, arrow, ...). When given a file path, the [->dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var--.3Edataset) function can often detect the format automatically by the file extension and obtain the dataset. The same function can make datasets from other sources, such as sequences of Clojure maps in memory, or (again with broad format support) data downloaded from the internet.

For output, the [rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rows) function gives the dataset as a sequence of maps, and the [write!](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-write.21) function can be used to serialize any dataset into any supported format.


```clojure
> (ds/->dataset [{:name "fred"
                  :age 42
                  :likes "pizza"}
                 {:name "ethel"
                  :age 42
                  :likes "sushi"}
                 {:name "sally"
                  :age 21
                  :likes "opera"}])
_unnamed [3 3]:

| :name | :age | :likes |
|-------|-----:|--------|
|  fred |   42 |  pizza |
| ethel |   42 |  sushi |
| sally |   21 |  opera |
```

## Filtering data

TMD datasets are logically _maps_ of column name to column data; this means that (for example) Clojure's `dissoc` can be used to remove a column. Datasets can also be filtered row-wise, by predicates of a single column, or of entire rows - this is similar to Clojure's `filter` function, but can operate much more efficiently by exploiting tabular structure.

```clojure
> (-> (ds/->dataset "lucy.csv")
      (dissoc "likes"))
lucy.csv [3 2]:

|  name | age |
|-------|----:|
|  fred |  42 |
| ethel |  42 |
| sally |  21 |
```

```clojure
> (-> (ds/->dataset "lucy.csv")
      (ds/filter-column "age" #(> % 30)))
lucy.csv [2 3]:

|  name | age | likes |
|-------|----:|-------|
|  fred |  42 | pizza |
| ethel |  42 | sushi |
```

## Adding data

The powerful [row-map](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-map) function can be used to create or update columns that derive from data already in the dataset. Adding rows is typically accomplished by concatenating two (or more) datasets. The [functional](https://cnuernber.github.io/dtype-next/tech.v3.datatype.functional.html) namespace provides convenient functions for operating on scalar, element-wise, or columnar data.

```clojure
> (-> (ds/->dataset "lucy.csv")
      (ds/row-map (fn [{:strs [age]}]
                    {"half-age" (/ age 2.0)})))
lucy.csv [3 4]:

|  name | age | likes | half-age |
|-------|----:|-------|---------:|
|  fred |  42 | pizza |     21.0 |
| ethel |  42 | sushi |     21.0 |
| sally |  21 | opera |     10.5 |
```

## Statistics

TMD has tools for calculating summary statistics on datasets. The [descriptive-stats](https://techascent.github.io/tech.ml.dataset/docs/tech.v3.dataset.html#var-descriptive-stats) function produces a dataset of summary statistics for each column in the input dataset - perfect for initial exploration, or further meta analysis or operation. Broad support for further columnar statistical analysis is provided by the [statistics](https://cnuernber.github.io/dtype-next/tech.v3.datatype.statistics.html) namespace.

```clojure
> (-> (ds/->dataset "lucy.csv")
      (ds/row-map (fn [{:strs [age]}]
                    {"half-age" (/ age 2.0)}))
      (ds/descriptive-stats {:stat-names [:col-name :datatype :min :mean :max :standard-deviation]}))
lucy.csv: descriptive-stats [4 6]:

| :col-name | :datatype | :min | :mean | :max | :standard-deviation |
|-----------|-----------|-----:|------:|-----:|--------------------:|
|      name |   :string |      |       |      |                     |
|       age |    :int16 | 21.0 |  35.0 | 42.0 |         12.12435565 |
|     likes |   :string |      |       |      |                     |
|  half-age |  :float64 | 10.5 |  17.5 | 21.0 |          6.06217783 |
```

## Grouping

Like a Clojure sequence, a dataset can be grouped into a _map_ of value to dataset with that value. The [group-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-group-by) function accomplishes this. The related [group-by->indexes](https://techascent.github.io/tech.ml.dataset/docs/tech.v3.dataset.html#var-group-by-.3Eindexes) function produces maps of value to row-indexes of the input dataset - working with indexes can be more efficient than constructing concrete grouped datasets.

```clojure
> (-> (ds/->dataset "lucy.csv")
      (ds/group-by #(if (> (get % "age") 30) :old :not-old)))
{:old lucy.csv [2 3]:

|  name | age | likes |
|-------|----:|-------|
|  fred |  42 | pizza |
| ethel |  42 | sushi |
, :not-old lucy.csv [1 3]:

|  name | age | likes |
|-------|----:|-------|
| sally |  21 | opera |
}
```

```clojure
> (-> (ds/->dataset "lucy.csv")
      (ds/group-by->indexes #(if (> (get % "age") 30) :old :not-old)))
{:old [0 1], :not-old [2]}
```

## Combining datasets

Because datasets are _maps_ of column name to column data, they can be combined column-wise using Clojure's `merge` function. The [concat](https://techascent.github.io/tech.ml.dataset/docs/tech.v3.dataset.html#var-concat) function can be used for row-wise combination of two or more datasets. The [join](https://techascent.github.io/tech.ml.dataset/docs/tech.v3.dataset.join.html) namespace provides database-like joins for aligning data from multiple datasets.

```clojure
> (merge (ds/->dataset (for [i (range 3)] {"index" i}))
         (ds/->dataset "lucy.csv"))
_unnamed [3 4]:

| index |  name | age | likes |
|------:|-------|----:|-------|
|     0 |  fred |  42 | pizza |
|     1 | ethel |  42 | sushi |
|     2 | sally |  21 | opera |
```

## Date, time, and other datatypes

TMD knows about dates, times, instants, and many other types from the comprehensive `java.time` library. Working with these types can be much more convenient than dealing with them as strings, and datatypes are preserved throughout operations, so downstream tooling can avoid dealing with these data as strings as well.

In addition to `java.time` types, all Clojure types (e.g., keywords), UUIDs, as well as a comprehensive set of signed and unsigned numeric types of different widths are also transparently supported.

```clojure
> (def ds (ds/->dataset [{:date "1981-03-10"}
                         {:date "1999-12-31"}]
                        {:parser-fn {:date :local-date}}))
#'ds
> (.until (first (:date ds))
          (last (:date ds)))
#object[java.time.Period 0x2d9a2c24 "P18Y9M21D"]
```

---

## Further reading

 - The [README](https://github.com/techascent/tech.ml.dataset#techmldataset) on GitHub has information about installing, and first steps with TMD.

 - The [walkthrough](https://techascent.github.io/tech.ml.dataset/100-walkthrough.html) topic has long-form examples of processing real data with TMD.

 - The [quick reference](https://techascent.github.io/tech.ml.dataset/200-quick-reference.html) summarizes many of the most frequently used functions with hints about their use.

 - The [API docs](https://techascent.github.io/tech.ml.dataset/index.html) list every function available in TMD.
