# tech.ml.dataset Quick Reference

This topic summarizes many of the most frequently used TMD functions, together with some quick notes about their use. Functions here are linked to further documentation, or their source. Note, unless a namespace is specified, each function is accessible via the `tech.ml.dataset` namespace.

For a more thorough treatment, the [API docs](https://techascent.github.io/tech.ml.dataset/index.html) list every available function.

### Table of Contents
1. [Loading/Saving](#LoadingSaving)
1. [Accessing Values](#AccessingValues)
1. [REPL Friendly Printing](#PrintOptions)
1. [Exploring Datasets](#ExploringDatasets)
1. [Selecting Subrects](#SelectingSubrects)
1. [Manipulating Datasets](#ManipulatingDatasets)
1. [Elementwise Arithmetic](#ElementwiseArithmetic)
1. [Forcing Lazy Evaluation](#ForcingLazyEvaluation)

-----
<div id="LoadingSaving"></div>

## Loading/Saving

* [->dataset, ->>dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var--.3Edataset) - obtains datasets from files or streams of csv/tsv, sequence-of-maps, map-of-arrays, xlsx, xls, and other typical formats. If their respective namespaces and dependencies are loaded, this function can also load parquet and arrow. [SQL](https://github.com/techascent/tech.ml.dataset.sql) and [ClojureScript](https://github.com/cnuernber/tmdjs) support is provided by separate libraries.
* [write!](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-write.21) - Writes csv, tsv, nippy (or a variety of other formats) with optional gzipping. Depends on scanning file path string to determine options.
* [parquet support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html)
* [xlsx, xls support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.poi.html)
* [fast xlsx support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.fastexcel.html)
* [arrow support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html)
* [dataset->data](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-dataset-.3Edata) - Useful if you want an entire dataset represented as Clojure/JVM datastructures. Primitive arrays save space, roaring bitmaps represent missing sets, and string tables receive special treatment.
* [data->dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-data-.3Edataset) - Inverse of data->dataset.
* [tech.ml.dataset.io.univocity/csv->rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.io.univocity.html#var-csv-.3Erows) - lower-level support for lazily parsing a csv or tsv as a sequence of `string[]` rows. Offers a subset of the `->dataset` options.
* [tech.ml.dataset.parse/rows->dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.io.string-row-parser.html#var-rows-.3Edataset) - lower-level support for obtaining a dataset from a sequence of `string[]` rows. Offers subset of the `->dataset` options.

-----
<div id="AccessingValues"></div>

## Accessing Values

* Datasets are logically maps of column name to column, and to this end implement `IPersistentMap`. So, `(map meta (vals ds))`  will return a sequence of column metadata. Moreover, datasets implement `Ifn`, and so are functions of their column names. Thus, `(ds :colname)` will return the column named `:colname`. Functions like `keys`, `vals`, `contains?`, `assoc`, `dissoc`, `merge`, and map-style destructuring all work on datasets. Notably, `update` does not work as update always returns a persistent map.
* Columns are iterable and implement indexed (random access) so they work with `map`, `count` and `nth`. Columns also implement `IFn` analgous to to persistent vectors. Helpfully, using negative values as indexes reads from the end similar to numpy and pandas.
* [row-count](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-count) - count dataset and column rows.
* [rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rows) - get the rows of the dataset as a `java.util.List` of persistent-map-like maps. Accomplished by a flyweight implementation of `clojure.lang.APersistentMap` where data is read out of the underlying dataset on demand. This keeps the data in the backing store for lazily access; this makes reading marginally more expensive, but allows this call not to increase memory working-set size. Indexing rows returned like this with negative values indexes from the end similar to numpy and pandas.
* [rowvecs](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rowvecs) - get the rows of the dataset as a `java.util.List` of persistent-vector-like entries. These rows are safe to use in maps. When using row values as keys in maps, the `{:copying? true}` option can help with performance, because each hash and equals comparison is using data located in the vector, not re-reading the data out of the source dataset. Negative values index from the end similar to numpy and pandas.
* `rows` and `rowvecs` are lazy and thus `(rand-nth (ds/rows ds))` is a relatively efficient pathway (and fun).  `(ds/rows (ds/sample ds))` is also good for a quick scan.
* [column-count](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-column-count) - count columns.
* Typed random access is supported the `(tech.v3.datatype/->reader col)` transformation.  This is guaranteed to return an implementation of `java.util.List` storing typed values. These implement `IFn` like a column or persistent vector. Direct access to packed datetime columns may produce surprising results; call `tech.v3.datatype.datetime/unpack` on the column prior to calling `tech.v3.datatype/->reader` to get to the unpacked datatype. Negative indexes on readers index from the end.
* [missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-missing) - return a RoaringBitmap of missing indexes. For columns, returns the column's specific missing indexes. For datasets, returns a union of all the columns' missing indexes.
* [meta, with-meta, vary-meta](https://github.com/clojure/clojure/blob/master/src/clj/clojure/core.clj#L202) - both datasets and columns implement `clojure.lang.IObj` so metadata works. The key `:name` has meaning in the system and setting it directly on a column is not recommended.  In general, operations preserve metadata.

-----
<div id="PrintOptions"></div>

## REPL Friendly Printing

REPL workflows are an important part of TMD, and so controlling what is printed (especially for larger datasets) is critical. Many options are provided, by metadata, to get the right information on the screen for perusal.

Be default, printing is abbreviated, the helpful [print-all](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-print-all) function overrides this behavior to enable printing all rows.

In general, any option can be set like `(vary-meta ds assoc :print-column-max-width 10)`.

* Summary of [print metadata options](https://github.com/techascent/tech.ml.dataset/blob/7c8c7514e0e35995050c1e326122a1826cc18273/src/tech/v3/dataset/print.clj#L93)

-----
<div id="ExploringDatasets"></div>

## Exploring Datasets

* [head](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-head) - obtains a dataset consisting of the first N rows of the input dataset.
* [tail](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-tail) - obtains a dataset consisting of the last N rows of the input dataset.
* [sample](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-sample) - samples N rows, randomly, as a dataset.
* [rand-nth](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rand-nth) - samples a single row of the dataset.
* [descriptive-stats](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-descriptive-stats) - produces a dataset of columnwise descriptive statistics.
* [brief](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-brief) - get descriptive statistics as Clojure data (an edn sequence of maps, one for each column).

-----
<div id="SelectingSubrects"></div>

## Selecting Subrects

Recall that since datasets are maps, `assoc`, `dissoc`, and `merge` all work at the dataset level - beyond that, consider these helpful subrect selection functions.

* [select-columns](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-select-columns) - select a subset of columns. Notably, this also controls column _order_ for downstream printing and serialization.
* [select-rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-select-rows) - get a specific subset of rows from a datasets or column.
* [select](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-select) - get a specific set of rows and columns in a single call, can be used for renaming.
* [drop-rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-drop-rows) - drop rows by index from a datasets or column.
* [drop-missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-drop-missing) - drop any rows with missing values from the dataset.

-----
<div id="ManipulatingDatasets"></div>

## Manipulating Datasets

* [new-dataset](https://github.com/techascent/tech.ml.dataset/blob/7c8c7514e0e35995050c1e326122a1826cc18273/src/tech/v3/dataset/impl/dataset.clj#L380) - Create a new dataset from a sequence of columns. Columns may be actual columns created via `tech.ml.dataset.column/new-column` or they could be maps containing at least keys `#:tech.v3.dataset{:name :data}` but also potentially `#:tech.v3.dataset{:metadata :missing}` in order to create a column with a specific set of missing values and metadata. `:force-datatype true` will disable the system from attempting to scan the data for missing values and e.g. create a float column from a vector of Float objects. The above also applies to using `clojure.core/assoc` with a dataset.
* ⭐ [row-map](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-map) ⭐ - maps a function from map->map in parallel over the dataset. The returned maps will be used to create or update columns in the output dataset, merging with the original. Note there are options to return a sequence of datasets as opposed to a single large final dataset.
* [row-mapcat](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-mapcat) - maps a function from map->sequence-of-maps over the dataset in parallel, potentially expanding or shrinking the result (in terms of row count). When expanding, row information not included in the original map is efficiently duplicated. Note there are options to return a sequence of datasets as opposed to a single potentially very large final dataset.
* [pd-merge](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.join.html#var-pd-merge) - implements generalized left, right, inner, outer, and cross joins. Allows combining datasets in a way familiar to users of traditional databases.
* [replace-missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-replace-missing), [replace-missing-value](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-replace-missing-value) - replace missing values in one or more columns.
* [group-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-group-by-column), [group-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-group-by) - creates a map of value to dataset with that value. These datasets are created via indexing into the original dataset for efficiency, so no data is copied.
* [sort-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-sort-by-column), [sort-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-sort-by) - sorts the dataset by column values.
* [filter-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-filter-column), [filter](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-filter) - produces a new dataset with only rows that pass a predicate.
* [concat-copying](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-concat-copying), [concat-inplace](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-concat-inplace) - produces a new dataset as a concatenation of supplied datasets. Copying can be more efficient than in-place, but uses more memory - `(apply ds/concat-copying x-seq)` is **far** more efficient than `(reduce ds/concat-copying x-seq)`; this also is true for `concat-inplace`.
* [unique-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-unique-by-column), [unique-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-unique-by) - removes duplicate rows.  Passing in `keep-fn` allows you to choose either first, last, or some other criteria for rows that have the same values. For `unique-by`, `identity` will work just fine (rows have sane equality semantics).
* [pmap-ds](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-pmap-ds) - maps a function of ds->ds in parallel over batches of data in the dataset. Can return either a new dataset via concat-copying or a sequence of datasets.
* [left-join-asof](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.join.html#var-left-join-asof) - specialized join-nearest functionality useful for doing things like finding the nearest values in time in irregularly sampled data.
* [rolling](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.rolling.html#var-rolling) - fixed and variable rolling window operations.
* [group-column-by-agg](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.html#var-group-by-column-agg) - unusually high performance primitive that logically combines `group-by` and `reduce` operations. Each key in the supplied map of reducers becomes a column in the output dataset.
* [neanderthal support](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.neanderthal.html) - transformations of datasets to/from neanderthal dense native matrixes.
* [tensor support](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.tensor.html) - transformations of datasets to/from [tech.v3.tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html) objects.

Many of the functions above come in `->column` variants, which can be faster by avoiding the creation of fully-realized output datasets with superfluous data. Moreover, some of these functions come in `->indexes` variants, which simply return indexes and thus skip creating sub-datasets. Operating in index space as such can be _very_ efficient.

-----
<div id="ElementwiseArithmetic"></div>

## Elementwise Arithmetic

Functions in the `tech.v3.datatype.functional` namespace operate elementwise on a column, lazily returning a new column. It is highly recommended to remove all missing values before using element-wise arithmetic as the `functional` namespace has no knowledge of missing values. Integer columns with missing values will be upcast to float or double columns in order to support a missing value indicator.

Note the use of `dfn` from `(require [tech.v3.datatype.functional :as dfn])`:

```clojure
(assoc ds :value (dtype/elemwise-cast (ds :value) :int64)
          :shrs-or-prn-amt (dtype/elemwise-cast (ds :shrs-or-prn-amt) :int64)
          :cik (dtype/const-reader (:cik filing) (ds/row-count ds))
          :investor (dtype/const-reader investor (ds/row-count ds))
          :form-type (dtype/const-reader form-type (ds/row-count ds))
          :edgar-id (dtype/const-reader (:edgar-id filing) (ds/row-count ds))
          :weight (dfn// (ds :value)
                         (double (dfn/reduce-+ (ds :value)))))
```


-----
<div id="ForcingLazyEvaluation"></div>

## Forcing Lazy Evaluation

In pandas or with R's `data.table`s one frequently needs to consider making a copy of some data before operating on it. Making too many copies uses too much memory, making too few copies leads to confusing non-local overwrites of data. There is untold lossage in these nonfunctional notions of dataset processing. Unlike these, TMD's datasets are functional.

TMD's functional datasets rely on index indirection, lazyness, and structural sharing to simplify the mental model necessary to reason about their operation. This allows low-cost aggregation of operations, and eliminates most wondering about whether making a copy is necessary or not (it's generally not). However, these indirections sometimes increase read costs.

At any time, `clone` can be used to make a clean copy of the dataset that relies on no indirect computation, and stores the data separately, so there is no chance of accidental overwrites. Clone is multithreaded and very efficient, boiling down to parallelized iteration over the data and `System/arraycopy` calls. Moreover, calling `clone` can reduce the in-memory size of the dataset by a bit - sometimes 20%, by converting `List`s that have some overhead into arrays that have no extra capacity.

 * [tech.v3.datatype/clone](https://github.com/cnuernber/dtype-next/blob/152f09f925041d41782e05009bbf84d7d6cfdbc6/src/tech/v3/datatype.clj#L95) - clones the dataset realizing lazy operations and copying the data into java arrays. Operates on datasets and columns.

---

## Additional Selling Points

Sophisticated support for [Apache Arrow](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html), including mmap support for JDK-8->JDK-17 although if you are on an M-1 Mac you will need to use JDK-17. Also, with arrow, per-column compression (LZ4, ZSTD) exists across all supported platforms. At the time of writing, the official Arrow SDK does not support mmap, or JDK-17, and has no user-accessible way to save a compressed streaming format file.

Support is provided for operating on _sequences_ of datasets, enabling working on larger, potentially out-of-memory workloads. This is consistent with the design of the parquet and arrow data storage systems and aggregation operations for sequences of datasets are efficiently implemented in the
[tech.v3.dataset.reductions](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.html) namespace.

Preliminary support for algorithms from the [Apache Data Sketches](https://datasketches.apache.org/) system can be found in the [apache-data-sketch](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.apache-data-sketch.html) namespace. Summations/means in this area are implemented using the
[Kahan compensated summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm) algorithm.

### Efficient Rowwise Operations

TMD uses efficient parallelized mechanisms to operate on data for rowwise map and mapcat operations. Argument functions are passed maps that lazily read only the required data from the underlying dataset (huge savings over reading all the data). TMD scans the returned maps from the argument function for datatype and missing information. Columns derived from the mapping operation overwrite columns in the original dataset - the powerful `row-map` function works this way.

The mapping operations are run in parallel using a primitive named `pmap-ds` and the resulting datasets can either be returned in a sequence or combined into a single larger dataset.

* [row-map](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-map)
* [row-mapcat](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-mapcat)
* [rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rows)
* [rowvecs](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rowvecs)

---

## 📚 Additional Documentation 📚

The best place to start is the "Getting Started" topic in the documentation: [https://techascent.github.io/tech.ml.dataset/000-getting-started.html](https://techascent.github.io/tech.ml.dataset/000-getting-started.html)

The "Walkthrough" topic provides long-form examples of processing real data: [https://techascent.github.io/tech.ml.dataset/100-walkthrough.html](https://techascent.github.io/tech.ml.dataset/100-walkthrough.html)

The API docs document every available function: [https://techascent.github.io/tech.ml.dataset/](https://techascent.github.io/tech.ml.dataset/)

The provided Java API ([javadoc](https://techascent.github.io/tech.ml.dataset/javadoc/tech/v3/TMD.html) / [with frames](https://techascent.github.io/tech.ml.dataset/javadoc/index.html)) and sample program ([source](java_test/java/jtest/TMDDemo.java)) show how to use TMD from Java.
