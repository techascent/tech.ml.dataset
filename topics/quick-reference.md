# tech.ml.dataset Quick Reference

Functions are linked to their source but if no namespace is specified they are
also accessible via the `tech.ml.dataset` namespace.

This is not an exhaustive listing of all functionality; just a quick brief way to find
functions that are we find most useful.


## Loading/Saving

* [->dataset, ->>dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var--.3Edataset) - loads csv, tsv,
  sequence-of-maps, map-of-arrays, xlsx, xls, and if their respective namespaces and dependencies are loaded, parquet and arrow.
* [write!](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-write.21) - Writes csv, tsv or
  nippy with gzipping. Depends on scanning file path string to determine options.
* [dataset->data](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-dataset-.3Edata) - Useful if you want the entire
 dataset represented as (mostly) pure Clojure/JVM datastructures.  Missing sets are
 roaring bitmaps, data is probably in primitive arrays.  String tables receive special
 treatment.
* [data->dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-data-.3Edataset) - Inverse of data->dataset.
* [tech.ml.dataset.io.univocity/csv->rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.io.univocity.html#var-csv-.3Erows) - Lazily parse a
 csv or tsv returning a sequence of string[] rows.  This uses a subset of the ->dataset options.
* [tech.ml.dataset.parse/rows->dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.io.string-row-parser.html#var-rows-.3Edataset) - Given
 a sequence of string[] rows, parse data into a dataset.  Uses subset of the ->dataset
 options.
* [parquet support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html)
* [arrow support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html)
* [xlsx, xls support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.poi.html)
* [fast xlsx support](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.fastexcel.html)


## Accessing Values

* Datasets overload Ifn so are functions of their column names.  `(ds :colname)` will
  return the column named `:colname`.  Datasets implement `IPersistentMap` so
  `(map (comp second meta) ds)` or `(map meta (vals ds))`  will return a sequence of column
  metadata.  `keys`, `vals`, `contains?`, `assoc`, `dissoc`, `merge` and map-style destructuring
  all work on datasets.  Note that `update` does not work as update will always return a
  persistent map.
* Columns are iterable and implement indexed so you can use them with `map`, `count`
  and `nth`.  They furthermore overload IFn such that they are functions of their indexes similar
  to persistent vectors.  Using negative values as indexes will index from the end similar to numpy and pandas.
* Typed random access is supported the `(tech.v3.datatype/->reader col)`
  transformation.  This is guaranteed to return an implementation of `java.util.List`
  and also overloads `IFn` such that like a persistent vector passing in the index
  will return the value - e.g. `(col 0)` returns the value at index 0.  Direct access
  to packed datetime columns may be surprising; call `tech.v3.datatype.datetime/unpack`
  on the column prior to calling `tech.v3.datatype/->reader` to get to the unpacked
  datatype.  Using negative values as indexes will index from the end similar to numpy and pandas.
* [row-count](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-count) - works on datasets and columns.
* [column-count](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-column-count) - number of columns.
* [rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rows) - get the rows of the
 dataset as a `java.util.List` of persistent-map-like maps.  Implemented as a flyweight
 implementation of `clojure.lang.APersistentMap` where data is read out of the underlying dataset on demand.  This keeps the 
 data in the backing store and lazily reads it so you will have relatively more expensive reading of the
 data but will not increase your memory working-set size.   Using negative values as indexes will index from the end similar to numpy and pandas.
* [rowvecs](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rowvecs) - Get the rows of the
 dataset as a 'java.util.List' of rows.  These rows behave like persistent vectors and are safe to use
 in maps.  If you are going to use row values as keys in maps, passing in `{:copying? true}` will be more
 efficient as then each hash and equals comparison is using data in the vector and not re-reading the data
 out of the source dataset.   Using negative values as indexes will index from the end similar to numpy and pandas.
* [missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-missing) - return a RoaringBitmap of missing indexes.  If this is a column, it is the column's specific missing indexes.  If this is a dataset, return a union of all the columns' missing indexes.
* [meta, with-meta, vary-meta](https://github.com/clojure/clojure/blob/master/src/clj/clojure/core.clj#L202) - Datasets and columns implement
  `clojure.lang.IObj` so you can get/set metadata on them freely. `:name` has meaning in the system and setting it
  directly on a column is not recommended.  Metadata is generally carried forward through most of the operations below.


`rows` and `rowvecs` are lazy and thus `(rand-nth (ds/rows ds))` is
a relatively efficient pathway (and fun).  `(ds/rows (ds/sample ds))` is also
pretty good for quick scans.

## Print Options

We use these options frequently during exploration to get more/less printing
output.  These are used like `(vary-meta ds assoc :print-column-max-width 10)`.
Often it is useful to print the entire table:  `(vary-meta ds assoc :print-index-range :all)`

* [print metadata options](https://github.com/techascent/tech.ml.dataset/blob/7c8c7514e0e35995050c1e326122a1826cc18273/src/tech/v3/dataset/print.clj#L93)


## Dataset Exploration


* [head](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-head) - Return dataset consisting of first N rows.
* [tail](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-tail) - Return dataset consisting of last N rows.
* [sample](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-sample) - Randomly sample N rows of the dataset.
* [rand-nth](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-rand-nth) - Randomly sample a row of the dataset.
* [descriptive-stats](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-descriptive-stats) - return a dataset of
 columnwise descriptive statistics.
* [brief](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-brief) - Return a sequence of maps of  descriptive statistics.



## Subrect Selection

Keeping in mind that assoc, dissoc, and merge all work at the dataset level - here are some other
pathways that are useful for subrect selection.

* [select](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-select) - can be used for renaming.
 Anything iterable can be used for the rows.
* [select-columns](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-select-columns) - Select a subset of columns.
* [select-rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-select-rows) - works on datasets and columns.
* [drop-rows](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-drop-rows) - works on datasets and columns.
* [drop-missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-drop-missing) - Drop rows with missing values from the dataset.


## Dataset Manipulation

Several of the functions below come in `->column` variants and some come additional
in `->indexes` variants.  `->column` variants are going to be faster than the base
versions and `->indexes` simply return indexes and thus skip creating sub-datasets
so these are faster yet.

* [new-dataset](https://github.com/techascent/tech.ml.dataset/blob/7c8c7514e0e35995050c1e326122a1826cc18273/src/tech/v3/dataset/impl/dataset.clj#L380) - Create a new dataset from a sequence of columns.  Columns may be actual columns created via `tech.ml.dataset.column/new-column` or they could be maps containing at least keys `#:tech.v3.dataset{:name :data}` but also potentially `#:tech.v3.dataset{:metadata :missing}` in order to create a column with a specific set of missing values and metadata.  `:force-datatype true` will disable the system
from attempting to scan the data for missing values and e.g. create a float column
from a vector of Float objects.  The above also applies to using `clojure.core/assoc` with a dataset.
* [replace-missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-replace-missing), [replace-missing-value](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-replace-missing-value) - replace missing values in one or more columns.
* [group-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-group-by-column), [group-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-group-by) - Create a persistent map of value->dataset.  Sub-datasets are created via indexing into the original dataset so data is not copied.
* [sort-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-sort-by-column), [sort-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-sort-by) - Return a sorted dataset.
* [filter-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-filter-column), [filter](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-filter) - Return a new dataset with only rows that pass the predicate.
* [concat-copying](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-concat-copying), [concat-inplace](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-concat-inplace) - Given Y datasets produce a new dataset.  Copying is generally much more efficient than in-place for a dataset count > 2.   `(apply ds/concat-copying x-seq)` is
**far** more efficient than `(reduce ds/concat-copying x-seq)`; this also is true for `concat-inplace`.
* [unique-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-unique-by-column), [unique-by](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-unique-by) - Remove duplicate rows.  Passing in `keep-fn` allows
you to choose either first, last, or some other criteria for rows that have the same
values.  For `unique-by`, `identity` will work just fine.
* [row-map](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-map) - In parallel, map a function from map->map over the dataset.  The returned maps will be
  used to create new columns in a new dataset and the result merged with the original.  Note there are options to return a sequence of datasets as opposed to a single large
  final dataset.
* [row-mapcat](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-mapcat) - In parallel, map a function from map->sequence-of-maps over the dataset potentially
  expanding or shrinking the result.  When multiple maps are returned, row information not included in the original map is efficiently duplicated.  Note there are options to
  return a sequence of datasets as opposed to a single potentially very large final dataset.
* [pmap-ds](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-pmap-ds) - Split dataset into batches and in parallel map a function from ds->ds across the dataset.
  Can return either a new dataset via concat-copying or a sequence of datasets.
* [pd-merge](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.join.html#var-pd-merge) - Generalized left,right,inner,outer, and cross joins.
* [left-join-asof](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.join.html#var-left-join-asof) - Join-nearest type functionality useful for doing things like finding
  the 3, 6, and 12 month prices from a dataset daily prices where you what the nearest price as things don't trade on the weekends.
* [rolling](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.rolling.html#var-rolling) - fixed and variable rolling window operations.
* [group-column-by-agg](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.html#var-group-by-column-agg) - Very high performance primitive taking a sequence of
  datasets and producing a new dataset that is first grouped by one or more columns and then the per-group data is reduced using a map of reducers.  Each key in the map becomes
  a column in the result dataset.
* [neanderthal support](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.neanderthal.html) - transformations of datasets to/from neanderthal dense native matrixes.
* [tensor support](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.tensor.html) - transformations of datasets to/from [tech.v3.tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html) objects.


## Elementwise Arithmetic


Functions in 'tech.v3.datatype.functional' all will apply various elementwise
arithmetic operations to a column lazily returning a new column.  It is highly recommended to
remove all missing values before using elemwise arithmetic as the `functional` namespace
has no knowledge of missing values.  Integer columns with missing values will be upcast
to float or double columns in order to support a missing value indicator.

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


## Forcing Lazy Evaluation

 The dataset system relies on index indirection and laziness quite often.  This allows
 you to aggregate up operations and pay relatively little for them however sometimes
 it increases the accessing costs of the data by an undesirable amount.  Because
 of this we use `clone` quite often to force calculations to complete before
 beginning a new stage of data processing.  Clone is multithreaded and very efficient
 often boiling down into either parallelized iteration over the data or
 `System/arraycopy` calls.

 Additionally calling 'clone' after loading will reduce the in-memory size of the
 dataset by a bit - sometimes 20%.  This is because lists that have allocated extra
 capacity are copied into arrays that have no extra capacity.

 * [tech.v3.datatype/clone](https://github.com/cnuernber/dtype-next/blob/152f09f925041d41782e05009bbf84d7d6cfdbc6/src/tech/v3/datatype.clj#L95) - Clones the dataset realizing lazy operation and copying the data into
 java arrays.  Will clone datasets or columns.
