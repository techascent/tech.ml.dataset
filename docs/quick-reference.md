# Quick Reference - Core API

Functions are linked to their source but if no namespace is specified they are
also accessible via the `tech.ml.dataset` namespace.

This is not an exhaustive listing of all functionality; just a quick brief way to find
functions that are we find most useful.


## Loading/Saving

* [->dataset, ->>dataset](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L765) - loads csv, tsv,
  sequence-of-maps, map-of-arrays, xlsx, xls, parquet and arrow.
* [write-csv!](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L989) - Writes csv or tsv with
  gzipping. Depends on scanning file path string to determine options.
* [nippy freeze/thaw support](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L989).
* [dataset->data](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L891) - Useful if you want the entire
 dataset represented as (mostly) pure Clojure/JVM datastructures.  Missing sets are
 roaring bitmaps, data is probably in primitive arrays.  String tables receive special
 treatment.
* [data->dataset](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L918) - Inverse of data->dataset.
* [tech.ml.dataset.parse/csv->rows](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/parse.clj#L719) - Lazily parse a
 csv or tsv returning a sequence of string[] rows.  This uses a subset of the ->dataset options.
* [tech.ml.dataset.parse/rows->dataset](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/parse.clj#L632) - Given
 a sequence of string[] rows, parse data into a dataset.  Uses subset of the ->dataset
 options.


## Accessing Values

* Datasets overload Ifn so are functions of their column names.  `(ds :colname)` will
  return the column named `:colname`.  Datasets are sequence of columns so
  `(map meta ds)` will return a sequence of column metadata.
* Columns are iterable and implement indexed so you can use them with `map`, `count`
  and `nth`.  `nth` allows relatively inefficient random access.
* Efficient random access is supported the `(tech.v2.datatype/->reader col)`
  transformation.  This is guaranteed to return an implementation of `java.util.List`
  and also overloads `IFn` such that like a persistent vector passing in the index
  will return the value - e.g. `(col 0)` returns the value at index 0.  Direct access
  to packed datetime columns may be surprising; call `tech.v2.datatype.datetime/unpack`
  on the column prior to calling `tech.v2.datatype/->reader` to get to the unpacked
  datatype.
* [row-count](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L46).
* [column-count](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L54).
* [mapseq-reader](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/readers.clj#L64) - get the rows of the
 dataset as a `java.util.List` of persistent-map-like maps.  Implemented as a flyweight
 implementation of `clojure.lang.APersistentMap`.  This keeps the data in the backing
 store and lazily reads it so you will have relatively more expensive reading of the
 data but will not increase your memory working-set size.
* [value-reader](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/readers.clj#L44) - Get the rows of the
 dataset as a 'java.util.List' of readers.  These readers behave like persistent maps
 but are not safe to use as keys in maps - use `vec` and get real persistent vectors if
 you intend to call equals or hashCode on these.
* [tech.ml.dataset.column/missing](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/column.clj#L65) - return
 a RoaringBitmap of the missing indexes
* [columns-with-missing-seq](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj)
* [missing](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L281) - Return the union of all missing
  indexes.  Useful in combination with drop-rows to quickly eliminate missing values
  from the dataset.
* [meta, with-meta, vary-meta](https://github.com/clojure/clojure/blob/master/src/clj/clojure/core.clj#L202) - Datasets and columns implement 
  `clojure.lang.IObj` so you can get/set metadata on them freely. `:name` has meaning in the system and setting it
  directly on a column is not recommended.  Metadata is generally carried forward through most of the operations below.


`mapseq-reader` and `value-reader` are lazy and thus `(rand-nth (mapseq-reader ds))` is
a relatively efficient pathway (and fun).  `(ds/mapseq-reader (ds/sample ds))` is also
pretty good for quick scans.

## Print Options

We use these options frequently during exploration to get more/less printing
output.  These are used like `(vary-meta ds assoc :print-column-max-width 10).

* [`:print-column-max-width`](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/print.clj#L97).
* [`:print-index-range`](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/print.clj#L97).
* [`:print-line-policy`](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/print.clj#L93).


## Dataset Exploration

* [head](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L210)
* [tail](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L220)
* [sample](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L237)
* [rand-nth](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L252)
* [descriptive-stats](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L682) - return a dataset of
 columnwise descriptive statistics.
* [brief](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L745) - Return a sequence of maps of
 descriptive statistics.



## Subrect Selection

* [select](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L196) - can be used for renaming.
 Anything iterable can be used for the rows.
* [select-columns](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L237).
* [drop-columns](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L154)
* [select-rows](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L255).
* [drop-rows](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L260).



## Dataset Manipulation

Several of the functions below come in `->column` variants and some come additional
in `->indexes` variants.  `->column` variants are going to be faster than the base
versions and `->indexes` simply return indexes and thus skip creating sub-datasets
so these are faster yet.

* [assoc](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L175) - add or replace columns.
* [dissoc](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj#L200) - remove columns, similar to
 drop-columns.
* [new-dataset](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/impl/dataset.clj#L183) - Create a new dataset from a sequence of columns.  Columns may be actual columns created via `tech.ml.dataset.column/new-column` or they could be maps containing at least keys `{:name :data}` but also potentially `{:metadata :missing}` in order to create a column with a specific set of missing values and metadata.  `:force-datatype true` will disable the system
from attempting to scan the data for missing values and e.g. create a float column
from a vector of Float objects.
* [group-by-column](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L404), [group-by](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L378) - Create a persistent map of value->dataset.  Sub-datasets are created via indexing into the original dataset so data is not copied.
* [sort-by-column](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L466), [sort-by](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L427) - Return a sorted dataset.
* [filter-column](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L350), [filter](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L330) - Return a new dataset with only rows that pass the predicate.
* [column-cast](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj) - Change the datatype of a column.
* [concat-copying](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L553), [concat-inplace](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L544) - Given Y datasets produce a new dataset.  Whether to
do in-place or copying concatenation depends roughly on the number of datasets.
Inplace works best with under 5 datasets where all datasets have an identical row
count.  Copying can increase your ram usage but returns a dataset that will be more
efficient to iterate over later. `(apply ds/concat-copying x-seq)` is
**far** more efficient than `(reduce ds/concat-copying x-seq)`; this also is true for
`concat-inplace`.
* [unique-by-column](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L607), [unique-by](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset/base.clj#L590) - Remove duplicate rows.  Passing in `keep-fn` allows
you to choose either first, last, or some other criteria for rows that have the same
values.
* [columnwise-concat](https://github.com/techascent/tech.ml.dataset/blob/e051de4e82a43b80d2fbcf3d4b52759a9cb878c8/src/tech/ml/dataset.clj) - Concatenate columns into longer
dataset repeating values in other columns.


## Elementwise Arithmetic


Functions in 'tech.v2.datatype.functional' all will apply various elementwise
arithmetic operations to a column lazily returning a new column.
```clojure
       (ds/assoc ds
                 :value (dtype/set-datatype (ds :value) :int64)
                 :shrs-or-prn-amt (dtype/set-datatype (ds :shrs-or-prn-amt) :int64)
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

 * [tech.v2.datatype/clone](https://github.com/techascent/tech.datatype/blob/master/src/tech/v2/datatype.clj#L218) - Clones the dataset realizing lazy operation and where copying the data into java arrays.  Will clone datasets or columns.



## Further Examples

* [Boulder Rescue Response Times](https://nextjournal.com/chrisn/boulder-rescue-response-times/).
