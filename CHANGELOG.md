# Changelog
## 4.01
 * Major cleanup of dependencies.  Logging works now, for better or definitely
   at times for worse.  To silence annoying, loud logging call:
   ```clojure
   (tech.ml.dataset.utils/set-slf4j-log-level :info)
   ```.
 * Added a large-dataset reduction namespace: `tech.ml.dataset.reductions`.  Current
   very beta but in general large reductions will reduce to java streams as these have
   parallelization possibilities that sequences do not have; for instance you can
   get a parallel stream out of a hash map.

## 4.00
 * Upgrade to smile 2.5.0

## 3.11
 * Major fix to tech.v2.datatype.mmap/mmap-file - resource types weren't being set in
   default.
 * `tech.ml.dataset/csv->dataset-seq` - Fixed input stream closing before sequence is
   completely consumed.

## 3.10
 * `tech.libs.arrow/write-dataset-seq-to-stream!` - Given a sequence of datasets, write
    an arrow stream with one record-batch for each dataset.
 * `tech.libs.arrow/stream->dataset-seq-copying` - Given an arrow stream, return a
    sequence of datasets, one for each arrow data record.
 * `tech.libs.arrow/stream->dataset-seq-inplace` - Given an arrow stream, return a
    sequence of datasets constructed in-place on memory mapped data.  Expects to be
	used with in a `tech.resource/stack-resource-context` but accepts options for
	`tech.v2.datatype.mmap/mmap-file`.
 * `tech.libs.arrow/visualize-arrow-stream` - memory-maps a file and returns the arrow
    structure in a way that prints nicely to the REPL.  Useful for exploring an arrow
	file and quickly seeing the low level structure.
 * `tech.ml.dataset/csv->dataset-seq` - Given a potentially large csv, parse it into
    a sequence of datasets.  These datasets are guaranteed to share a schema and so
	an efficient form of writing really large arrow files is to using this function
	along with `tech.libs.arrow/write-dataset-seq-to-stream!`.


## 3.08
#### Arrow Support
 * Proper arrow support.  In-place or accelerated copy pathway into the jvm.
 * 'tech.libs.arrow` exposes a few functions to dive through arrow files and
   product datatsets.  Right now only stream file format is supported.
   Copying is supported via their blessed API.  In-place is supported by
   a more or less clean room implementation using memory mapped files.  There
   will be a blog post on this soon.
 * tech.datatype has a new namespace, tech.v2.datatype.mmap that supports memory
   mapping files and direct memory access for address spaces (and files) larger
   than the java nio 2GB limit for memory mapping and nio buffers.

## 3.07
 * Issue 122 - Datasets with columns of datasets did not serialize to nippy.

## 3.06
 * Issue 118 - sample, head, tail, all set :print-index-range so the entire ds prints.
 * Issue 119 - median in descriptive stats
 * Issue 117 - filter-column allows you to pass in a value for exact matches.
 * Updated README thanks to joinr.
 * Updated walkthrough.

## 3.05
 * Rebuilt with java8 so class files are java8 compatible.

## 3.04 - Bad release, built with java 11
 * Issue 116 - `tech.ml.dataset/fill-range-replace` - Given a numeric or date column,
   interpolate column such that differences between successive vaules are smaller
   than a given cutoff.  Use replace-missing functionality on all other columns
   to fill in values for generated rows.
 * Issue 115 - `tech.ml.dataset/replace-missing` Subset of replace-missing from
   tablecloth implemented.

## 3.03
 * Bugfix - Some string tables saved out with version 2.X would not load correctly.

## 3.02
 * Issue 98 - Reading csv/xlsx files sometimes produce numbers which breaks setting
   colnames to keywords.
 * Issue 113 - NPE when doing hasheq on empty datatset
 * Issue 114 - Columns now have full hasheq implementation - They are
   `IPersisentCollections`.

## 3.01
 * Datasets implement IPersistentMap.  This changes the meaning of `(seq dataset)`
   whereas it used to return columns it now returns sequences of map entries.
   It does mean, however, that you can destructure datasets in let statements to
   get the columns back and use clojure.core/[assoc,dissoc], contains? etc.
   Some of the core Clojure functions, such as select-keys, will change your dataset
   into a normal clojure persistent map so beware.


## 2.15
* fix nippy save/load for string tables.
* string tables now have arraylists for their int->str mapping.
* saving encoded-text columns is now possible with their encoding object.

## 2.14 - BAD RELEASE, NIPPY SAVE/LOAD BROKEN
 * There is a new parse type: `:encoded-text`.  When read, this will appear to be a
   string column however the user has a choice of encodings and utf-8 is the default.
   This is useful when you need a particular encoding for a column.  It is roughly
   twice as efficient be default as a normal string encoding (utf-8 vs. utf-16).

## 2.13
 * `nth`, `map` on packed datetime columns (or using them as functions)
   returns datetime objects as opposed to their packed values.  This means that if you
   ask a packed datetime column for an object reader you get back an unpacked value.

## 2.12
 * Better support of `nth`.  Columns cache the generic reader used for nth queries
   and all tech.v2.datatype readers support nth and count natively in base java
   interface implementations.
 * New namespace - `tech.ml.dataset.text.bag-of-words` that contains code to convert
  a dataset with a text field into a dataset with document ids and  and a
  document-id->token-idx dataset.
 * [Quick Reference](docs/quick-reference.md)

## 2.11
 * After several tries got docs up on cljdoc.  Need to have provided deps cleaned
   up a bit better.

## 2.09
 - Include logback-classic as a dependency as smile.math brings in slf4j and this causes
   an error if some implementation of slf4j isn't included thus breaking things like
   cljdoc.
 - Experimental options options for parsing text (:encoded-text) when dealing with
   large text fields.


## 2.08
 * Major datatype datetime upgrade - [Issue 31](https://github.com/techascent/tech.datatype/issues/31)


## 2.07
 * Bugfix - string tables that required integer storage were written out
   incorrectly to nippy files.


## 2.06
 * `left-join-asof` - Implementation of algorithms from pandas'
    [`merge_asof'](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.merge_asof.html).


## 2.05
 * Bugfix release - We now do not ever parse to float32 numbers by default.  This was
   silently causing data loss.  The cost of this is that files are somewhat larger and
   potentially we need to have an option to set the default sequence of datatypes
   attempted during data parsing.


## 2.04
 * Added `concat-copying`.  This is much faster when you want to concatenate many
   things at the cost of copying the data and thus potentially increasing the working
   set size in memory.


## 2.03
 * Saving to nippy is much faster because there is a new function to efficiently
   construct a string table from a reader of strings:
   `tech.ml.dataset.string-table/string-table-from-strings`.

## 2.02
 * **breaking change** - Remove date/uuid inference pathway for strings from
   mapseq/spreadsheet pathways.
 * nippy freeze/thaw is efficiently supported :-).


## 2.01
 * Issue-94 - Ragged csv data loads automatically now.
 * Issue-87 - Printing double numbers is much better.
 * Fixed saving tsv files - was writing out csv files.
 * Fixed writing packed datatypes - was writing integers.
 * Added parallelized loading of csv - helps a bit but only if parsing
   is really expensive, so only when lots of datetime types or something
   of that nature.

## 2.0
 * No changes from beta-59

## 2.0-beta-59
 * `tech.datatype` now supports persistent vectors made via `clojure.core.vector-of`.
   `vector-of` is a nice middle ground between raw persistent vectors and java arrays
   and may be a simple path for many users into typed storage and datasets.


## 2.0-beta-58
 - mistake release...nothing to see here....


## 2.0-beta-57
 * Issue-92 - ->dataset failed for map-style datasets
 * Issue-93 - use smile.io to load arrow/parquet files via `->dataset`
 * Issue-91 - apply doesn't work with columns or readers

## 2.0-beta-56
**breaking changes**
 * Upgraded smile to latest version (2.4.0).  This is a very new API so if
   you are relying transitively on smile via dataset this may have broke your
   systems.  Smile 1.4.X and smile 2.X are very different interfaces so this
   is important to get in before releasing a 2.0 version of dataset.

 There is now an efficient conversion to/from smile dataframes.

### New Functions
   * `->dataset` conversion a smile dataframe to a dataset.
   * `dataset->smile-dataframe` conversion a dataset to a smile dataframe.
     Columns that are reader based will be copied into java arrays.  To enable
	 predictable behavior a new function was added.
   * `ensure-array-backed` - ensure each column in the dataset has a zerocopy
   conversion to a java array enabled by `tech.v2.datatype/->array`.
   * `invert-string->number` - The pipeline function `string->number` stores a string
     table in the column metadata.  Using this metadata, invert the string->number
	 operation returning the column back to its original state.  This metadata is
	 :label-map which is a map from column-data to number.


## 2.0-beta-55
 * Issue-89 - column iterables operate in object space meaning missing
   values are nil as opposed to the datatype's missing value indicator.

## 2.0-beta-XX
 * Datatype readers now suppport typed java stream creation (typedStream method).

## 2.0-beta-54
 * Issue-88 - rename column fails on false name

## 2.0-beta-53
 * Issue-86 - joins on datasets with not typical names
 * Issue-85 - select-rows can take a scalar.

## 2.0-beta-52
 * `dtype/clone` works correctly for arrays.

## 2.0-beta-51
 * profiled group-by-column quite a bit.  Found/fixed several issues,
   about 10X faster if table is wide as compared to long.
 * Fixed printing in a few edge cases.
 * Issue-84 - `tech.ml.dataset.column/scan-data-for-missing` fix.

## 2.0-beta-50
 * Memory optimization related to roaring bitmap usage.
 * Issue-82 - Empty datasets no longer printed.

## 2.0-beta-49
 * Small fix to printing to make pandoc work better.

## 2.0-beta-48
 * much better printing.  Dataset now correctly print multiline column data and there
   are a set of options to control the printing.  See `dataset->str`.

## 2.0-beta-47
 * UUIDs are now supported as datatypes.  This includes parsing them from strings
   out of csv and xlsx files and as a fully supported object in mapseq pathways.

## 2.0-beta-46
 * Conversion of instant->zoned-date-time uses UTC zone instead of system zone.

## 2.0-beta-45
 * issue-76 - quartiles for datetime types in descriptive stats
 * issue-71 - Added shape function to main dataset api.  Returns shape in row major
   format.
 * issue-69 - Columns elide missing values during print operation.

## 2.0-beta-44
 * drop-rows on an empty set is a noop.
 * `tech.ml.dataset.column/stats` was wrong for columns with missing
   values.


## 2.0-beta-43
 * `tech.v2.datatype` was causing double-read on boolean readers.
 * issue-72 - added `max-num-columns` because csv and tsv files with more than 512
   columns were failing to parse.  New default is 8192.
 * issue-70 - The results of any join have two maps in their metadata -
   :left-column-names - map of original left-column-name->new-column-name.
   :right-column-names - map of original right-column-name->new-column-name.


## 2.0-beta-42
 * `n-initial-skip-rows` works with xlsx spreadsheets.
 * `assoc`, `dissoc` implemented in the main dataset namespace.

## 2.0-beta-41
 * issue-67 - Various `tech.v2.datatype.functional` functions are updated to be
 more permissive about their inputs and cast the result to the appropriate
 datatype.

## 2.0-beta-40
 * issue-65 - datetimes in mapseqs were partially broken.
 * `tech.v2.datatype.functional` will now change the datatype appropriately on a
    lot of unary math operations.  So for instance calling sin, cos, log, or log1p
	on an integer reader will now return a floating point reader.  These methods used
	to throw.
 * subtle bug in the ->reader method defined for object arrays meant that sometimes
   attempting math on object columns would fail.
 * `tech.ml.dataset/column-cast` - Changes the column datatype via a an optionally
   privided cast function.  This function is powerful - it will correctly convert
   packed types to their string representation, it will use the parsing system on
   string columns and it uses the same complex datatype argument as
   `tech.ml.dataset.column/parse-column`:
```clojure
user> (doc ds/column-cast)
-------------------------
tech.ml.dataset/column-cast
([dataset colname datatype])
  Cast a column to a new datatype.  This is never a lazy operation.  If the old
  and new datatypes match and no cast-fn is provided then dtype/clone is called
  on the column.

  colname may be a scalar or a tuple of [src-col dst-col].

  datatype may be a datatype enumeration or a tuple of
  [datatype cast-fn] where cast-fn may return either a new value,
  the :tech.ml.dataset.parse/missing, or :tech.ml.dataset.parse/parse-failure.
  Exceptions are propagated to the caller.  The new column has at least the
  existing missing set if no attempt returns :missing or :cast-failure.
  :cast-failure means the value gets added to metadata key :unparsed-data
  and the index gets added to :unparsed-indexes.


  If the existing datatype is string, then tech.ml.datatype.column/parse-column
  is called.

  Casts between numeric datatypes need no cast-fn but one may be provided.
  Casts to string need no cast-fn but one may be provided.
  Casts from string to anything will call tech.ml.dataset.column/parse-column.
user> (def stocks (ds/->dataset "test/data/stocks.csv" {:key-fn keyword}))

#'user/stocks
user> (ds/head stocks)
test/data/stocks.csv [5 3]:

| :symbol |      :date | :price |
|---------+------------+--------|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
user> (ds/head stocks)
test/data/stocks.csv [5 3]:

| :symbol |      :date | :price |
|---------+------------+--------|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
user> (take 5 (stocks :price))
(39.81 36.35 43.22 28.37 25.45)
user> (take 5 ((ds/column-cast stocks :price :string) :price))
("39.81" "36.35" "43.22" "28.37" "25.45")
user> (take 5 ((ds/column-cast stocks :price [:int32 #(Math/round (double %))]) :price))
(40 36 43 28 25)
user>
```


## 2.0-beta-29
 * renamed 'column-map' to 'column-name->column-map'.  This is a public interface change
   and we do apologize!
 * added 'column-map' which maps a function over one or more columns.  The result column
   has a missing set that is the union of the input columns' missing sets:
```clojure
user> (-> (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}])
          (ds/column-map
           :summed
           (fn ^double [^double lhs ^double rhs]
             (+ lhs rhs))
           :a :b))
_unnamed [3 3]:

| :a |    :b | :summed |
|----+-------+---------|
|  1 |       |         |
|    | 2.000 |         |
|  2 | 3.000 |   5.000 |
user> (tech.ml.dataset.column/missing
       (*1 :summed))
#{0,1}
```

## 2.0-beta-38
 * [issue-64] - more tests revealed more problems with concat with different column
   types.
 * added `tech.v2.datatype/typed-reader-map` where the result datatype is derived
   from the input datatypes of the input readers.  The result of map-fn is
   unceremoniously coerced to this datatype -
```clojure
user> (-> (ds/->dataset [{:a 1.0} {:a 2.0}])
               (ds/update-column
                :a
                #(dtype/typed-reader-map (fn ^double [^double in]
                                           (if (< in 2.0) (- in) in))
                                         %)))
_unnamed [2 1]:

|     :a |
|--------|
| -1.000 |
|  2.000 |
```
 * Cleaned up the tech.datatype widen datatype code so it models a property type graph
   with clear unification rules (where the parent are equal else :object).

## 2.0-beta-37
 * [issue-64] - concat columns with different datatypes does a widening.  In addition,
   there are tested pathways to change the datatype of a column without changing the
   missing set.
 * `unroll-column` takes an optional argument `:indexes?` that will record the source
   index in the entry the unrolled data came from.

## 2.0-beta-36
 * generic column data lists now support `.addAll`

## 2.0-beta-35
 * `tech.datatype` - all readers are marked as sequential.
 * `unroll-column` - Given a column that may container either iterable or scalar data,
    unroll it so it only contains scalar data duplicating rows.
 * Issue 61 - Empty bitsets caused exceptions.
 * Issue 62 - IP addresses parsed as durations.

## 2.0-beta-34
 * Major speed (100x+) improvements to `tech.ml.dataset.column/unique` and especially
   `tech.ml.dataset.pipeline/string->number.

## 2.0-beta-33
 * `tech.v2.datatype` namespace has a new function - [make-reader](https://github.com/techascent/tech.datatype/blob/d735507fe6155e4e112e5640df4c211213f0deba/src/tech/v2/datatype.clj#L458) - that reifies
   a reader of the appropriate type.  This allows you to make new columns that have
   nontrivial translations and datatypes much easier than before.
 * `tech.v2.datatype` namespace has a new function - [->typed-reader](https://github.com/techascent/tech.datatype/blob/5b4745f728a2773ae542fac9613ffd1c482b9750/src/tech/v2/datatype.clj#L557) - that typecasts the incoming object into a reader of the appropriate datatype.
 This means that .read calls will be strongly typed and is useful for building up a set
 of typed variables before using `make-reader` above.
 * Some documentation on the implications of
   [columns, readers, and datatypes](docs/columns-readers-and-datatypes.md).

## 2.0-beta-32
 * Issue 52 - CSV columns with empty column names get named after their index.  Before they would cause
   an exception.
 * `tech.datatype` added a [method](https://github.com/techascent/tech.datatype/blob/bcffe8abe81a53022a5e5d24eae2577c58287bb7/src/tech/v2/datatype.clj#L519)
   to transform a reader into a  persistent-vector-like object that derives from
   `clojure.lang.APersistentVector` and thus gains benefit from the excellent equality
   and hash semantics of persistent vectors.

## 2.0-beta-31
 * Fixed #38 - set-missing/remove-rows can take infinite seqs - they are trimmed to
   dataset length.
 * Fixed #47 - Added [`columnwise-concat`](https://github.com/techascent/tech.ml.dataset/blob/bb3f3dbad78a04d81c08d6ae8f1507c6f4e26ed9/src/tech/ml/dataset.clj#L177)
   which is a far simpler version of dplyr's
   https://tidyr.tidyverse.org/reference/pivot_longer.html.  This is implemented
   efficiently in terms of indexed reader concatentation and as such should work
   on tables of any size.
 * Fixed #57 - BREAKING PUBLIC API CHANGES - We are getting more strict on the API - if
   a function is dataset-last (thus appropriate for `->>`) then any options must be
   passed before the dataset.  Same is true for the set of functions that are dataset
   first.  We will be more strict about this from now on.

## 2.0-beta-30
 * Parsing datetime types now works if the column starts with missing values.
 * An efficient formulation of java.util.map is introduced for when you have
   a bitmap of keys and a single value:
   `tech.v2.datatype.bitmap/bitmap-value->bitmap-map`.  This is used for
   replace-missing type operations.

## 2.0-beta-29
 * `brief` now does not return missing values.  Double or float NaN or INF values
   from a mapseq result in maps with fewer keys.
 * Set of columns used for default descriptive stats is reduced to original set as
   this fits on a small repl nicely.  Possible to override.  `brief` overrides this
   to provide defaults to get more information.
 * `unique-by` returns indexes in order.
 * Fixed #51 - mapseq parsing now follows proper number tower.

## 2.0-beta-28
 * Fixed #36 - use key-fn uniformly across all loaded datatypes
 * Fixed #45 - select can take a map.  This does a selection and
     a projection to new column names.
 * Fixed #41 - boolean columns failed to convert to doubles.
 * Fixed #44 - head,tail,shuffle,rand-nth,sample all implemented in format
     appropriate for `->>` operators.

## 2.0-beta-27
 * Update `tech.datatype` with upgraded and fewer dependencies.
   - asm 7.1 (was 7.0)
   - org.clojure/math.combinatorics 1.6 (was 1.2)
   - org.clojure/test.check 1.0.0

## 2.0-beta-25
 * Optimized filter.  Record of optimization is on
   [zulip](https://clojurians.zulipchat.com/#narrow/stream/151924-data-science/topic/tech.2Eml.2Edataset.20-.20filter).
   Synopsis is a speedup of like 10-20X depending on how much work you want to do :-).
   The base filter pathway has a speedup of around 2-4X.

## 2.0-beta-23
 * Updated description stats to provide list of distinct elements for categorical
   columns of length less than 21.
 * Updated mapseq system to provide nil values for missing data as opposed to the
   specific column datatype's missing value indicator.  This can be overridden
   by passing in `:missing-nil?` false as an option.
 * Added `brief` function to main namespace so you can get a nice brief description
   of your dataset when working from the REPL.  This prints out better than
   `descriptive-stats`.

## 2.0-beta-21
 * loading jsons files found issues with packing.
 * optimized conversion to/from maps.

## 2.0-beta-20
 * sort-by works with generic comparison fns.

## 2.0-beta-19
 * descriptive stats works with mixed column name types
 * argsort is now used for all sort functions
 * `->` versions of sort added so you can sort in -> pathways
 * instants and such can used for sorting

#### Added Functions
 - `column->dataset` - map a transform function over a column and return a new
   dataset from the result.  It is expected the transform function returns a map.
 - `drop-rows`, `select-rows`, `drop-columns` - more granular select calls.
 - `append-columns` - append a list of columns to a dataset.  Used with column->dataset.
 - `column-labeled-mapseq` - Create a sequence of maps with a :value and :label members.
   this flattens the dataset by producing Y maps per row instead of 1 map per row
   where the maps themselves are labeled with the value in their :value member.  This
   is useful to building vega charts.
 - `->distinct-by-column` - take the first row where a given key is present.  The arrow
   form of this indicats the dataset is the first argument.
 - `->sort-by`, `->sort-by-column` - Forms of these functions for using in `(->)`
    dataflows.
 - `interpolate-loess` - Produce a new column from a given pair of columns using loess
    interpolation to create the column.  The interpolator is saved as metadata on the
	new column.



## 2.0-beta-16
* Missing a datetime datatype for parse-str and add-to-container! means
  a compile time error.  Packed durations can now be read from mapseqs.

## 2.0-beta-15
* Descriptive stats now works with instants.

## 2.0-beta-14
* Descriptive stats now works with datetime types.

## 2.0-beta-12
* Support for parsing and working with durations.  Strings that look like times -
   "00:00:12" will be parsed into hh:mm:ss durations.  The value can have a negative
   sign in front.  This is in addition to the duration's native serialization string
   type.
* Added short test for tensors in datasets.  This means that the venerable print-table
  is no longer enough as it doesn't account for multiline strings and thus datatets
  with really complex things will not print correctly for a time.

## 2.0-beta-11
* Various fixes related to parsing and working with open data.
* `tech.ml.dataset.column/parse-column` - given a string column that failed to parse for
  some reason, you can force the system to attempt to parse it using, for instance,
  relaxed parsing semantics where failures simply record the failure in metadata.
* relaxed parsing in general is supported across all input types.

## 0.26
### Added
* rolling (rolling windows of computed functions - math operation)
* dataset/dssort-by
* dataset/ds-take-nth

## 0.22
### Added
* PCA
