# Changelog
## 2.0-beta-29
 * renamed 'column-map' to 'column-name->column-map'.  This is a public interface change
   and we do apologize!
 * added 'column-map' which maps a function over one or more columns.  The result column
   has a missing set that is the union of the input columns' missing sets.

## 2.0-beta-28
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
