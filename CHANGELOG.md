# Changelog
# 7.066
 * hamf bugfix
 
# 7.065
 * hamf persistent vector upgrade
 * promotional object parser fix for na-as-missing? option
 * Switch from object locking to reentrant locks in group-by-column-agg
 
# 7.064
 * Small perf optimizations around sparse columns and arrow.
 
# 7.063
 * Hamf upgrade - fast merge iterators, better sort operator
 * Bugfix in tech.v3.dataset.rolling - options map was ignored - now all options should be respected
 * Major arrow upgrade - support for sparse columns save/load in arrow.  See [test/tech/v3/libs/arrow_tests.clj]
 
# 7.062
 * hamf bugfix in apply-concat.

# 7.061
 * Upgrade to hamf to fix pmap with custom pool issue and initial cut at sparse columns.  There is no serialization
   yet as that requires significant changes to arrow to work for our intended use case.

# 7.060
 * Fixes [issue 458](https://github.com/techascent/tech.ml.dataset/issues/458) - replace missing with a value
   works correctly when a column contains all missing values.

# 7.059
 * dtype-next upgrade to fix clone-after-filter issue.

# 7.058
 * faster single column reduction when you have large columns and many missing -- avoids per-idx binary search of missing set.

# 7.057
 * Slightly faster arrow compressed writies.
 * column-cast no longer appends roaring bitmaps to metadata unless requested.

# 7.056
 * Arrow support for UUID and bigdecimal types.

# 7.055
 * Upgrade dtype-next to [version 10.136](https://github.com/cnuernber/dtype-next/blob/master/CHANGELOG.md#10136).

# 7.053
 * Column parsers are more rigorous in promoting their datatypes after clear op.

# 7.052
 * Fixing update-values - found untested (on mac) pathway that failed when run in cloud.

# 7.051
 * Much faster string table clone and much faster arrow write of string tables.

# 7.050
 * fix bug in stringtable clone.

# 7.049
 * Optimizations to string table clone, string table create and arrow serialization.

# 7.047
 * hamf bugfix for update-values.

# 7.046
 * dataset parsers return something that is not a dataset when the internal datasets have no columns.

# 7.045
 * Bulk add-constant! method used for adding missing values.

# 7.044
 * initial support for clearing dataset parsers - resets their row count but does not reset the schema.  Use tech.v3.dataset.protocols/ds-clear.

# 7.043
 * Legacy smile -- 2.6.0 -- support was removed.  Support for later smile versions has moved to the [scicloj system](https://github.com/scicloj/scicloj.ml.smile) and operations like PCA are best implemented at this time using neanderthal.

# 7.042
 * Upgrade hamf to get new api methods - lines and re-matches.

# 7.041
 * Slightly faster promotional object parser.

# 7.040
 * Fix for [issue 450](https://github.com/techascent/tech.ml.dataset/issues/450) - emapped columns could reduce as
   a different type than declared in the emap declaration.
 * Small perf improvements for unique-by.

# 7.039
 * Fix error in dtype-next/native-buffer/native-buffer->byte-array

# 7.038
 * Upgrade to hamf 2.020.
 * Fix for [issue 447](https://github.com/techascent/tech.ml.dataset/issues/447) - filter column by keyword.

# 7.037
 * Nippy loading is about 2x faster in the case of large string tables.
 * Arrow read pathways support :text-as-strings? to mirror :strings-as-text? on the write side so you can save out uncompressed data in the fastest-to-read format.

# 7.036
 * Major optimization (>9x!) loading of arrow files when large string tables/dictionaries are used.

# 7.035
 * Latest dtype-next (10.124) - contains upgrades to ham-fisted which allow pmap et al. to accept arbitrary executor services.
 * Fix for [issue 438](https://github.com/techascent/tech.ml.dataset/issues/438) - keyword dataset names in tribuo.
 * Fix for [issue 435](https://github.com/techascent/tech.ml.dataset/issues/435) - pd-merge's outer must accept empty datasets.
 * Fix for issues 432 and 371 - select-row-type operations don't remove `:print-index-range :all` metadata.



# 7.034
 * Reverted transit encoding of instant back to milliseconds since epoch as js api doesn't support microseconds since epoch.

# 7.033
 * [issue-434](https://github.com/techascent/tech.ml.dataset/issues/413) - bad transit encoding - packed instants are microseconds since epoch and have been for a while - not milliseconds since epoch.

# 7.031
 * [issue-413](https://github.com/techascent/tech.ml.dataset/issues/413) - reduce with packed columns.
 * [issue-414](https://github.com/techascent/tech.ml.dataset/issues/414) - categorical maps are now integers.
 * [issue-410](https://github.com/techascent/tech.ml.dataset/issues/410) - json parsing fails if parser-fn is provided.

# 7.030
 * [issue-408](https://github.com/techascent/tech.ml.dataset/issues/408) - xlsx files with numberic column names now load.
 * dtype-next upgrade fixing a few issues, most notably [issue-99](https://github.com/cnuernber/dtype-next/issues/99).

# 7.029
 * large parquet files now load - slowly as loading can't be parallelized - without holding onto more memory than they should.

# 7.028
 * [issue 400](https://github.com/techascent/tech.ml.dataset/issues/400) - CSV parser issue and upgrade.
 * [issue 401](https://github.com/techascent/tech.ml.dataset/issues/401) - parquet file failed to parse - missing columns.

# 7.027
 * Moved transit bindings from tmdjs into tech.v3.libs.clj-transit.

# 7.026
 * column sub-buffer failed to offset roaring bitmap missing indexes.

# 7.025
 * New option - `:disable-na-as-missing?` - to [disable treating NA as missing](https://github.com/techascent/tech.ml.dataset/pull/399).
 * Pathway to generically get a [tribuo trainer](https://github.com/techascent/tech.ml.dataset/pull/393).
 * Fix for tribuo changing [predicted column datatypes](https://github.com/techascent/tech.ml.dataset/pull/397).

# 7.024
 * Faster group-by-column-agg when a large (500+ entries) agg map is passed in.
 * Small optimizations to the categorical one-hot-encoding pathway.

# 7.023
 * [Issue 387](https://github.com/techascent/tech.ml.dataset/issues/387) - select now respects persistent vectors of booleans.

# 7.022
 * Issue with pd-merge where exception is thrown if all columns are used for join.
 * Allow system to load duplicate headers - [PR 386](https://github.com/techascent/tech.ml.dataset/pull/386) - thanks ezrand.
 * Bump fastexcel version and expose [StableID](https://github.com/techascent/tech.ml.dataset/pull/385) - thanks again ezrand.

# 7.021
 * hamf typed-nth operations (dnth, fnth, etc) that are efficient
   when input is the analogous primitive array.

# 7.020
 * hamf perf upgrades.
 * big perf upgrade for parsing sequences of maps.

 # 7.019
 * hamf perf upgrades.

# 7.018
 * hamf perf upgrades.

# 7.017
 * hamf perf upgrades.
 * Fix for [critical CVE-2021-40531](https://nvd.nist.gov/vuln/detail/CVE-2021-40531).

# 7.016
 * hamf perf upgrades.

# 7.015
 * hamf fix for compose-reducers.

# 7.013
 * Fixes join regression.  Join algorithm refactored to use hamf primitives.

# 7.012
 * hamf-2.0 upgrade.
 * Fix for [issue-377](https://github.com/techascent/tech.ml.dataset/issues/377).
 * Moved from broken Travis CI to Github CI thanks to @iperdomo.
 * Documentation fix thanks to @mars0i.

# 7.011
 * Moved to custom linkedhashmap implementation in hamf that has optimized union *and*
   has equiv semantics for the keys.  This is not a persistent map of any sort but is
   at least a step closer.  Potential fix for issue 372.

# 7.010
 * Fix for serious error in fastruct equiv pathway.
 * minimal, much faster pathways for column (tech.v3.dataset.impl.column/construct-column)
   and dataset (tech.v3.dataset.impl.dataset/construct-dataset) construction.  These pathways
   will not detect errors nor will they detect missing values so use them with care.


# 7.009
 * Small optimizations to bring back some performance for group-by-column-agg that was lost
   between 6 and 7.

# 7.007
 * row-at defaults to a copying operation so you can safely use this with datasets
   that may be zero-copied from other sources.
 * Fix for [issue 367](https://github.com/techascent/tech.ml.dataset/issues/367)

# 7.006
* dtype-next optimization for native-buffer->string

# 7.005
 * various dtype-next upgrades.

# 7.002
 * dtype-next upgrade.

# 7.001
 * Big dtype-next upgrade bringing pass-by-value and return-by-value and fixing some API issues
   in tech.v3.datatype.functional.

# 7.000-beta-55
 * dataset->csv-seq now uses charred's batch loading system and is parallelized.  See docs.

# 7.000-beta-53
 * Fixes [issue-363](https://github.com/techascent/tech.ml.dataset/issues/363) - ds/rows will now elide keys
   for missing values.  This imposes a small perf hit during read but allows things like the JSON serialization
   system to work better and is a bit more idiomatic.

# 7.000-beta-51
 * Huge dtype-next upgrade - we fixed a lot of argument order issues which unforunately means
   existing projects will have issues with latest version if they used the changed apis.  Please
   check out the dtype-next changelog.

# 7.000-beta-38
 * Writing long sequences of datasets into a single arrow file no longer causes
   stack overflow issues (clojure.core/concat is not used any more).

# 7.000-beta-37
 * Major fix to hamf hashtables fixing subtle issue resizing transient hashmaps.

# 7.000-beta-34
 * Major hamf refactoring in preparation for bringing system out of beta.

# 7.000-beta-33
 * Adding `elide-header?` option for printing:
```clojure
user> (vary-meta (ds/head ds) merge {:maximum-precision 3 :elide-header? true})
|    :a |
|------:|
| 0.197 |
| 0.463 |
| 0.765 |
| 0.546 |
| 0.076 |
```

# 7.000-beta-32
 * Arrow's binary datatype is supported so we can read images and such via arrow.
 * Minor ham-fisted update.
 * Minor charred write-json update.
 * [issue 352](https://github.com/techascent/tech.ml.dataset/issues/352) - maximum-precision is supported to control dataset-wide maximum precision
   when formatting doubles.

# 7.000-beta-31
 * large hamf upgrade for faster maps and faster map boolean operations.
 * charred upgrade for faster json parsing when using `key-fn keyword`.
 * [issue 349](https://github.com/techascent/tech.ml.dataset/issues/349) - list types for arrow.

# 7.000-beta-30
 * Higher performance mapseq parsing and dataset creation for more efficient creation of smaller datasets
   via transduce, mapseq-parser, ->dataset and the various csv parsing pathways.

# 7.000-beta-29
 * parquet supports streaming data into output streams.

# 7.000-beta-28
 * m-1 mac support upgraded - arrow lz4 compression, zstd compression and snappy
   support all tested.  dtype-next upgrade required for lz4, dependency upgrade required
   for zstd, snappy.

# 7.000-beta-27
 * New charred with faster json parsing.
 * Updated ham-fisted - maps and vectors derive from the base Clojure classes.

# 7.000-beta-24
 * NEW DOCS!!
 * Faster hamf map creation and mapv-type operations.


# 7.000-beta-23
 * Categorical map producing NAN regression.
 * Column inference regression.

# 7.000-beta-20
 * Really optimized row-mapcat - required some new primitives from ham-fisted.

# 7.000-beta-19
 * row-mapcat has a few simple optimizations.
 * Fixed [issue 346](https://github.com/techascent/tech.ml.dataset/issues/346) - print-all was broken in 7.X.

# 7.000-beta-17
 * dataset group-by operations must respect the initial order of keys in the grouping criteria.
 * group-by-column, group-by are heavily optimized and quite a bit faster for large datasets.

# 7.000-beta-16
 * Latest dtype-next - support for jdk-19 and fix for arggroup.

# 7.000-beta-14
 * Fix for [issue 342](https://github.com/techascent/tech.ml.dataset/issues/341) - join on date columns.

# 7.000-beta-12
 * Large hamf update.
 * fixed a set of issues - descriptive-stats col order, nippy after arrow,
   column correlation type.

# 7.000-beta-10
 * added normal set functions - union, intersection, and difference - to
   `tech.v3.dataset.set`.

# 7.000-beta-9
 * Lots of small dtype-next updates
 * slightly better resource tracking
 * first deps.edn-based release.
 * Smile, poi are no longer auto-included dependencies.  You have to include them manually.
 * pca removed from math, now only in tech.v3.dataset.neanderthal.

# 7.000-beta-6
 * Added a transduce-compatible rf pathway for sequence of maps - `ds/mapseq-rf`.

# 7.000-beta-5
 * Integration of ham-fisted deeply into dtype-next, tmd, and tablecloth

# 6.103
 * issues [329](https://github.com/techascent/tech.ml.dataset/issues/329) and [328](https://github.com/techascent/tech.ml.dataset/issues/328) - CVE related dependency upgrades.

# 6.102
 * [issue 325](https://github.com/techascent/tech.ml.dataset/issues/325) - Usage of tmd delays shutdown requiring `shutdown-agents`.

# 6.101
 * [issue 324](https://github.com/techascent/tech.ml.dataset/issues/324) - ILookup for columns
 * update to charred to fix `unread too far` issues.

# 6.100
 * [issue 323](https://github.com/techascent/tech.ml.dataset/issues/323) - Null schema type in arrow files.

# 6.099
 * [issue 322](https://github.com/techascent/tech.ml.dataset/issues/322) - Categorical maps must be integers.

# 6.098
 * Update to clojure 1.11 for development.
 * Upgrade to dtype-next for unary min,max and to get rid of 1.11 warnings.
 * Upgrade tech.io for updated nippy again to get rid of 1.11 warnings.

# 6.096
 * Latest tech.io - pulls in important charred csv fix.
 * [issue 320](https://github.com/techascent/tech.ml.dataset/issues/320) - specify encoding for files.
 * drop-rows, select-rows can take negative indexes.

# 6.095
 * Better parallelization for column-map.
 * Fix for [issue 307](https://github.com/techascent/tech.ml.dataset/issues/307) - Bugs in categorical mapping.


# 6.094
 * dtype-next upgrade.
 * Fix for round-tripping arrow files with compression.

# 6.093
 * Fixes for issue [259](https://github.com/techascent/tech.ml.dataset/issues/259/), which is same as new issue 311.  `:key-fn` should only be applied once per column and does not have to
   be idemptotent.

# 6.092
 * Fixes for issues [312](https://github.com/techascent/tech.ml.dataset/issues/312/), [315](https://github.com/techascent/tech.ml.dataset/issues/315/), [316](https://github.com/techascent/tech.ml.dataset/issues/316/)

# 6.091
 * Upgrade to latest charred - no user visible change expected.

# 6.090
 * replace-missing when provided both a direction and a default value will fill in missing items
   after the direction is applied with the missing value.
 * Added `:updown` and `:downup` options to replace previous behavior when desired.

# 6.089
 * CSV parsing now supports `:comment-char` that defaults to #.  Lines that begin with this character are ignored.
 * Fix for [issue 304](https://github.com/techascent/tech.ml.dataset/issues/304/) - n-initial-skip-rows not respected when parsing a csv file.
 * Experimental fix for [issue 305](https://github.com/techascent/tech.ml.dataset/issues/305/) - replace-missing with `:down` or `:up` should leave values missing when the initial replacement fails instead of trying the opposite direction.  This may leave datasets with some missing values.

# 6.087
 * Fix pd-merge `:outer` conditional - [issue 302](https://github.com/techascent/tech.ml.dataset/issues/302/)

# 6.086
 * Update dtype-next as [issue 57](https://github.com/cnuernber/dtype-next/issues/57) was blocking tmd deployment on some macs.
 * Fix for [issue 298](https://github.com/techascent/tech.ml.dataset/issues/298) - nippy'd columns fail thaw.

# 6.085
 * Update to dtype-next.
 * Support for decimal types from parquet - thanks to chrysophylax.

# 6.084
 * Various updates to the charred library.
 * column whitelist/blacklists can be column indexes again.

# 6.081
 * json/csv read/write is going through the [charred library](https://github.com/cnuernber/charred).

# 6.080
 * Update json parsing to dtype-next's json parser.

# 6.079
 * Optimized new csv processing system.  Slightly faster and uses less memory.

# 6.078
 * Switched to the new csv processing system in dtype-next for parsing csv's.  This eliminates
   a source of more or less unfixable issues regarding univocity and it should be nearly
   identical in performance while using less memory.
 * Additionally there is a new interface for csv -
   [tech.v3.dataset.io.csv](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.io.csv.html)
   - processing that efficiently allows you to load a CSV into a sequence of datasets based
   on row-counts.
 * The univocity-based processing system will still be kept around as there may be files
   that load significantly faster or that load correctly with the univocity processing
   system.

# 6.077
 * Upgrade to dtype-next to make `(ds/filter-column ds col identity)` consistent w/r/t missing
   values across numeric and object datatypes.
 * `drop-missing` has a 2-arg variant that takes a dataset and column name.  This is a much
   faster pathway than `(ds/filter-column ds col identity)` for dropping missing values.


# 6.076
 * New print options and bug fix for [issue 266](https://github.com/techascent/tech.ml.dataset/issues/266) - printing
   first style of `first ... last` is the default as I think it is generally more useful than just first or last.
   Skipped a version due to bug in this system.

# 6.074
 * [issue 295](https://github.com/techascent/tech.ml.dataset/issues/295) - new-column exported from api had
   incorrect signature.
 * [issue 294](https://github.com/techascent/tech.ml.dataset/issues/294) - arrow files with lz4 dependent-block
   encoding fail for the jpoinz decoder.  The only sane resolution here is to use the C lz4 library decoding system
   while we work through these issues upstream.

# 6.072
 * Support for reading/writing csv, tsv, edn, json bzip2 and zip files.  Zip files
   are only read when there is a single zipentry in them.  bzip2 requires the user
   to require `tech.v3.dataset.bzip2` in order to work.  See namespace documentation.

# 6.071
 * Initial tribuo support - see docs for tech.v3.libs.tribuo.
 * Upgrade dtype-next - claypoole now comes by default.

# 6.069
 * The public java api docs are updated and it has a rowMap overload that supports
   options to pass to pmapDs.

# 6.068
 * `column-map` is no longer lazy when an explicit datatype is provided.  The result
   is now generated immediately in parallel.  Laziness can be achieved via the
   dtype-next emap api along with `assoc`.

# 6.067
 * Defaulting `:strings-as-text?` to false for the multiple dataset pathway as
   support for delta dictionaries was only recently solidified in the [Arrow SDK
   itself](https://issues.apache.org/jira/browse/ARROW-13467).

# 6.066
 * Major rework of arrow support to include support for all known arrow file formats
   and tested files in various formats across latest (7.0.0) pyarrow.
 * Fix for [issue 289](https://github.com/techascent/tech.ml.dataset/issues/289) - cross
   pd-merge produced incorrect result.

# 6.065
 * Fixing [issue 287](https://github.com/techascent/tech.ml.dataset/issues/287) - dataset corrupt after
   nippy serialization.  This had of course nothing to do with nippy but was caused by a bug in
   dataset->data pathway.
# 6.064
 * upgrade dtype-next to eliminate some superfluous logging and to enable
   window positioning on variable rolling windows.
 * Removed neanderthal as a required dependency.  IT is now lazily loaded upon call
   of PCA.

# 6.062
 * Upgrade dtype-next which removed an experimental fast list creation pathway.

## 6.061
 * Construct a dataset with sequences of java.util.HashMap now works.

## 6.060
 * Neanderthal is preferred but is not a required dependency.

## 6.059
 * Lazily load neanderthal specifically for PCA when necessary.

## 6.058
 * Upgrade datatype to provide faster map/vector constructors.
 * Upgrade rowvecs pathway so the copying option is considerably faster for dataset
   of columns or less.

## 6.057
 * Typed out faststruct's constructor.

## 6.056
 * JSON read/write upgraded to support gzip - ".json.gz".

## 6.055
 * Fixes [issue 284](https://github.com/techascent/tech.ml.dataset/issues/284) - unroll column fails on single column dataset.

## 6.054
 * Removed fastexcel as an automatic dependency due to [issue 283](https://github.com/techascent/tech.ml.dataset/issues/283).  The api documentation now
   indicates the known working fastexcel version.
 * Added Reductions namespace with example to Java API.

## 6.053
 * Java API, Documentation and Sample.
 * Non-backward-compatible Fixes to rolling API's `:comp-fn` optional argument - the
   parameters to the function are reversed so that things like `clojure.core/-` work.

## 6.052
 * `tech.v3.dataset.neaderthal/dataset->dense` supports float32 datatypes.
## 6.051
 * Replace-missing on packed datatypes now works - you have to pass in the unpacked
   value (which most users will anyway).

## 6.050
 * Arrow compression is now supported.  See documentation in the libs/arrow namespace.
 * Major dtype-next upgrade - now with a Java (tm) API :-).

## 6.049
 * Arrow has been rebuilt to be minimally dependent on the official arrow SDK
   and support JDK-17.

## 6.048
 * Small upgrade to dtype-next with a more flexible new-array-of-structs definition
 and [documentation](https://cnuernber.github.io/dtype-next/tech.v3.datatype.struct.html#var-new-array-of-structs).
 See [unit tests](https://github.com/techascent/tech.ml.dataset/blob/a7ad63c6082f6731b143ae47d2b8f71456888acb/test/tech/v3/dataset_test.clj#L1381)
 for how to convert an array of structs into a dataset.

## 6.047
 * Support for making datasets out of arrays of structs.

## 6.046
 * Disable automatic file-backed-text because mmap is broken on m-1 macs.  The fix for
   this is moving to JDK-17, btw, where it is a normal API call that works fine.  Don't
   expect this to come back.

## 6.044
 * col-parsers/make-fixed-parser allows you to make fixed type parsers for custom
   datatypes.


## 6.043
 * Text works correctly when mmap pathways are disabled - larray fails on m-1 mac.

## 6.042
 * Intermediate versions before this - Lots of micro optimizations to row-mapcat and
   some to micro-opts to group-by-column-agg.
 * Support for LocalTime datatype.  Parquet and Arrow support this conversion.  Arrow files
   will read localtime back in as the datatype `:time-microseconds`.  Users can use
   `:local-time` as a parser datatype and there is support for parsing some simple
   variations of local-time data.


## 6.036

 * [pmap-ds](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-pmap-ds)
 * [row-mapcat](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-row-mapcat)
 Row mapcat has an option to produce a sequence of datasets.  This flows naturally into
 group-by-column-agg.  Keep this in mind as it keeps the size of the working set in memory
 fairly low.

## 6.035
 * Main api namespaces are code-generated to ensure discoverability.  Namespaces affected are
   tech.v3.datatype, tech.v3.datatype.functional, tech.v3.datatype.datetime, tech.v3.dataset,
   tech.v3.dataset.metamorph.
 * clj-kondo bindings and mostly clean linting pass failing only on 2 places on my dev machine.
   Working with borkdude to deal with small number of current failings.

## 6.031
 * Upgrade to latest dtype-next - fix for ternary <,<=,>,>= in dfn namespace.
 * dtype-next's main api now includes efficient in-place reverse.
 * reverse-rows - reverse the order of the rows of the dataset.
 * select-missing - select only rows where one of the columns has a missing value.
 * The high performance aggregations in the reduce namespace  now support a specialized
   filter argument to filter out a row index very late in the process.

## 6.030
 * [issue 275](https://github.com/techascent/tech.ml.dataset/issues/275) - pokemon.csv failed
 to parse correctly due to quote issues.

## 6.029
 * [issue 267](https://github.com/techascent/tech.ml.dataset/issues/267) - converting between
   probability distributions back to labels quietly ignored NAN values leading to errors.
 * [issue 273](https://github.com/techascent/tech.ml.dataset/issues/273) - Approximate Bayesian Bootstrap
   implemented for replace-missing.
 * [induction](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-induction) - create a new
   dataset via induction over each row of a previous dataset.

## 6.027
 * [issue 274](https://github.com/techascent/tech.ml.dataset/issues/274) - replace-missing drops metadata.
 * Latest dtype-next - specifically new functions in jvm-map namespace.

## 6.026
 * Minor upgrade - ds/concat returns nil of all arguments are nil or nothing is passed in.
 This matches the behavior of concat.

## 6.025
 * [locker](https://cnuernber.github.io/dtype-next/tech.v3.datatype.locker.html) - an efficient
   and threadsafe way to manipulate global variables.
 * [dtype-next issue 42] - elemwise cast on column failed with specific example.

## 6.024
 * nth is now correct on columns with negative indexes - see unit test.
 * nth is now correct on many more dtype types with negative indexes - see dtype-next unit tests.
 * [shift](https://cnuernber.github.io/dtype-next/tech.v3.datatype.functional.html#var-shift) functionality is included in dtype-next.


## 6.023
 * [issue 270](https://github.com/techascent/tech.ml.dataset/issues/270) - join with double columns was failing
   due to set-constant issue in dtype-next.

## 6.022
 * Moved ztellman's primitive-math library into datatype under specific namespace
   prefix due to dtype-next [issue 41](https://github.com/cnuernber/dtype-next/issues/41).

## 6.021
 * tech.v3.dataset.neanderthal/dense->dataset - creates a dataset with integer column names.

## 6.020
 * Fix for missing datetime values causing descriptive stats to fail.

## 6.019
 * [min-n-by-column](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-min-n-by-column) - Find the mininum N rows by column - uses guava minmaxheap under the covers.
   Sorting the result of this is an efficient way to find have a sorted top-N-type operation.
 * Changed the default concatenation pathway to be copying by default.  This often times just
   works better and results in much faster processing pipelines.
 * Fixed a few issues with packed datatypes.
 * Changed extend-column-with-empty so that it copies data.  I am less sure about this
   change but it fixed an issue with packed datatypes at the cost that joins are often
   no longer in place.  So if you get OOM errors now doing certain joins this change
   is the culprit and we should back off and set it back to what it was.

## 6.016
 * Fixed dtype/writer? queries to accurately reflect actual situations.

## 6.015
 * row-map - map a function across the rows of the dataset (represented as maps).  The result
 should itself be a map and the dataset created from these maps will be merged back into
 the original ds.

## 6.014
 * tech.v3.dataset.reductions/group-by-column-agg can take a tuple of column names in addition
   to a single column name.  In the case of a tuple the grouping will be the vector of column
   values evaluated in object space (so missing will be nil).
 * [issue 260 - numeric, sorting of missing values](https://github.com/techascent/tech.ml.dataset/issues/260)
 * [issue 262 - positional renaming of columns](https://github.com/techascent/tech.ml.dataset/issues/262)
 * [issue 257 - implement pandas merge functionality](https://github.com/techascent/tech.ml.dataset/issues/257)

## 6.011
 * Upgrade tech.io so ls, metadata works for files similarly to how it works for aws.

## 6.010
 * More dtype-next datetime functions - `local-date->epoch-months`, `epoch-months->epoch-days`
   `epoch-days->epoch-months`, `epoch-months->local-date`.
 * Clean way to create new reducers for the reductions namespace - `tech.v3.dataset.reductions/index-reducer`.

## 6.009
 * latest dtype-next.
 * filter-by-column, sort-by-column use unpacked datatypes.
 * New parser argument, 'parse-type', that allows you to turn on string parsing when
   working with a sequence of maps.
 * `->dataset` is more robust to sequences of maps that may contain nil values.
 * `concat` can take datasets with different subsets of columns, just like concat of
   a sequence of maps does.


## 6.006
 * latest dtype-next.  perf fixes for continuous wavelet transform, linear-regression, some
   issue fixes.
 * [issue 257](https://github.com/techascent/tech.ml.dataset/issues/257) - implement pandas
   merge functionality.
 * [issue 255](https://github.com/techascent/tech.ml.dataset/issues/255) - Surprising behavior
   if dataset has no categorical columns (nil vs empty dataset).
 * Upgrade Apache Poi to version 5.0.0.

## 6.005
 * Fix in k-fold; it could fail for certain sizes of datasets.

## 6.004
 * major fix for odd? event? etc. in tech.v3.datatype.functional.
 * head,tail can accept numbers larger than row-count.
 * dtype-next tech.v3.datatype.functional namespace now has vectorized versions of
   sum, dot-product, magnitude-squared, and distance that it will use if the input
   is backed by a double array and if jdk.incubator.vector module is enabled.

## 6.003
 * New accessors - rows, row-at - both work in sequence-of-maps space.  -1 indexes for
   row-at return data indexed from the end so (row-at ds -1) returns the last dataset
   row.
 * When accessing columns via ifn interface - `(col idx)`, negative numbers index from
   the end so for instance -1 retrieves the last value in the column.
 * Large and potentially destabilizing optimization in some cases where argops/argfilter can
   return a range if the filtered region is contiguous and
   then new columns are sitting on sub-buffers of other columns as opposed to indexed-buffers.
   A sub-buffer doesn't pay the same indexing costs and is still capable of accessing the
   underlying data (such as a double array) whereas an indexed buffer cannot faithfully return
   the underlying data.  This can dramatically reduce indexing costs for certain operations
   and allows System/arraycopy and friends to be used for further operations.

## 6.002
 * [issue 254] - Unexpected behaviour of rolling with LazySeq and ChunkedCons.
 * [issue 252] - Nippy serialization of tech.v3.dataset.impl.column.Column.

## 6.001
 * Moved to 3 digit change qualifier.  Hopefully we get a new major version before we hit
   99 bugfixes but no guarantees.
 * [issue 250] - Columns of persistent vectors failed to save/restore from nippy.

## 6.00
 * [issue 247](https://github.com/techascent/tech.ml.dataset/issues/247) - certain pathways would load gzipped as binary.
 * [issue 248](https://github.com/techascent/tech.ml.dataset/issues/248) - Reflection in index code.
 * [issue 249](https://github.com/techascent/tech.ml.dataset/issues/249) - Failure for dataset->data for string columns with missing data.
 * update dtype-next for much more efficient cumulative summation type operations.

## 6.00-beta-15
 * Upgrade to dtype-next for fft-based convolutions.

## 6.00-beta-14
 * New rolling namespace for a high level pandas-style rolling api.
 * Lots of datetime improvements.
 * tech.v3.datatype improvements (new conv1d, diff, gradient functionality).

## 6.00-beta-13
 * Fix for parquet failing to load local files in windows.  Thanks hadoop, that is
   3 hours of my life that will never come back ;-).


## 6.00-beta-12

 * Parquet documentation to address logging slowdown.  If writing parquet files is
   unreasonably slow then please read the documentation on logging.  The java
   parquet implementations logs so much it slows things down 5x-10x.
 * Fix to nippy to be backward compatible.

## 6.00-beta-11
 * [issue 244](https://github.com/techascent/tech.ml.dataset/issues/242) - NPE with packed column.
 * [issue 243](https://github.com/techascent/tech.ml.dataset/issues/243) - xls, xlsx parsing documentation.
 * [issue 208](https://github.com/techascent/tech.ml.dataset/issues/208) - k-fold, train-test-split both now take random seed
   similar to shuffle.


## 6.00-beta-10
 * [issue 242](https://github.com/techascent/tech.ml.dataset/issues/242) - drop-columns
   reorders columns.

## 6.00-beta-9
* [issue 240](https://github.com/techascent/tech.ml.dataset/issues/240) - poor remove columns perf.
* Thorough fix to make `column-map` be a bit more predictable and forgiving.


## 6.00-beta-8
 * Fix for [issue 238](https://github.com/techascent/tech.ml.dataset/issues/238)

## 6.00-beta-7
 * Final upgrade for geni benchmark and moved a utility function to make
   it more generally accessible.

## 6.00-beta-5
 * Perf upgrades - ensuring geni benchmark speed stays constant.

## 6.00-beta-4
 * `[column-map-m](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-column-map-m)

## 6.00-beta-2
 * Small bugfix so missing sets are correct set in `column-map`.

## 6.00-beta-1

 Data types and missing values are much more aggressively inferred - which is
 `O(n-rows)`) throught the api. There is a new API to disable the inference - Either
 pass something that is already a column or pass in a map with keys:
```clojure
  #:tech.v3.dataset{:data ... :missing ... :metadata ... :force-datatype? true}
```
  Put another way, the input to `#{assoc ds/update-column ds/add-column
  ds/add-or-update-column}`  is already a column
  (see `tech.v3.dataset.column/new-column`) or if `:tech.v3.dataset/force-datatype?` is
  true **and** `:tech.v3.dataset/data` is convertible to a reader then the data will not
  be scanned for datatype or missing values.  If the input data is a primitive-typed
  container then it will be scanned for missing values alone and anything else is
  passed through the object parsing system which is what is used for sequences of maps,
  maps of sequences and spreadsheets.

  In this way in general the system will do more work than before - more scans of the
  result of things like transducer pathways and persistent vectors but in return the
  dataset's column datatypes should match the user's expectations.  If too much time is
  being taken up via attempting to infer datatypes and missing sets then the user has
  the option to pass in explicitly constructed columns or column data representations
  both of which will disable the scanning.  Once the data is typed elementwise
  mathematical operations of the type in `:tech.v3.datatype.function` **will not**
  result in further scans the data.


Itemized Changes:

 * `assoc, ds/add-column, ds/update-column, ds/add-or-update-column` type operations all
    upgraded such that datatype and missing are inferred much more frequently.
 * `:tech.ml.dataset.parse/missing`, `:tech.ml.dataset.parse/parse-failure` -> `:tech.v3.dataset/missing`, `:tech.v3.dataset/parse-failure`.
 * `column-map` - Now scans results to infer datatype if not provided as opposed to assuming result is the
    widest of the input column types.  Also users can provide their own function that calculates missing sets as opposed to
	the default behavior being the union of the input columns' missing sets.




## 5.21
 * [Issue 233](https://github.com/techascent/tech.ml.dataset/issues/233) - Poi xlsx parser can now autodetect dates.  Note that fastexcel is the default
   xslx parser so in order to parse xlsx files using poi use `tech.v3.libs.poi/workbook->datasets`.
 * [PR 232](https://github.com/techascent/tech.ml.dataset/pull/232) - Option - `:disable-comment-skipping?` - to disable comment skipping in csv files.


## 5.20
 * Return an Iterable from csv->rows as opposed to a seq.  Iterator-seq has nontrivial overhead.
 * Fixes for issues [229](https://github.com/techascent/tech.ml.dataset/issues/229),
   [230](https://github.com/techascent/tech.ml.dataset/issues/230), and [231](https://github.com/techascent/tech.ml.dataset/issues/231).

## 5.19
 * Using builder model for parquet both for forward compatibility and so we can set an output stream
   as opposed to a file path.  This allows a graal native pathway to work wtih parquet.

## 5.18
 * Graal-native friendly mmap pathways (no requiring resolve, you have to explicity set the implementation in your main.clj file).
 * Parquet write pathway update to make more standard and more likely to work with future versions of parquet.  This means, however, that there will
   no longer be a direct correlation between number of datasets and number of record batches in a parquet file as the standard pathway takes care
   of writing out record batches when a memory constraint is triggered.  So if you save a dataset you may get a parquet file back that contains
   a sequence of datasets.  There are many parquet options, see the documentation for
   [ds-seq->parquet](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html#var-ds-seq-.3Eparquet).

## 5.17
 * [Issue 225](https://github.com/techascent/tech.ml.dataset/issues/224) - column/row selection should return empty datasets when no columns are selected.
 * nil headers now print fine - thanks to DavidVujic.

## 5.15
 * [Issue 224](https://github.com/techascent/tech.ml.dataset/issues/224) - dataset creating fails in map case when all vals are seqs.

## 5.14
 * Another set of smaller upgrades to csv parsing.
 * [Reservoir sampling](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.html#var-reservoir-dataset) is supported for large aggregations.
 * tech.io (and thus nippy) is upgraded.

## 5.13
 * Various optimizations to csv parsing making it a bit (2x) faster.

## 5.12
 * All statistical/reduction summations now use  [Kahan's compensated summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm).  This makes summation
   much more accurate for very large streams of data.
 * [Issue 220](https://github.com/techascent/tech.ml.dataset/issues/220) - confusing behavior on dataset creation.  This may result in different
   behavior than was expected previously when using maps of columns as dataset constructors.

## 5.11
 * Many more algorithms exposed and documentation updated for the
   [apache-data-sketch](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.apache-data-sketch.html)
   namespace.

## 5.10
 * apache data sketch set-cardinality algorithms hyper-log-log and theta.

## 5.07
 * [tech.v3.dataset.reductions](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.reductions.html)
  namespace now includes direct aggregations
  including group-by aggregations and also t-dunnings' t-digest algorithm for
  probabilistic cdf and quantile estimation.

## 5.06
 * Bugs introduced by t-digest version 3.2.  Ignore this release.

## 5.05
 * Ragged per-row arrays are partially support for parquet.

## 5.04
 * `headers?` now also works when writing csv/tsv documents (thanks to behrica).

## 5.03
 * group-by now is done with a linkedhashmap thus the keys are ordered in terms
   of first found in the data.  This is useful for operations such as re-indexing
   a previously sorted dataset as it.

## 5.02
 * Fix for fastexcel files containing formulas.
 * Codox org.ow2.asm fix from dtype-next.

## 5.00
 * Large upgrade to [`dtype-next`](https://github.com/cnuernber/dtype-next/).
 * All public functions are dataset-first.  This breaks compatibility with sort-by
   filter, etc.
 * Namespace changes to be `tech.v3` across the board as opposed to `tech.v2` along
   with `tech.ml`.
 * All smile dataframe functionality is in tech.v3.libs.smile.data.

## 4.03
 * Fix for #136

## 4.02
 * Optimized conversion from a dataset to and from a neanderthal dense matrix is
   now supported -- see tech.ml.dataset.neanderthal.

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
   product datasets.  Right now only stream file format is supported.
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
 * Issue 113 - NPE when doing hasheq on empty dataset
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
