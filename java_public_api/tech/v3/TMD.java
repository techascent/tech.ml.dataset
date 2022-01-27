package tech.v3;


import static tech.v3.Clj.*;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import java.util.Map;
import java.util.Comparator;
import org.roaringbitmap.RoaringBitmap;
import tech.v3.datatype.Buffer;
import tech.v3.datatype.NDBuffer;
import tech.v3.datatype.IFnDef;


/**
 * `tech.ml.dataset` is a high performance library for processing columnar data similar to
 * pandas or R' data table.  Datasets are `maps` of their columns and columns derive from
 * various Clojure interfaces such as IIndexed and IFn to make accessing their data as easy
 * as possible.
 *
 * Columns have a conversion to a `tech.v3.datate.Buffer` object accessible via
 * `tech.v3.DType.toBuffer()` so if you want higher performance non-boxing access that is
 * also available.  Any bit of sequential data can be turned into a column.  The best way
 * is if the data is already in a primitive array or nio buffer use that as a column - it
 * will be used in place.  It is also possible to direclty instantiate a Buffer object in
 * a read-only pathway to create a virtualized column:
 *
 *```java
 *println(head(assoc(colmapDs, kw("c"), new tech.v3.datatype.LongReader() {
 *	public long lsize() { return 10; }
 *	public long readLong( long idx) {
 *	  return 2*idx;
 *	}
 *    })));
 *  //testds [5 3]:
 *
 *  //|  :b | :a | :c |
 *  //|----:|---:|---:|
 *  //| 9.0 |  0 |  0 |
 *  //| 8.0 |  1 |  2 |
 *  //| 7.0 |  2 |  4 |
 *  //| 6.0 |  3 |  6 |
 *  //| 5.0 |  4 |  8 |
 *```
 *
 * Datasets implement a subset of java.util.Map and clojure's persistent map interfaces.
 * This means you can use various `java.util.Map` functions and you can also use
 * `clojure.core/assoc`, `clojure.core/dissoc`, and `clojure.core/merge` in order to add and
 * remove columns from the dataset.  These are exposed in `tech.v3.Clj` as equivalently named
 * functions.  In combination with the fact that columns implement Clojure.lang.IIndexed
 * providing `nth` as well as the single arity IFn invoke method you can do a surprising
 * amount of dataset processing without using bespoke TMD functions at all.
 *
 * All of the functions in `tech.v3.datatype.VecMath` will work with column although most
 * of those functions require the columns to have no missing data.  The recommendation is to
 * do you missing-value processing first and then move into the various elemwise functions.
 * Integer columns with missing values will upcast themselves to double columns for any math
 * operation so the result keeps consistent w/r/t NaN behavior.  Again, ideally missing values
 * should be dealt with before doing operations in the `VecMath` namespace.
 *
 */
public class TMD {
  private TMD(){}

  static final IFn toDatasetFn = requiringResolve("tech.v3.dataset-api", "->dataset");
  static final IFn isDatasetFn = requiringResolve("tech.v3.dataset-api", "dataset?");
  static final IFn rowCountFn = requiringResolve("tech.v3.dataset.base", "row-count");
  static final IFn columnCountFn = requiringResolve("tech.v3.dataset.base", "column-count");
  static final IFn columnFn = requiringResolve("tech.v3.dataset", "column");
  static final IFn missingFn = requiringResolve("tech.v3.dataset-api", "missing");
  static final IFn selectFn = requiringResolve("tech.v3.dataset-api", "select");
  static final IFn selectRowsFn = requiringResolve("tech.v3.dataset-api", "select-rows");
  static final IFn dropRowsFn = requiringResolve("tech.v3.dataset-api", "drop-rows");
  static final IFn selectColumnsFn = requiringResolve("tech.v3.dataset-api", "select-columns");
  static final IFn dropColumnsFn = requiringResolve("tech.v3.dataset-api", "drop-columns");
  static final IFn renameColumnsFn = requiringResolve("tech.v3.dataset-api", "rename-columns");
  static final IFn replaceMissingFn = requiringResolve("tech.v3.dataset.missing", "replace-missing");
  static final IFn rowsFn = requiringResolve("tech.v3.dataset", "rows");
  static final IFn rowvecsFn = requiringResolve("tech.v3.dataset", "rowvecs");
  static final IFn headFn = requiringResolve("tech.v3.dataset", "head");
  static final IFn tailFn = requiringResolve("tech.v3.dataset", "tail");
  static final IFn sampleFn = requiringResolve("tech.v3.dataset", "sample");
  static final IFn shuffleFn = requiringResolve("tech.v3.dataset", "shuffle");
  static final IFn reverseFn = requiringResolve("tech.v3.dataset", "reverse-rows");

  static final IFn columnMapFn = requiringResolve("tech.v3.dataset", "column-map");
  static final IFn rowMapFn = requiringResolve("tech.v3.dataset", "row-map");
  static final IFn rowMapcatFn = requiringResolve("tech.v3.dataset", "row-mapcat");
  static final IFn pmapDsFn = requiringResolve("tech.v3.dataset", "pmap-ds");

  static final IFn sortByFn = requiringResolve("tech.v3.dataset", "sort-by");
  static final IFn sortByColumnFn = requiringResolve("tech.v3.dataset", "sort-by-column");
  static final IFn filterFn = requiringResolve("tech.v3.dataset", "filter");
  static final IFn filterColumnFn = requiringResolve("tech.v3.dataset", "filter-column");
  static final IFn groupByFn = requiringResolve("tech.v3.dataset", "group-by");
  static final IFn groupByColumnFn = requiringResolve("tech.v3.dataset", "group-by-column");
  static final IFn concatCopyingFn = requiringResolve("tech.v3.dataset", "concat-copying");
  static final IFn concatInplaceFn = requiringResolve("tech.v3.dataset", "concat-inplace");
  static final IFn uniqueByFn = requiringResolve("tech.v3.dataset", "unique-by");
  static final IFn uniqueByColumnFn = requiringResolve("tech.v3.dataset", "unique-by-column");

  static final IFn descriptiveStatsFn = requiringResolve("tech.v3.dataset", "descriptive-stats");

  static final IFn pdMergeFn = requiringResolve("tech.v3.dataset.join", "pd-merge");
  static final IFn joinAsof = requiringResolve("tech.v3.dataset.join", "left-join-asof");

  static final Object toNeanderthalDelay = delay(new IFnDef() {
      public Object invoke() {
	//Bindings to make as-tensor work with neanderthal
	require("tech.v3.libs.neanderthal");
	//Actual function to convert a dataset into a neanderthal double or float matrix.
	return requiringResolve("tech.v3.dataset.neanderthal", "dataset->dense");
      }
    });
  static final Object neanderthalToDatasetDelay = delay(new IFnDef() {
      public Object invoke() {
	//tensor bindings
	require("tech.v3.libs.neanderthal");
	return requiringResolve("tech.v3.dataset.neanderthal", "dense->dataset");
      }
    });
  static final Object toTensorDelay = delay(new IFnDef() {
      public Object invoke() {
	return requiringResolve("tech.v3.dataset.tensor", "dataset->tensor");
      }
    });
  static final Object tensorToDatasetDelay = delay(new IFnDef() {
      public Object invoke() {
	return requiringResolve("tech.v3.dataset.tensor", "tensor->dataset");
      }
    });

  static final IFn writeFn = requiringResolve("tech.v3.dataset", "write!");


  /**
   * Basic pathway to take data and get back a datasets.  If dsData is a string
   * a built in system can parse csv, tsv, csv.gz, tsv.gz and .nippy format files.
   * Specific other formats such as xlsx, apache arrow and parquet formats are provided
   * in other classes.
   *
   * Aside from string data formats, you can explicitly provide either a sequence of maps
   * or a map of columns with the map of columns being by far more the most efficient.
   * In the map-of-columns approach arrays of primitive numeric data and native buffers will
   * be used in-place.
   *
   * The options for parsing a dataset are extensive and documented at
   * [->dataset](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var--.3Edataset).
   *
   * Example:
   *
   *```java
   *  Map ds = makeDataset("https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv");
   *  tech.v3.Clj.println(head(ds));
   * // https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:
   * // | symbol |       date | price |
   * // |--------|------------|------:|
   * // |   MSFT | 2000-01-01 | 39.81 |
   * // |   MSFT | 2000-02-01 | 36.35 |
   * // |   MSFT | 2000-03-01 | 43.22 |
   * // |   MSFT | 2000-04-01 | 28.37 |
   * // |   MSFT | 2000-05-01 | 25.45 |
   *Map colmapDs = makeDataset(hashmap(kw("a"), range(10),
   *				       kw("b"), toDoubleArray(range(9,-1,-1))),
   *			       hashmap(kw("dataset-name"), "testds"));
   *println(colmapDs);
   * // testds [10 2]:
   *
   * // |  :b | :a |
   * // |----:|---:|
   * // | 9.0 |  0 |
   * // | 8.0 |  1 |
   * // | 7.0 |  2 |
   * // | 6.0 |  3 |
   * // | 5.0 |  4 |
   * // | 4.0 |  5 |
   * // | 3.0 |  6 |
   * // | 2.0 |  7 |
   * // | 1.0 |  8 |
   * // | 0.0 |  9 |
   *```
   */
  public static Map makeDataset(Object dsData, Map options) {
    return (Map)call(toDatasetFn, dsData, options);
  }
  /**
   * Make a dataset.  See 2-arity form of function.
   */
  public static Map makeDataset(Object dsData) {
    return (Map)call(toDatasetFn, dsData, null);
  }
  /**
   * Returns true if this object is a dataset.
   */
  public static boolean isDataset(Object ds) {
    return (boolean)call(isDatasetFn, ds);
  }
  /** Return the number of rows. */
  public static long rowCount(Object ds) {
    return (long)call(rowCountFn, ds);
  }
  /** Return the number of columns. */
  public static long columnCount(Object ds) {
    return (long)call(columnCountFn, ds);
  }
  /** Return the column named `cname` else throw exception. */
  public static Object column(Object ds, Object cname) {
    return call(columnFn, ds, cname);
  }
  /**
   * Efficiently create a column definition explicitly specifying name, data, missing, and
   * metadata.   The result can be `assoc`d back into the dataset.
   */
  public static Map columnDef(Object name, Object data, Object missing, Object metadata) {
    return hashmap(keyword("tech.v3.dataset", "name"), name,
		   keyword("tech.v3.dataset", "data"), data,
		   keyword("tech.v3.dataset", "missing"), missing,
		   keyword("tech.v3.dataset", "metadata"), metadata);
  }
  /**
   * Efficiently create a column definition explicitly specifying name, data, missing, and
   * metadata.   The result can be `assoc`d back into the dataset.
   */
  public static Map columnDef(Object data, Object missing) {
    return hashmap(keyword("tech.v3.dataset", "name"), "_unnamed",
		   keyword("tech.v3.dataset", "data"), data,
		   keyword("tech.v3.dataset", "missing"), missing);
  }
  /**
   * Efficiently create a column definition explicitly specifying name, data, missing, and
   * metadata.   The result can be `assoc`d back into the dataset.
   */
  public static Map columnDef(Object data, Object missing, Object metadata) {
    return hashmap(keyword("tech.v3.dataset", "name"), "_unnamed",
		   keyword("tech.v3.dataset", "data"), data,
		   keyword("tech.v3.dataset", "missing"), missing,
		   keyword("tech.v3.dataset", "metadata"), metadata);
  }
  /**
   * Select a sub-rect of the dataset.  Dataset names is a sequence of column names that must
   * exist in the dataset.  Rows is a sequence, list, array, or bitmap of integer row
   * indexes to select.  Dataset returned has column in the order specified
   * by `columnNames`.
   */
  public static Map select(Object ds, Object columnNames, Object rows) {
    return (Map)call(selectFn, ds, columnNames, rows);
  }
  /**
   * Select columns by name.  All names must exist in the dataset.
   */
  public static Map selectColumns(Object ds, Object columnNames ) {
    return (Map)call(selectColumnsFn, ds, columnNames);
  }
  /**
   * Drop columns by name.  All names must exist in the dataset.
   * Another option is to use the Clojure function `dissoc`.
   */
  public static Map dropColumns(Object ds, Object columnNames ) {
    return (Map)call(dropColumnsFn, ds, columnNames);
  }
  /**
   * Rename columns providing a map of oldname to newname.
   */
  public static Map renameColumns(Object ds, Map renameMap) {
    return (Map)call(renameColumnsFn, ds, renameMap);
  }
  /**
   * Select rows by index.
   */
  public static Map selectRows(Object ds, Object rowIndexes) {
    return (Map)call(selectRowsFn, ds, rowIndexes);
  }
  /**
   * Drop rows by index.
   */
  public static Map dropRows(Object ds, Object rowIndexes) {
    return (Map)call(dropRowsFn, ds, rowIndexes);
  }
  /**
   * Return the missing set of a dataset or a column in the form of a RoaringBitmap.
   */
  public static RoaringBitmap missing(Object dsOrColumn) {
    return (RoaringBitmap)call(missingFn, dsOrColumn);
  }
  /**
   * Replace the missing values from a column or set of columns.  To replace across
   * all columns use the keyword :all.
   *
   * Strategy can be:
   *
   * * `:up` - take next value
   * * `:down` - take previous value
   * * `:lerp` - linearly interpolate across values.  Datetime objects will have
   *    interpolation in done in millisecond space.
   * * `vector(:value, val)` - Provide this value explicity to replace entries.
   * * `:nearest` - use the nearest value.
   * * `:midpoint` - use the mean of the range.
   * * `:abb` - impute missing values using approximate bayesian bootstrap.
   *
   * Further documentation is located at [replace-missing](https://techascent.github.io/tech.ml.dataset/tech.v3.dataset.html#var-replace-missing).
   */
  public static Map replaceMissing(Object ds, Object strategy, Object columns) {
    Keyword actualStrat;
    Object value;
    if (isVector(strategy)) {
      actualStrat = (Keyword)call(strategy,0);
      value = call(strategy,1);
    }
    else {
      actualStrat = (Keyword)strategy;
      value = null;
    }
    if (value != null ) {
      return (Map)call(replaceMissingFn, ds, columns, actualStrat, value);
    } else {
      return (Map)call(replaceMissingFn, ds, columns, actualStrat);
    }
  }
  /**
   * Replace missing values.  See 3-arity form of function for documentation.
   */
  public static Map replaceMissing(Object ds, Object strategy) {
    return replaceMissing(ds, strategy, kw("all"));
  }
  /**
   * Return the rows of the dataset in a flyweight map format.  Maps share keys
   * and read their data lazily from the base dataset.
   */
  public static Buffer rows(Object ds) {
    return (Buffer)call(rowsFn, ds);
  }
  /**
   * Return the rows of the dataset where each row is just a flat Buffer of data.
   *
   * When copying is true data is copied upon each access from the underlying dataset.  This
   * makes doing something like using each row as the key in a map more efficient.
   */
  public static Buffer rowvecs(Object ds, boolean copying) {
    return (Buffer)call(rowvecsFn, ds, hashmap(kw("copying?"), copying));
  }
  /** Return the rows of the dataset where each row is just a flat Buffer of data. */
  public static Buffer rowvecs(Object ds) {
    return (Buffer)call(rowvecsFn, ds);
  }

  /** Return the first 5 rows of the dataset */
  public static Map head(Object ds) {
    return (Map)call(headFn, ds);
  }
  /** Return the first N rows of the dataset */
  public static Map head(Object ds, long nRows) {
    return (Map)call(headFn, ds, nRows);
  }
  /** Return the last 5 rows of the dataset */
  public static Map tail(Object ds) {
    return (Map)call(tailFn, ds);
  }
  /** Return the last N rows of the dataset */
  public static Map tail(Object ds, long nRows) {
    return (Map)call(tailFn, ds, nRows);
  }
  /** Return a random sampling of 5 rows without replacement of the data */
  public static Map sample(Object ds) {
    return (Map)call(sampleFn, ds);
  }
  /** Return a random sampling of N rows without replacement of the data */
  public static Map sample(Object ds, long nRows) {
    return (Map)call(sampleFn, ds, nRows);
  }
  /**
   * Return a random sampling of N rows of the data.
   *
   * Options:
   *
   * * `:replacement?` - Do sampling with replacement. Defaults to false.
   * * `:seed` - Either an integer or an implementation of java.util.Random.
   */
  public static Map sample(Object ds, long nRows, Map options) {
    return (Map)call(sampleFn, ds, nRows, options);
  }
  /** Randomly shuffle the dataset rows. */
  public static Map shuffle(Object ds) {
    return (Map)call(shuffleFn, ds);
  }
  /**
   * Randomly shuffle the dataset rows.
   *
   * Options:
   *
   * * `:seed` - Either an integer or an implementation of java.util.Random.
   */
  public static Map shuffle(Object ds, Map options) {
    return (Map)call(shuffleFn, ds, options);
  }
  /** Reverse the rows of the dataset */
  public static Map reverseRows(Object ds) {
    return (Map)call(reverseFn, ds);
  }

  /**
   * Map a function across 1 or more columns to produce a new column.  The new column is
   * serially scanned to detect datatype and its missing set.
   */
  public static Map columnMap(Object ds, Object resultCname, IFn mapFn, Object srcCnames) {
    return (Map)call(columnMapFn, ds, resultCname, mapFn, srcCnames);
  }
  /**
   * Map a function across the rows of the dataset with each row in map form.  Function must
   * return a new map for each row.
   */
  public static Map rowMap(Object ds, IFn mapFn) {
    return (Map)call(rowMapFn, ds, mapFn);
  }
  /**
   * Map a function across the rows of the dataset with each row in map form.  Function must
   * return either null or a sequence of maps and thus can produce many new rows for
   * each input row.  Function is called in a parallelized context.
   *
   * See options for pmapDs. Especially note `:max-batch-size` and `:result-type`. In
   * order to conserve memory it may be much more efficient to return a sequence of
   * datasets rather than one large dataset. If returning sequences of datasets
   * perhaps consider a transducing pathway across them or the
   * tech.v3.dataset.reductions namespace.
   */
  public static Object rowMapcat(Object ds, IFn mapFn, Object options) {
    return call(rowMapcatFn, ds, mapFn, options);
  }
  /**
   * Parallelize mapping a function from dataset->dataset across a dataset.  Function may
   * return null.  The original dataset is simply sliced into n-core results and
   * map-fn is called n-core times with the results either concatenated into a new dataset or
   * returned as an Iterable.
   *
   * @param mapFn a function from dataset->dataset although it may return null.
   *
   * Options:
   *
   * * `:max-batch-size` - Defaults to 64000.  This controls the size of each parallelized
   *   chunk.
   * * `:result-type` - Either `:as-seq` in which case the output of this function is a
   *   sequence of datasets or `:as-ds` in which case the output is a single dataset.  The
   *   default is `:as-ds`.
   */
  public static Object pmapDS(Object ds, IFn mapFn, Object options) {
    return call(pmapDsFn, ds, mapFn, options);
  }

  /**
   * Sort a dataset by first mapping `sortFn` over it and then sorting over the result.
   * `sortFn` is passed each row in map form and the return value is used to sort the
   * dataset.
   *
   * @param sortFn function taking a single argument which is the row-map and returns the value
   * to sort on.
   * @param compareFn Comparison operator or comparator.  Some examples are the Clojure '<' or
   * '>' operators - tech.v3.Clj.lessThanFn, tech.v3.Clj.greaterThanFn.  The clojure keywords
   * `:tech.numerics/<` and `:tech.numerics/>` can be used for somewhat higher performance
   * unboxed primitive comparisons or the Clojure function `compare` - tech.v3.Clj.compareFn -
   * which is similar to .compareTo except it works with null and the input must implement
   * Comparable.  Finally you can instantiate an instance of java.util.Comparator.
   *
   * Options:
   *
   *  * `:nan-strategy` - General missing strategy.  Options are `:first`, `:last`, and
   *  `:exception`.
   *  * `:parallel?` - Uses parallel quicksort when true and regular quicksort when false.
   */
  public static Map sortBy(Object ds, IFn sortFn, Object compareFn, Object options) {
    return (Map)call(sortByFn, ds, sortFn, compareFn, options);
  }
  /** Sort a dataset.  See documentation of 4-arity version.*/
  public static Map sortBy(Object ds, IFn sortFn, Object compareFn) {
    return (Map)call(sortByFn, ds, sortFn, compareFn, null);
  }
  /** Sort a dataset.  See documentation of 4-arity version.*/
  public static Map sortBy(Object ds, IFn sortFn) {
    return (Map)call(sortByFn, ds, sortFn);
  }

  /**
   * Sort a dataset by using the values from column `cname`.
   * to sort on.
   * @param compareFn Comparison operator or comparator.  Some examples are the Clojure '<' or
   * '>' operators - tech.v3.Clj.lessThanFn, tech.v3.Clj.greaterThanFn.  The clojure keywords
   * `:tech.numerics/<` and `:tech.numerics/>` can be used for somewhat higher performance
   * unboxed primitive comparisons or the Clojure function `compare` - tech.v3.Clj.compareFn -
   * which is similar to .compareTo except it works with null and the input must implement
   * Comparable.  Finally you can instantiate an instance of java.util.Comparator.
   *
   * Options:
   *
   *  * `:nan-strategy` - General missing strategy.  Options are `:first`, `:last`, and
   *  `:exception`.
   *  * `:parallel?` - Uses parallel quicksort when true and regular quicksort when false.
   */
  public static Map sortByColumn(Object ds, Object cname, Object compareFn, Object options) {
    return (Map)call(sortByColumnFn, ds, cname, compareFn, options);
  }
  /** Sort a dataset by a specific column.  See documentation on 4-arity version.*/
  public static Map sortByColumn(Object ds, Object cname, Object compareFn) {
    return (Map)call(sortByColumnFn, ds, cname, compareFn, null);
  }
  /** Sort a dataset by a specific column.  See documentation on 4-arity version.*/
  public static Map sortByColumn(Object ds, Object cname) {
    return (Map)call(sortByColumnFn, ds, cname);
  }
  /**
   * Filter a dataset.  Predicate gets passed all rows and must return a `truthy` values.
   */
  public static Map filter(Object ds, IFn predicate) {
    return (Map)call(filterFn, ds, predicate);
  }
  /**
   * Filter a dataset.  Predicate gets passed a values from column cname and must
   * return a `truthy` values.
   */
  public static Map filterColumn(Object ds, Object cname, IFn predicate) {
    return (Map)call(filterColumnFn, ds, cname, predicate);
  }
  /**
   * Group a dataset returning a Map of keys to dataset.
   *
   * @param groupFn Gets passed each row in map format and must return the desired key.
   *
   * @return a map of key to dataset.
   */
  public static Map groupBy(Object ds, IFn groupFn) {
    return (Map)call(groupByFn, ds, groupFn);
  }
  /**
   * Group a dataset by a specific column returning a Map of keys to dataset.
   *
   * @return a map of key to dataset.
   */
  public static Map groupByColumn(Object ds, Object cname) {
    return (Map)call(groupByColumnFn, ds, cname);
  }
  /**
   * Concatenate an Iterable of datasets into one dataset via copying data into one
   * dataset.  This generally results in higher performance than an in-place concatenation
   * with the exception of small (< 3) numbers of datasets. Null datasets will be silently
   * ignored.
   */
  public static Map concatCopying(Object datasets) {
    return (Map)call(applyFn, concatCopyingFn, datasets);
  }

  /**
   * Concatenate an Iterable of datasets into one dataset via creating virtual buffers that
   * index into the previous datasets. This generally results in lower performance than a
   * copying concatenation with the exception of small (< 3) numbers of datasets.  Null
   * datasets will be silently ignored.
   */
  public static Map concatInplace(Object datasets) {
    return (Map)call(applyFn, concatInplaceFn, datasets);
  }
  /**
   * Create a dataset with no duplicates by taking first of duplicate values.
   *
   * @param uniqueFn is passed a row and must return the uniqueness criteria.  A uniqueFn is
   * the identity function.
   */
  public static Map uniqueBy(Object ds, IFn uniqueFn) {
    return (Map)call(uniqueByFn, ds, uniqueFn);
  }
  /**
   * Make a dataset unique using a particular column as the uniqueness criteria and taking
   * the first value.
   */
  public static Map uniqueByColumn(Object ds, Object cname) {
    return (Map)call(uniqueByFn, ds, cname);
  }

  /**
   * Create a dataset of the descriptive statistics of the input dataset.  This works with
   * date-time columns, missing values, etc. and serves as very fast way to quickly get a feel
   * for a dataset.
   *
   * Options:
   *
   * * `:stat-names` - A set of desired stat names.  Possible statistic operations are:
   *  `[:col-name :datatype :n-valid :n-missing :min :quartile-1 :mean :mode :median
   *   :quartile-3 :max :standard-deviation :skew :n-values :values :histogram :first
   *   :last]`
   */
  public static Map descriptiveStats(Object ds, Object options) {
    return (Map)call(descriptiveStatsFn, ds, options);
  }
  /**
   * Create a dataset of the descriptive statistics of the input dataset.  This works with
   * date-time columns, missing values, etc. and serves as very fast way to quickly get a feel
   * for a dataset.
   *
   * Options:
   *
   * * `:stat-names` - A set of desired stat names.  Possible statistic operations are:
   *  `[:col-name :datatype :n-valid :n-missing :min :quartile-1 :mean :mode :median
   *   :quartile-3 :max :standard-deviation :skew :n-values :values :histogram :first
   *   :last]`
   */
  public static Map descriptiveStats(Object ds) {
    return (Map)call(descriptiveStatsFn, ds);
  }
  /**
   * Perform a join operation between two datasets.
   *
   * Options:
   *
   * * `:on` - column name or list of columns names.  Names must be found in both datasets.
   * * `:left-on` - Column name or list of column names
   * * `:right-on` - Column name or list of column names
   * * `:how` - `:left`, `:right` `:inner`, `:outer`, `:cross`.  If `:cross`, then it is
   *    an error to provide `:on`, `:left-on`, `:right-on`. Defaults to `:inner`.
   *
   * Examples:
   *
   *```java
   *Map dsa = makeDataset(hashmap("a", vector("a", "b", "b", "a", "c"),
   *				  "b", range(5),
   *				  "c", range(5)));
   *println(dsa);
   * //_unnamed [5 3]:
   *
   * //| a | b | c |
   * //|---|--:|--:|
   * //| a | 0 | 0 |
   * //| b | 1 | 1 |
   * //| b | 2 | 2 |
   * //| a | 3 | 3 |
   * //| c | 4 | 4 |
   *
   *
   *Map dsb = makeDataset(hashmap("a", vector("a", "b", "a", "b", "d"),
   *				  "b", range(5),
   *				  "c", range(6,11)));
   *println(dsb);
   * //_unnamed [5 3]:
   *
   * //| a | b |  c |
   * //|---|--:|---:|
   * //| a | 0 |  6 |
   * //| b | 1 |  7 |
   * //| a | 2 |  8 |
   * //| b | 3 |  9 |
   * //| d | 4 | 10 |
   *
   * //Join on the columns a,b.  Default join mode is inner
   * println(join(dsa, dsb, hashmap(kw("on"), vector("a", "b"))));
   * //inner-join [2 4]:
   *
   * //| a | b | c | right.c |
   * //|---|--:|--:|--------:|
   * //| a | 0 | 0 |       6 |
   * //| b | 1 | 1 |       7 |
   *
   *
   * //Outer join on same columns
   *println(join(dsa, dsb, hashmap(kw("on"), vector("a", "b"),
   *				   kw("how"), kw("outer"))));
   * //outer-join [8 4]:
   *
   * //| a | b | c | right.c |
   * //|---|--:|--:|--------:|
   * //| a | 0 | 0 |       6 |
   * //| b | 1 | 1 |       7 |
   * //| b | 2 | 2 |         |
   * //| a | 3 | 3 |         |
   * //| c | 4 | 4 |         |
   * //| a | 2 |   |       8 |
   * //| b | 3 |   |       9 |
   * //| d | 4 |   |      10 |
   *```
   */
  public static Map join(Map leftDs, Map rightDs, Map options) {
    return (Map)pdMergeFn.invoke(leftDs, rightDs, options);
  }

  /**
   * Perform a left join but join on nearest value as opposed to matching value.
   * Both datasets must be sorted by the join column and the join column itself
   * must be either a datetime column or a numeric column.  When the join column
   * is a datetime column the join happens in millisecond space.
   *
   * Options:
   *
   * * `:asof-op` - One of the keywords `[:< :<= :nearest :>= :>]`.  Defaults to `:<=`.
   *
   * Examples:
   *
   *```java
   *println(head(googPrices, 200));
   * //GOOG [68 3]:
   * //| symbol |       date |  price |
   * //|--------|------------|-------:|
   * //|   GOOG | 2004-08-01 | 102.37 |
   * //|   GOOG | 2004-09-01 | 129.60 |
   * //|   GOOG | 2005-03-01 | 180.51 |
   * //|   GOOG | 2004-11-01 | 181.98 |
   * //|   GOOG | 2005-02-01 | 187.99 |
   * //|   GOOG | 2004-10-01 | 190.64 |
   * //|   GOOG | 2004-12-01 | 192.79 |
   * //|   GOOG | 2005-01-01 | 195.62 |
   * //|   GOOG | 2005-04-01 | 220.00 |
   * //|   GOOG | 2005-05-01 | 277.27 |
   * //|   GOOG | 2005-08-01 | 286.00 |
   * //|   GOOG | 2005-07-01 | 287.76 |
   * //|   GOOG | 2008-11-01 | 292.96 |
   * //|   GOOG | 2005-06-01 | 294.15 |
   * //|   GOOG | 2008-12-01 | 307.65 |
   * //|   GOOG | 2005-09-01 | 316.46 |
   * //|   GOOG | 2009-02-01 | 337.99 |
   * //|   GOOG | 2009-01-01 | 338.53 |
   * //|   GOOG | 2009-03-01 | 348.06 |
   * //|   GOOG | 2008-10-01 | 359.36 |
   * //|   GOOG | 2006-02-01 | 362.62 |
   * //|   GOOG | 2006-05-01 | 371.82 |
   * //|   GOOG | 2005-10-01 | 372.14 |
   * //|   GOOG | 2006-08-01 | 378.53 |
   * //|   GOOG | 2006-07-01 | 386.60 |
   * //|   GOOG | 2006-03-01 | 390.00 |
   * //|   GOOG | 2009-04-01 | 395.97 |
   * //|   GOOG | 2008-09-01 | 400.52 |
   * //|   GOOG | 2006-09-01 | 401.90 |
   * //|   GOOG | 2005-11-01 | 404.91 |
   * //|   GOOG | 2005-12-01 | 414.86 |
   * //|   GOOG | 2009-05-01 | 417.23 |
   * //|   GOOG | 2006-04-01 | 417.94 |
   * //|   GOOG | 2006-06-01 | 419.33 |
   * //|   GOOG | 2009-06-01 | 421.59 |
   * //|   GOOG | 2006-01-01 | 432.66 |
   * //|   GOOG | 2008-03-01 | 440.47 |
   * //|   GOOG | 2009-07-01 | 443.05 |
   * //|   GOOG | 2007-02-01 | 449.45 |
   * //|   GOOG | 2007-03-01 | 458.16 |
   * //|   GOOG | 2006-12-01 | 460.48 |
   * //|   GOOG | 2009-08-01 | 461.67 |
   * //|   GOOG | 2008-08-01 | 463.29 |
   * //|   GOOG | 2008-02-01 | 471.18 |
   * //|   GOOG | 2007-04-01 | 471.38 |
   * //|   GOOG | 2008-07-01 | 473.75 |
   * //|   GOOG | 2006-10-01 | 476.39 |
   * //|   GOOG | 2006-11-01 | 484.81 |
   * //|   GOOG | 2009-09-01 | 495.85 |
   * //|   GOOG | 2007-05-01 | 497.91 |
   * //|   GOOG | 2007-01-01 | 501.50 |
   * //|   GOOG | 2007-07-01 | 510.00 |
   * //|   GOOG | 2007-08-01 | 515.25 |
   * //|   GOOG | 2007-06-01 | 522.70 |
   * //|   GOOG | 2008-06-01 | 526.42 |
   * //|   GOOG | 2010-02-01 | 526.80 |
   * //|   GOOG | 2010-01-01 | 529.94 |
   * //|   GOOG | 2009-10-01 | 536.12 |
   * //|   GOOG | 2010-03-01 | 560.19 |
   * //|   GOOG | 2008-01-01 | 564.30 |
   * //|   GOOG | 2007-09-01 | 567.27 |
   * //|   GOOG | 2008-04-01 | 574.29 |
   * //|   GOOG | 2009-11-01 | 583.00 |
   * //|   GOOG | 2008-05-01 | 585.80 |
   * //|   GOOG | 2009-12-01 | 619.98 |
   * //|   GOOG | 2007-12-01 | 691.48 |
   * //|   GOOG | 2007-11-01 | 693.00 |
   * //|   GOOG | 2007-10-01 | 707.00 |

   *Map targetPrices = makeDataset(hashmap("price", new Double[] { 200.0, 300.0, 400.0 }));

   *println(leftJoinAsof("price", targetPrices, googPrices, hashmap(kw("asof-op"), kw("<="))));
   * //asof-<= [3 4]:
   * //| price | symbol |       date | GOOG.price |
   * //|------:|--------|------------|-----------:|
   * //| 200.0 |   GOOG | 2005-04-01 |     220.00 |
   * //| 300.0 |   GOOG | 2008-12-01 |     307.65 |
   * //| 400.0 |   GOOG | 2008-09-01 |     400.52 |
   * println(leftJoinAsof("price", targetPrices, googPrices, hashmap(kw("asof-op"), kw(">"))));
   * //asof-> [3 4]:
   * //| price | symbol |       date | GOOG.price |
   * //|------:|--------|------------|-----------:|
   * //| 200.0 |   GOOG | 2005-01-01 |     195.62 |
   * //| 300.0 |   GOOG | 2005-06-01 |     294.15 |
   * //| 400.0 |   GOOG | 2009-04-01 |     395.97 |
   *```
   */
  public static Map leftJoinAsof(Object colname, Map lhs, Map rhs, Object options) {
    return (Map)joinAsof.invoke(colname, lhs, rhs, options);
  }
  public static Map leftJoinAsof(Object colname, Map lhs, Map rhs) {
    return (Map)joinAsof.invoke(colname, lhs, rhs);
  }

  /**
   * Convert a dataset to a neanderthal 2D matrix such that the columns of the dataset
   * become the columns of the matrix.  This function dynamically loads the neanderthal
   * MKL bindings so there may be some pause when first called.  If you would like to have
   * the pause somewhere else call `require("tech.v3.dataset.neanderthal"); at some
   * previous point of the program.  You must have an update-to-date version of
   * neanderthal in your classpath such as `[uncomplicate/neanderthal "0.43.3"]`.
   *
   * See the [neanderthal documentation](https://neanderthal.uncomplicate.org/)`
   *
   * @param layout One of `:column` or `:row`.
   * @param datatype One of `:float32` or `:float64`.
   *
   * Note that you can get a tech tensor (tech.v3.datatype.NDBuffer) from a neanderthal
   * matrix using `tech.v3.DType.asTensor()`.
   *
   */
  public static Object toNeanderthal(Object ds, Keyword layout, Keyword datatype) {
    return call(deref(toNeanderthalDelay), ds, layout, datatype);
  }
  /**
   * Convert a dataset to a neanderthal 2D matrix such that the columns of the dataset
   * become the columns of the matrix.  See documentation for 4-arity version of
   * function.  This function creates a column-major float64 (double) matrix.
   */
  public static Object toNeanderthal(Object ds) {
    return call(deref(toNeanderthalDelay), ds);
  }
  /**
   * Convert a neanderthal matrix to a dataset such that the columns of the matrix
   * become the columns of the dataset.  Column names are the indexes of the columns.
   */
  public static Map neanderthalToDataset(Object denseMat) {
    return (Map)call(deref(neanderthalToDatasetDelay), denseMat);
  }
  /**
   * Convert a dataset to a jvm-heap based 2D tensor such that the columns of the
   * dataset become the columns of the tensor.
   *
   * @param datatype Any numeric datatype - `:int8`, `:uint8`, `:float32`, `:float64`, etc.
   */
  public static NDBuffer toTensor(Object ds, Keyword datatype) {
    return (NDBuffer)call(deref(toTensorDelay), ds, datatype);
  }
  /**
   *  Convert a dataset to a jvm-heap based 2D double (float64) tensor.
   */
  public static NDBuffer toTensor(Object ds) {
    return (NDBuffer)call(deref(toTensorDelay), ds);
  }
  /**
   * Convert a tensor to a dataset such that the columns of the tensor
   * become the columns of the dataset named after their index.
   */
  public static Map tensorToDataset(Object tens) {
    return (Map)call(deref(tensorToDatasetDelay), tens);
  }
  /**
   * Write a dataset to disc as csv, tsv, csv.gz, tsv.gz or nippy.
   *
   * Reading/writing to parquet or arrow is accessible via separate clasess
   */
  public static void writeDataset(Object ds, String path, Object options) {
    call(writeFn, ds, path, options);
  }
  /**
   * Write a dataset to disc as csv, tsv, csv.gz, tsv.gz or nippy.
   */
  public static void writeDataset(Object ds, String path) {
    writeDataset(ds, path, null);
  }
}
