package tech.v3.dataset;


import static tech.v3.Clj.*;
import clojure.lang.IFn;
import java.util.Map;


/**
 * High speed grouping aggregations based on sequences of datasets.
 */
public class Reductions {
  private Reductions(){}

  static final IFn reducerFn = requiringResolve("tech.v3.dataset.reductions", "reducer");
  static final IFn sumFn = requiringResolve("tech.v3.dataset.reductions", "sum");
  static final IFn meanFn = requiringResolve("tech.v3.dataset.reductions", "mean");
  static final IFn rowCountFn = requiringResolve("tech.v3.dataset.reductions", "row-count");
  static final IFn distinctFn = requiringResolve("tech.v3.dataset.reductions", "distinct");
  static final IFn countDistinctFn = requiringResolve("tech.v3.dataset.reductions", "count-distinct");
  static final IFn reservoirDsFn = requiringResolve("tech.v3.dataset.reductions", "reservoir-dataset");
  static final IFn reservoirDescStatFn = requiringResolve("tech.v3.dataset.reductions", "reservoir-desc-stat");
  static final IFn probSetCardFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-set-cardinality");
  static final IFn probQuantilesFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-quantiles");
  static final IFn probQuantileFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-quantile");
  static final IFn probMedianFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-median");
  static final IFn probCdfsFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-cdfs");
  static final IFn probPmfsFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-pmfs");
  static final IFn probIQRangeFn = requiringResolve("tech.v3.dataset.reductions.apache-data-sketch", "prob-interquartile-range");
  static final IFn groupByColumnAggFn = requiringResolve("tech.v3.dataset.reductions", "group-by-columns-agg");


  /**
   * Group a sequence of datasets by column or columns an in the process perform an aggregation.
   * The resulting dataset will have one row per grouped key.  Columns used as keys will always
   * be represented in the result.
   *
   * @param dsSeq Sequence of datasets such as produced by rowMapcat, dsPmap, or loading many
   * files.
   * @param colname Either a single column name or a vector of column names.  These will be the
   * grouping keys.
   * @param aggMap Map of result colname to reducer.  Various reducers are provided or you can
   * build your own via the `reducer` function.
   * @param options Options map. Described below.  May be null.
   *
   * Options:
   *
   * * `:map-initial-capacity` - initial hashmap capacity.  Resizing hash-maps is expensive
   *    so we would like to set this to something reasonable.  Defaults to 100000.
   * *  `:index-filter` - A function that given a dataset produces a function from long index
   *    to boolean.  Only indexes for which the index-filter returns true will be added to the
   *    aggregation.  For very large datasets, this is a bit faster than using filter before
   *    the aggregation.
   */
  public static Map groupByColumnsAgg(Iterable dsSeq, Object colname, Map aggMap, Map options) {
    return (Map)groupByColumnAggFn.invoke(colname, aggMap, options, dsSeq);
  }

  /**
   * Create a custom reducer.  perElemFn is passed the last return value as the first argument
   * followed by a value from each column as additional arguments.  It must always return the
   * current context.
   *
   * This is a easy way to instantiate tech.v3.datatype.IndexReduction so if you really need
   * the best possible performance you need to implement three methods of IndexReduction:
   *
   * * `prepareBatch` - Passed each dataset before processing.  Return value becomes first
   *   argument to `reduceIndex`.
   * * `reduceIndex` - Passed batchCtx, valCtx, and rowIdx.  Must return an updated or
   *   new valCtx.
   * * `finalize` - Passed valCtx and must return the final per-row value expected in
   *   result dataset.  The default is just to return valCtx.
   *
   * For `groupByColumnAgg` you do not need to worry about reduceReductions - there is no
   * merge step.
   *
   * @param colname One or more column names.  If multiple column names are specified then
   * perElemFn will need to take additional arguments.
   * @param perElemFn A function that takes the previous context along with the current row's
   * column values and returns a new context.
   * @param finalizeFn Optional function that performs a final calculation taking a context
   * and returning a value.
   */
  public static Object reducer(Object colname, IFn perElemFn, IFn finalizeFn) {
    return reducerFn.invoke(colname, perElemFn, finalizeFn);
  }
  /**
   * Create a custom reducer.  `perElemFn` is passed the last return value as the first
   * argument followed by a value from each column as additional arguments.  It must always
   * return the current context.
   *
   * This is a easy way to instantiate tech.v3.datatype.IndexReduction so if you really need
   * the best possible performance you need to implement three methods of IndexReduction:
   *
   * * `prepareBatch` - Passed each dataset before processing.  Return value becomes first
   *   argument to `reduceIndex`.
   * * `reduceIndex` - Passed batchCtx, valCtx, and rowIdx.  Must return valCtx.
   * * `finalize` - Passed valCtx and must return the final per-row value expected in
   *   result dataset.
   *
   * For `groupByColumnAgg` you do not need to worry about reduceReductions - there is no
   * merge step.
   *
   * @param colname One or more column names.  If multiple column names are specified then
   * perElemFn will need to take additional arguments.
   * @param perElemFn A function that takes the previous context along with the current row's
   * column values and returns a new context.
   */
  public static Object reducer(Object colname, IFn perElemFn) {
    return reducerFn.invoke(colname, perElemFn);
  }
  /**
   * Returns a summation reducer that sums an individual source column.
   */
  public static Object sum(Object colname) {
    return sumFn.invoke(colname);
  }
  /**
   * Returns a mean reducer that produces a mean value of an individual source column.
   */
  public static Object mean(Object colname) {
    return meanFn.invoke(colname);
  }
  /**
   * Returns a rowCount reducer that returns the number of source rows aggregated.
   */
  public static Object rowCount(Object colname) {
    return rowCountFn.invoke(colname);
  }
  /**
   * Returns a distinct reducer produces a set of distinct values.
   */
  public static Object distinct(Object colname) {
    return distinctFn.invoke(colname);
  }
  /**
   * Returns a distinct reducer that produces a roaringbitmap of distinct values.  This is many
   * times faster than the distinct reducer if your data fits into unsigned int32 space.
   */
  public static Object distinctUInt32(Object colname) {
    return distinctFn.invoke(colname);
  }
  /**
   * Returns a distinct reducer returns the number of distinct elements.
   */
  public static Object setCardinality(Object colname) {
    return countDistinctFn.invoke(colname);
  }
  /**
   * Returns a distinct reducer that expects unsigned integer values and returns the number
   * of distinct elements.  This is many times faster than the countDistinct function.
   */
  public static Object setCardinalityUint32(Object colname) {
    return countDistinctFn.invoke(colname, kw("int32"));
  }
  /**
   * Return a reducer that produces a probabilistically sampled dataset of at most nRows len.
   */
  public static Object reservoirDataset(long nRows) {
    return reservoirDsFn.invoke(nRows);
  }
  /**
   * Return a reducer which will probabilistically sample the source column producing at most
   * nRows and then call descriptiveStatistics on it with statName.
   *
   * Stat names are described in tech.v3.datatype.Statistics.descriptiveStats.
   */
  public static Object reservoirStats(Object colname, long nRows, Object statName) {
    return reservoirDescStatFn.invoke(colname, nRows, statName);
  }
  /**
   * Calculate a probabilistic set cardinality for a given column based on one of three
   * algorithms.
   *
   * Options:
   *
   * * `:datatype` - One of `#{:float64 :string}`.  Unspecified defaults to `:float64`.
   * * `:algorithm` - defaults to :hyper-log-log.  Further algorithm-specific options
   *   may be included in the options map.
   *
   * Algorithm specific options:
   *
   * * [:hyper-log-log](https://datasketches.apache.org/docs/HLL/HLL.html)
   *     * `:hll-lgk` - defaults to 12, this is log-base2 of k, so k = 4096. lgK can be
   *        from 4 to 21.
   *     * `:hll-type` - One of #{4,6,8}, defaults to 8.  The HLL_4, HLL_6 and HLL_8
   *        represent different levels of compression of the final HLL array where the
   *        4, 6 and 8 refer to the number of bits each bucket of the HLL array is
   *        compressed down to. The HLL_4 is the most compressed but generally slightly
   *        slower than the other two, especially during union operations.
   * * [:theta](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
   * * [:cpc](https://datasketches.apache.org/docs/CPC/CPC.html)
   *     * `:cpc-lgk` - Defaults to 10.
   */
  public static Object probSetCardinality(Object colname, Map options) {
    return probSetCardFn.invoke(colname, options);
  }
  /**
   * Probabilistic quantile estimation - see [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
   *
   * @param quantiles Sequence of quantiles.
   * @param k Defaults to 128. This produces a normalized rank error of about 1.7%"
   */
  public static Object probQuantiles(Object colname, Object quantiles, long k) {
    return probQuantilesFn.invoke(colname, quantiles, k);
  }
  /**
   * Probabilistic quantile estimation using default k of 128.
   * See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
   *
   * @param quantiles Sequence of numbers from 0-1.
   */
  public static Object probQuantiles(Object colname, Object quantiles) {
    return probQuantilesFn.invoke(colname, quantiles);
  }

  /**
   * Probabilistic quantile estimation using default k of 128.
   * See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
   * Multiple quantiles will be merged into a single quantile calculation so it may be more
   * convenient to use this function to produce multiple quantiles mapped to several result
   * columns as opposed to ending up with a single column of maps of quantile to value.
   *
   * @param quantile Number from 0-1.
   * @param k Defaults to 128. This produces a normalized rank error of about 1.7%
   */
  public static Object probQuantile(Object colname, double quantile, long k) {
    return probQuantileFn.invoke(colname, quantile);
  }
  /**
   * Probabilistic quantile estimation using default k of 128.
   * See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
   * Multiple quantiles will be merged into a single quantile calculation so it may be more
   * convenient to use this function to produce multiple quantiles mapped to several result
   * columns as opposed to ending up with a single column of maps of quantile to value.
   *
   * @param quantile Number from 0-1.
   */
  public static Object probQuantile(Object colname, double quantile) {
    return probQuantileFn.invoke(colname, quantile);
  }
  /**
   * Probabilistic median.  See documentation for probQuantiles.
   */
  public static Object probMedian(Object colname, long k) {
    return probMedianFn.invoke(colname, k);
  }
  /**
   * Probabilistic median with default K of 128.  See documentation for probQuantiles.
   */
  public static Object probMedian(Object colname) {
    return probMedianFn.invoke(colname);
  }
  /**
   * Probabilistic interquartile range.  See documentation for probQuantile.
   */
  public static Object probInterquartileRange(Object colname, long k) {
    return probIQRangeFn.invoke(colname, k);
  }
  /**
   * Probabilistic interquartile range.  See documentation for probQuantile.
   */
  public static Object probInterquartileRange(Object colname) {
    return probIQRangeFn.invoke(colname);
  }
  /**
   * Probabilistic CDF calculation, one for each double cdf passed in.
   * See documentation for progQuantiles.
   */
  public static Object probCDFS(Object colname, Object cdfs, long k) {
    return probCdfsFn.invoke(colname, cdfs, k);
  }
  /**
   * Probabilistic CDF calculation, one for each double cdf passed in.
   * See documentation for probQuantiles.
   */
  public static Object probCDFS(Object colname, Object cdfs) {
    return probCdfsFn.invoke(colname, cdfs);
  }
  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values). See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
   * See documentation for probQuantiles.
   *
   */
  public static Object probPMFS(Object colname, Object pmfs, long k) {
    return probPmfsFn.invoke(colname, pmfs, k);
  }
  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of splitPoints (values). See [DoublesSketch](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html).
   * See documentation for probQuantiles.
   *
   */
  public static Object probPMFS(Object colname, Object pmfs) {
    return probPmfsFn.invoke(colname, pmfs);
  }

}
