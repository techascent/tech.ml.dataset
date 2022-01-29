package tech.v3.dataset;


import static tech.v3.Clj.*;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Map;


/**
 * Functions related to training and evaluating ML models.  The functions are grouped into
 * a few groups.
 *
 * For the purpose of this system, categorical data means a column of data that is not numeric.
 * it could be strings, keywords, or arbitrary objects.
 *
 * PCA *for now* requires either opencpp's blas library or smile's MKL bindings.  Expect this
 * requirement to change to solely requiring neanderthal in the near future.
 *
 * Minimal example extra dependencies for PCA:
 *
 *```console
 * [org.bytedeco/openblas "0.3.10-1.5.4"]
 * [org.bytedeco/openblas-platform "0.3.10-1.5.4"]
 *```
 *
 * It is also important to note that you can serialize the fit results to nippy automatically
 * as included in dtype-next are extensions to nippy that work with tensors.
 */
public class Modelling {
  private Modelling(){}

  static final IFn fitCatFn = requiringResolve("tech.v3.dataset.categorical", "fit-categorical-map");
  static final IFn transCatFn = requiringResolve("tech.v3.dataset.categorical", "transform-categorical-map");
  static final IFn invCatFn = requiringResolve("tech.v3.dataset.categorical", "invert-categorical-map");
  static final IFn fitOneHotFn = requiringResolve("tech.v3.dataset.categorical", "fit-one-hot");
  static final IFn transOneHotFn = requiringResolve("tech.v3.dataset.categorical", "transform-one-hot");
  static final IFn invOneHotFn = requiringResolve("tech.v3.dataset.categorical", "invert-one-hot");

  static final IFn corrTableFn = requiringResolve("tech.v3.dataset.math", "correlation-table");
  static final IFn fillRangeReplaceFn = requiringResolve("tech.v3.dataset.math", "fill-range-replace");
  static final IFn fitPCAFn = requiringResolve("tech.v3.dataset.math", "fit-pca");
  static final IFn fitStdScaleFn = requiringResolve("tech.v3.dataset.math", "fit-std-scale");
  static final IFn fitMinMaxFn = requiringResolve("tech.v3.dataset.math", "fit-minmax");
  static final IFn transPCAFn = requiringResolve("tech.v3.dataset.math", "transform-pca");
  static final IFn transStdScaleFn = requiringResolve("tech.v3.dataset.math", "transform-std-scale");
  static final IFn transMinMaxFn = requiringResolve("tech.v3.dataset.math", "transform-minmax");
  static final IFn interpolateLOESSFn = requiringResolve("tech.v3.dataset.math", "interpolate-loess");

  static final IFn kFoldFn = requiringResolve("tech.v3.dataset.modelling", "k-fold-datasets");
  static final IFn trainTestFn = requiringResolve("tech.v3.dataset.modelling", "train-test-split");
  static final IFn setInfTargetFn = requiringResolve("tech.v3.dataset.modelling", "set-inference-target");
  static final IFn labelsFn = requiringResolve("tech.v3.dataset.modelling", "labels");
  static final IFn probDistToLabel = requiringResolve("tech.v3.dataset.modelling", "probability-distributions->label-column");
  static final IFn infTargetLabelMap = requiringResolve("tech.v3.dataset.modelling", "inference-target-label-map");


  /**
   * Fit an object->integer transform that takes each value and assigned an integer to it.  The
   * returned value can be used in transformCategorical to transform the dataset.
   *
   * Options:
   *
   * * `:table-args` - Either a sequence of vectors [col-val, idx] or a sorted sequence
   *   of column values where integers will be assigned as per the sorted sequence.  Any values
   *   found outside the the specified values will be auto-mapped to the next largest integer.
   * * `:res-dtype` - Datatype of result column.  Defaults to `:float64`.
   */
  public static Map fitCategorical(Object ds, Object cname, Object options) {
    return (Map)fitCatFn.invoke(ds, cname, options);
  }
  /**
   * Fit an object->integer transformation.  Integers will be assigned in random order.  For
   * more control over the transform see the 3-arity version of the function.
   */
  public static Map fitCategorical(Object ds, Object cname) {
    return (Map)fitCatFn.invoke(ds, cname);
  }
  /**
   * Apply an object->integer transformation with data obtained from fitCategorical.
   */
  public static Map transformCategorical(Object ds, Object catFitData) {
    return (Map)transCatFn.invoke(ds, catFitData);
  }
  /**
   * Reverse a previously transformed categorical mapping.
   */
  public static Map invertCategorical(Object ds, Object catFitData) {
    return (Map)invCatFn.invoke(ds, catFitData);
  }
  /**
   * Fit a transformation from a single column of categorical values to a `one-hot` encoded
   * group of columns.
   * .
   *
   * Options:
   *
   * * `:table-args` - Either a sequence of vectors [col-val, idx] or a sorted sequence
   *   of column values where integers will be assigned as per the sorted sequence.  Any values
   *   found outside the the specified values will be auto-mapped to the next largest integer.
   * * `:res-dtype` - Datatype of result column.  Defaults to `:float64`.
   *
   */
  public static Map fitOneHot(Object ds, Object cname, Object options) {
    return (Map)fitOneHotFn.invoke(ds, cname, options);
  }
  /**
   * Fit a mapping from a categorical column to a group of one-hot encoded columns.
   */
  public static Map fitOneHot(Object ds, Object cname) {
    return (Map)fitOneHotFn.invoke(ds, cname);
  }
  /**
   * Transform a dataset using a fitted one-hot mapping.
   */
  public static Map transformOneHot(Object ds, Object fitData) {
    return (Map)transOneHotFn.invoke(ds, fitData);
  }
  /**
   * Reverse a previously transformed one-hot mapping.
   */
  public static Map invertOneHot(Object ds, Object fitData) {
    return (Map)invOneHotFn.invoke(ds, fitData);
  }
  /**
   * Return a map of column to inversely sorted from greatest to least sequence of tuples of
   * column name, coefficient.
   *
   * Options:
   *
   * * `:correlation-type` One of `:pearson`, `:spearman`, or `:kendall`.  Defaults to
   *   `:pearson`.
   */
  public static Map correlationTable(Object ds, Object options) {
    return (Map)corrTableFn.invoke(ds, options);
  }
  /**
   * Return a map of column to inversely sorted from greatest to least sequence of tuples of
   * column name, pearson correlation coefficient.
   */
  public static Map correlationTable(Object ds) {
    return (Map)corrTableFn.invoke(ds);
  }
  /**
   * Expand a dataset ensuring that the difference between two successive values is less than
   * `max-span`.
   *
   * @param maxSpan The minimal span value.  For datetime types this is interpreted in
   * millisecond or epoch-millisecond space.
   * @param missingStrategy Same missing strategy types from `TMD.replaceMissing`.
   */
  public static Map fillRangeReplace(Object ds, Object cname, double maxSpan, Object missingStrategy) {
    Keyword strat;
    Object value = null;
    if (isVector(missingStrategy)) {
	strat = (Keyword)call(missingStrategy, 0);
	value = call(missingStrategy, 1);
      } else {
      strat = (Keyword)missingStrategy;
    }
    return (Map) fillRangeReplaceFn.invoke(ds, cname, maxSpan, strat, value);
  }
  /**
   * Fit a PCA transformation on a dataset.
   *
   * @return map of `{:means, :eigenvalues, :eigenvectors}`.
   *
   * Options:
   *
   * * `:method` - either `:svd` or `cov`.  Use either SVD transformation or covariance-matrix
   *   base PCA.  `:cov` method is somewhat slower but returns accurate variances.
   * * `:variance-amount` - Keep columns until variance is just less than variance-amount.
   *   Defaults to 0.95.
   * * `:n-components` - Return a fixed number of components.  Overrides `:variance-amount`
   *   an returns a fixed number of components.
   * * `:covariance-bias` - When using `:cov` divide by `n-rows` if true and `n-rows - 1` if
   *   false.  Defaults to false.
   */
  public static Object fitPCA(Object ds, Object options) {
    return fitPCAFn.invoke(ds, options);
  }
  /**
   * Fit a PCA transformation onto a dataset keeping 95% of the variance.  See documentation
   * for 2-arity form.
   */
  public static Object fitPCA(Object ds) {
    return fitPCAFn.invoke(ds);
  }
  /**
   * Transform a dataset by the PCA fit data.
   */
  public static Map transformPCA(Object ds, Object fitData) {
    return (Map)transPCAFn.invoke(ds, fitData);
  }
  /**
   * Calculate per-column mean, stddev.
   *
   * Options:
   *
   * * `:mean?` - Produce per-column means.  Defaults to true.
   * * `:stddev?` - Produce per-column standard deviation.  Defaults to true.
   */
  public static Object fitStdScale(Object ds) {
    return fitStdScaleFn.invoke(ds);
  }
  /**
   * Transform dataset to mean of zero and a standard deviation of 1.
   */
  public static Map transformStdScale(Object ds, Object fitData) {
    return (Map)transStdScaleFn.invoke(ds, fitData);
  }
  /**
   * Fit a bias and scale the dataset that transforms each colum to a target min-max
   * value.
   *
   * Options:
   *
   * * `:min` - Target minimum value.  Defaults it -0.5.
   * * `:max` - Target maximum value.  Defaults to 0.5.
   */
  public static Object fitMinMax(Object ds, Object options) {
    return fitMinMaxFn.invoke(ds, options);
  }
  /**
   * Fit a minmax transformation that will transform each column to a minimum of -0.5 and
   * a maximum of 0.5.
   */
  public static Object fitMinMax(Object ds) {
    return fitMinMaxFn.invoke(ds);
  }
  /**
   * Transform a dataset using a previously fit minimax transformation.
   */
  public static Map transformMinMax(Object ds, Object fitData) {
    return (Map)transMinMaxFn.invoke(ds, fitData);
  }
  /**
   * Map a LOESS-interpolation transformation onto a dataset.  This can be used
   * to, among other things, smooth out a column before graphing.  For the meaning
   * of the options, see documentation on the
   * org.apache.commons.math3.analysis.interpolationLoessInterpolator.
   *
   * Option defaults have been chosen to map somewhat closely to the R defaults.
   *
   * Options:
   *
   * * `:bandwidth` - Defaults to 0.75.
   * * `:iterations` - Defaults to 4.
   * * `:accuracy` - Defaults to LoessInterpolator/DEFAULT_ACCURACY.
   * * `:result-name` - Result column name.  Defaults to `yColname.toString +  "-loess"`.
   */
  public static Map interpolateLOESS(Object ds, Object xColname, Object yColname, Object options) {
    return (Map)interpolateLOESSFn.invoke(ds, xColname, yColname, options);
  }
  /**
   * Perform a LOESS interpolation using the default parameters.  For options see 4-arity
   * form of function.
   */
  public static Map interpolateLOESS(Object ds, Object xColname, Object yColname) {
    return (Map)interpolateLOESSFn.invoke(ds, xColname, yColname);
  }
  /**
   * Produce 2*k datasets from 1 dataset using k-fold algorithm.
   * Returns a k maps of the form `{:test-ds :train-ds}.
   *
   * Options:
   *
   * * `:randomize-dataset?` - When true, shuffle dataset.  Defaults to true.
   * * `:seed` - When randomizing dataset, seed may be either an integer or an implementation
   *   of `java.util.Random`.
   */
  public static Iterable kFold(Object ds, long k, Object options) {
    return (Iterable)kFoldFn.invoke(ds, k, options);
  }
  /**
   * Return k maps of the form `{:test-ds :train-ds}`.  For options see 3-arity form.
   */
  public static Iterable kFold(Object ds, long k) {
    return (Iterable)kFoldFn.invoke(ds, k);
  }
  /**
   * Split the dataset returning a map of `{:train-ds :test-ds}`.
   *
   * Options:
   *
   * * `:randomize-dataset?` - Defaults to true.
   * * `:seed` - When provided must be an integer or an implementation `java.util.Random`.
   * * `:train-fraction` - Fraction of dataset to use as training set.  Defaults to 0.7.
   */
  public static Map trainTestSplit(Object ds, Object options) {
    return (Map)trainTestFn.invoke(ds, options);
  }
  /**
   * Randomize then split dataset using 70% of the data for training and the rest for testing.
   */
  public static Map trainTestSplit(Object ds) {
    return (Map)trainTestFn.invoke(ds);
  }
  /**
   * Set a column in the dataset as the inference target.  This information is stored in the
   * column metadata.  This function is short form for:
   *
   *```java
   *  Object col = column(ds, cname);
   *  return assoc(ds, cname, varyMeta(col, assocFn, kw("inference-target?"), true));
   *```
   */
  public static Map setInferenceTarget(Object ds, Object cname) {
    return (Map)setInfTargetFn.invoke(ds, cname);
  }
  /**
   * Find the inference column.  If column was the result of a categorical mapping, reverse
   * that mapping.  Return data in a form that can be efficiently converted to a Buffer.
   */
  public static Object labels(Object ds) {
    return labelsFn.invoke(ds);
  }
  /**
   * Given a dataset where the column names are labels and the each row is a probabilitly
   * distribution across the labels, produce a Buffer of labels taking the highest probability
   * for each row to choose the label.
   */
  public static Object probabilityDistributionToLabels(Object ds) {
    return probDistToLabel.invoke(ds);
  }
  /**
   * Return a map of val->idx for the inference target.
   */
  public static Map inferenceTargetLabelMap(Object ds) {
    return (Map)infTargetLabelMap.invoke(ds);
  }
}
