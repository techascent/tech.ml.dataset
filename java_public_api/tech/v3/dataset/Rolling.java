package tech.v3.dataset;

import static tech.v3.Clj.*;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Map;

/**
 * Fixed and variable length rolling windows.  For variable rolling windows the dataset
 * must already be sorted by the target column.  Datetime support is provided in terms of
 * provide specific units in which to perform the rolling operation such as the keyword
 * `:days`.
 *
 */
public class Rolling {

  private Rolling(){}

  static final IFn meanFn = requiringResolve("tech.v3.dataset.rolling", "mean");
  static final IFn sumFn = requiringResolve("tech.v3.dataset.rolling", "sum");
  static final IFn minFn = requiringResolve("tech.v3.dataset.rolling", "min");
  static final IFn maxFn = requiringResolve("tech.v3.dataset.rolling", "max");
  static final IFn varianceFn = requiringResolve("tech.v3.dataset.rolling", "variance");
  static final IFn stddevFn = requiringResolve("tech.v3.dataset.rolling", "standard-deviation");
  static final IFn nth = requiringResolve("tech.v3.dataset.rolling", "nth");
  static final IFn firstFn = requiringResolve("tech.v3.dataset.rolling", "first");
  static final IFn lastFn = requiringResolve("tech.v3.dataset.rolling", "last");
  static final IFn rollingFn = requiringResolve("tech.v3.dataset.rolling", "rolling");

  /**
   * Fixed or variable rolling window reductions.
   *
   * @param windowSpec Window specification specifying the type of window, either a
   * window over a fixed number of rows or a window based on a fixed logical
   * quantitative difference i.e. three months or 10 milliseconds.
   * @param reducerMap map of dest column name to reducer where reducer is a map with
   * two keys, :column-name which is the input column to use and :reducer which is
   * an IFn that receives each window of data as a buffer.
   *
   * Example:
   *
   *```java
   * Map stocks = makeDataset("https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv");
   *
   * //Variable-sized windows require the source column to be sorted.
   * stocks = sortByColumn(stocks, "date");
   * Map variableWin = Rolling.rolling(stocks,
   *				      Rolling.variableWindow("date", 3, kw("months")),
   *				      hashmap("price-mean-3m", Rolling.mean("price"),
   *					      "price-max-3m", Rolling.max("price"),
   *					      "price-min-3m", Rolling.min("price")));
   *println(head(variableWin, 10));
   *https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [10 6]:
   * //| symbol |       date |  price | price-max-3m | price-mean-3m | price-min-3m |
   * //|--------|------------|-------:|-------------:|--------------:|-------------:|
   * //|   AAPL | 2000-01-01 |  25.94 |       106.11 |   58.92500000 |        25.94 |
   * //|    IBM | 2000-01-01 | 100.52 |       106.11 |   61.92363636 |        28.66 |
   * //|   MSFT | 2000-01-01 |  39.81 |       106.11 |   58.06400000 |        28.66 |
   * //|   AMZN | 2000-01-01 |  64.56 |       106.11 |   60.09222222 |        28.66 |
   * //|   AAPL | 2000-02-01 |  28.66 |       106.11 |   57.56583333 |        28.37 |
   * //|   MSFT | 2000-02-01 |  36.35 |       106.11 |   60.19363636 |        28.37 |
   * //|    IBM | 2000-02-01 |  92.11 |       106.11 |   62.57800000 |        28.37 |
   * //|   AMZN | 2000-02-01 |  68.87 |       106.11 |   59.29666667 |        28.37 |
   * //|   AMZN | 2000-03-01 |  67.00 |       106.11 |   54.65583333 |        21.00 |
   * //|   MSFT | 2000-03-01 |  43.22 |       106.11 |   53.53363636 |        21.00 |
   *
   * //Fixed window...
   *
   * Object radians = VecMath.mul(2.0*Math.PI, VecMath.div(range(33), 32.0));
   * Map sinds = makeDataset(hashmap("radians", radians, "sin", VecMath.sin(radians)));
   * Map fixedWin = Rolling.rolling(sinds,
   *				   Rolling.fixedWindow(4),
   *				   hashmap("sin-roll-mean", Rolling.mean("sin"),
   *					   "sin-roll-max", Rolling.max("sin"),
   *					   "sin-roll-min", Rolling.min("sin")));
   *println(head(fixedWin, 8));
   * //_unnamed [8 5]:

   * //|        sin |    radians | sin-roll-max | sin-roll-min | sin-roll-mean |
   * //|-----------:|-----------:|-------------:|-------------:|--------------:|
   * //| 0.00000000 | 0.00000000 |   0.19509032 |   0.00000000 |    0.04877258 |
   * //| 0.19509032 | 0.19634954 |   0.38268343 |   0.00000000 |    0.14444344 |
   * //| 0.38268343 | 0.39269908 |   0.55557023 |   0.00000000 |    0.28333600 |
   * //| 0.55557023 | 0.58904862 |   0.70710678 |   0.19509032 |    0.46011269 |
   * //| 0.70710678 | 0.78539816 |   0.83146961 |   0.38268343 |    0.61920751 |
   * //| 0.83146961 | 0.98174770 |   0.92387953 |   0.55557023 |    0.75450654 |
   * //| 0.92387953 | 1.17809725 |   0.98078528 |   0.70710678 |    0.86081030 |
   * //| 0.98078528 | 1.37444679 |   1.00000000 |   0.83146961 |    0.93403361 |
   *```
   */
  public static Map rolling(Object ds, Map windowSpec, Map reducerMap) {
    return (Map)rollingFn.invoke(ds, windowSpec, reducerMap);
  }
  /**
   * Create a variable window specification with a double windowsize for a particular column.
   * This specification will not work on datetime columns.
   */
  public static Map variableWindow(Object colname, double windowSize) {
    return hashmap(kw("window-type"), kw("variable"),
		   kw("column-name"), colname,
		   kw("window-size"), windowSize);
  }
  /**
   * Create a variable window specification with a double windowsize for a particular column
   * and a compFn which must take two values and return a double.  The function must take 2
   * arguments and the arguments are passed in as (later,earlier).  This allows the basic
   * clojure '-' operator to work fine in many cases.
   *
   */
  public static Map variableWindow(Object colname, double windowSize, Object compFn) {
    return hashmap(kw("window-type"), kw("variable"),
		   kw("column-name"), colname,
		   kw("window-size"), windowSize,
		   kw("comp-fn"), compFn);
  }
  /**
   * Create a datetime-specific variable window specification with a double windowsize for
   * a particular column.
   *
   * @param datetimeUnit One of `[:milliseconds, :seconds, :hours, :days, :months]`.
   */
  public static Map variableWindow(Object colname, double windowSize, Keyword datetimeUnit) {
    return hashmap(kw("window-type"), kw("variable"),
		   kw("column-name"), colname,
		   kw("window-size"), windowSize,
		   kw("units"), datetimeUnit);
  }
  /**
   * Return fixed size rolling window.  Window will be fixed over `window-size` rows.
   */
  public static Map fixedWindow(long windowSize) {
    return hashmap(kw("window-type"), kw("fixed"),
		   kw("window-size"), windowSize);
  }
  /**
   * Return fixed size rolling window.  Window will be fixed over `window-size` rows.
   *
   * @param winPos One of `[:left :center :right]`.  This combined with the default
   *        edge mode of `:clamp` dictates the windows of data the reducer sees.
   */
  public static Map fixedWindow(long windowSize, Keyword winPos) {
    return hashmap(kw("window-type"), kw("fixed"),
		   kw("window-size"), windowSize,
		   kw("relative-window-position"), winPos);
  }
  /**
   * Return fixed size rolling window.  Window will be fixed over `window-size` rows.
   *
   * @param winPos One of `[:left :center :right]`.  This combined with the default
   *        edge mode dictates windows of data the reducer sees.
   *
   * @param edgeMode One of `[:zero, null, :clamp]`.  Clamp means repeat the end value.
   */
  public static Map fixedWindow(long windowSize, Keyword winPos, Keyword edgeMode) {
    return hashmap(kw("window-type"), kw("fixed"),
		   kw("window-size"), windowSize,
		   kw("relative-window-position"), winPos,
		   kw("edge-mode"), edgeMode);
  }
  /**
   * Create a columnwise reducer.  This reducer gets sub-windows from the column and
   * must return a scalar value.
   */
  public static Map reducer(Object srcColname, IFn reduceFn) {
    return hashmap(kw("column-name"), srcColname,
		   kw("reducer"), reduceFn);
  }
  /** mean reducer*/
  public static Map mean(Object colname) {
    return (Map)meanFn.invoke(colname);
  }
  /** sum reducer*/
  public static Map sum(Object colname) {
    return (Map)sumFn.invoke(colname);
  }
  /** min reducer*/
  public static Map min(Object colname) {
    return (Map)minFn.invoke(colname);
  }
  /** max reducer*/
  public static Map max(Object colname) {
    return (Map)maxFn.invoke(colname);
  }
  /** stddev reducer*/
  public static Map stddev(Object colname) {
    return (Map)stddevFn.invoke(colname);
  }
  /** variance reducer*/
  public static Map variance(Object colname) {
    return (Map)varianceFn.invoke(colname);
  }
  /** reducer that keeps the first value*/
  public static Map first(Object colname) {
    return (Map)firstFn.invoke(colname);
  }
  /** reducer that keeps the last value*/
  public static Map last(Object colname) {
    return (Map)lastFn.invoke(colname);
  }
}
