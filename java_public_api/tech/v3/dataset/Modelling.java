package tech.v3.dataset;


import static tech.v3.Clj.*;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import java.util.Map;


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
  static final IFn numInfClassesFn = requiringResolve("tech.v3.dataset.modelling", "num-inference-classes");
  static final IFn infTargCnamesFn = requiringResolve("tech.v3.dataset.modelling", "inference-target-column-names");



  public static Map fitCategorical(Object ds, Object cname, Object options) {
    return (Map)fitCatFn.invoke(ds, cname, options);
  }
  public static Map fitCategorical(Object ds, Object cname) {
    return (Map)fitCatFn.invoke(ds, cname);
  }
  public static Map transformCategorical(Object ds, Object catFitData) {
    return (Map)transCatFn.invoke(ds, catFitData);
  }
  public static Map invertCategorical(Object ds, Object catFitData) {
    return (Map)invCatFn.invoke(ds, catFitData);
  }
  public static Map fitOneHot(Object ds, Object cname, Object options) {
    return (Map)fitOneHotFn.invoke(ds, cname, options);
  }
  public static Map fitOneHot(Object ds, Object cname) {
    return (Map)fitOneHotFn.invoke(ds, cname);
  }
  public static Map transformOneHot(Object ds, Object fitData) {
    return (Map)transOneHotFn.invoke(ds, fitData);
  }
  public static Map invertOneHot(Object ds, Object fitData) {
    return (Map)invOneHotFn.invoke(ds, fitData);
  }
  public static Map correlationTable(Object ds, Object options) {
    return (Map)corrTableFn.invoke(ds, options);
  }
  public static Map correlationTable(Object ds) {
    return (Map)corrTableFn.invoke(ds);
  }
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
  public static Object fitPCA(Object ds, Object options) {
    return fitPCAFn.invoke(ds, options);
  }
  public static Object fitPCA(Object ds) {
    return fitPCAFn.invoke(ds);
  }
  public static Map transformPCA(Object ds, Object fitData) {
    return (Map)transPCAFn.invoke(ds, fitData);
  }
  public static Object fitStdScale(Object ds, Object options) {
    return fitStdScaleFn.invoke(ds, options);
  }
  public static Object fitStdScale(Object ds) {
    return fitStdScaleFn.invoke(ds);
  }
  public static Map transformStdScale(Object ds, Object fitData) {
    return (Map)transStdScaleFn.invoke(ds, fitData);
  }
  public static Object fitMinMax(Object ds, Object options) {
    return fitMinMaxFn.invoke(ds, options);
  }
  public static Object fitMinMax(Object ds) {
    return fitMinMaxFn.invoke(ds);
  }
  public static Map transformMinMax(Object ds, Object fitData) {
    return (Map)transMinMaxFn.invoke(ds, fitData);
  }
  public static Map interpolateLOESS(Object ds, Object options) {
    return (Map)interpolateLOESSFn.invoke(ds, options);
  }
  public static Map interpolateLOESS(Object ds) {
    return (Map)interpolateLOESSFn.invoke(ds);
  }

}
