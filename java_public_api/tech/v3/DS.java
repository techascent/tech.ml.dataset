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
 *
 */
public class DS {
  private DS(){}

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



  public static Map makeDataset(Object dsData, Map options) {
    return (Map)call(toDatasetFn, dsData, options);
  }
  public static Map makeDataset(Object dsData) {
    return (Map)call(toDatasetFn, dsData, null);
  }
  public static boolean isDataset(Object ds) {
    return (boolean)call(isDatasetFn, ds);
  }
  public static long rowCount(Object ds) {
    return (long)call(rowCountFn, ds);
  }
  public static long columnCount(Object ds) {
    return (long)call(columnCountFn, ds);
  }
  public static Object column(Object ds, Object cname) {
    return call(columnFn, ds, cname);
  }
  public static Map columnDef(Object name, Object data, Object missing, Object metadata) {
    return hashmap(keyword("tech.v3.dataset", "name"), name,
		   keyword("tech.v3.dataset", "data"), data,
		   keyword("tech.v3.dataset", "missing"), missing,
		   keyword("tech.v3.dataset", "metadata"), metadata);
  }
  public static Map columnDef(Object data, Object missing) {
    return hashmap(keyword("tech.v3.dataset", "name"), "_unnamed",
		   keyword("tech.v3.dataset", "data"), data,
		   keyword("tech.v3.dataset", "missing"), missing);
  }
  public static Map columnDef(Object data, Object missing, Object metadata) {
    return hashmap(keyword("tech.v3.dataset", "name"), "_unnamed",
		   keyword("tech.v3.dataset", "data"), data,
		   keyword("tech.v3.dataset", "missing"), missing,
		   keyword("tech.v3.dataset", "metadata"), metadata);
  }
  public static Map select(Object ds, Object columnNames, Object rows) {
    return (Map)call(selectFn, ds, columnNames, rows);
  }
  public static Map selectColumns(Object ds, Object columnNames ) {
    return (Map)call(selectColumnsFn, ds, columnNames);
  }
  public static Map dropColumns(Object ds, Object columnNames ) {
    return (Map)call(dropColumnsFn, ds, columnNames);
  }
  public static Map renameColumns(Object ds, Map renameMap) {
    return (Map)call(renameColumnsFn, ds, renameMap);
  }
  public static Map selectRows(Object ds, Object rowIndexes) {
    return (Map)call(selectRowsFn, ds, rowIndexes);
  }
  public static Map dropRows(Object ds, Object rowIndexes) {
    return (Map)call(dropRowsFn, ds, rowIndexes);
  }
  public static RoaringBitmap missing(Object dsOrColumn) {
    return (RoaringBitmap)call(missingFn, dsOrColumn);
  }
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
  public static Map replaceMissing(Object ds, Object strategy) {
    return replaceMissing(ds, strategy, kw("all"));
  }
  public static Buffer rows(Object ds) {
    return (Buffer)call(rowsFn, ds);
  }
  public static Buffer rowvecs(Object ds, boolean copying) {
    return (Buffer)call(rowvecsFn, ds, hashmap(kw("copying?"), copying));
  }
  public static Buffer rowvecs(Object ds) {
    return (Buffer)call(rowvecsFn, ds);
  }


  public static Map head(Object ds) {
    return (Map)call(headFn, ds);
  }
  public static Map head(Object ds, long nRows) {
    return (Map)call(headFn, ds, nRows);
  }
  public static Map tail(Object ds) {
    return (Map)call(tailFn, ds);
  }
  public static Map tail(Object ds, long nRows) {
    return (Map)call(tailFn, ds, nRows);
  }
  public static Map sample(Object ds) {
    return (Map)call(sampleFn, ds);
  }
  public static Map sample(Object ds, long nRows) {
    return (Map)call(sampleFn, ds, nRows);
  }
  public static Map sample(Object ds, long nRows, Map options) {
    return (Map)call(sampleFn, ds, nRows, options);
  }
  public static Map shuffle(Object ds) {
    return (Map)call(shuffleFn, ds);
  }
  public static Map shuffle(Object ds, Map options) {
    return (Map)call(shuffleFn, ds, options);
  }
  public static Map reverseRows(Object ds) {
    return (Map)call(reverseFn, ds);
  }

  public static Map columnMap(Object ds, Object resultCname, IFn mapFn, Object srcCnames) {
    return (Map)call(columnMapFn, ds, resultCname, mapFn, srcCnames);
  }
  public static Map rowMap(Object ds, IFn mapFn) {
    return (Map)call(rowMapFn, ds, mapFn);
  }
  public static Object rowMapcat(Object ds, IFn mapFn, Object options) {
    return call(rowMapcatFn, ds, mapFn, options);
  }
  public static Object pmapDS(Object ds, IFn mapFn, Object options) {
    return call(pmapDsFn, ds, mapFn, options);
  }

  public static Map sortBy(Object ds, IFn sortFn, Object compareFn, Object options) {
    return (Map)call(sortByFn, ds, sortFn, compareFn, options);
  }
  public static Map sortBy(Object ds, IFn sortFn, Object compareFn) {
    return (Map)call(sortByFn, ds, sortFn, compareFn, null);
  }
  public static Map sortBy(Object ds, IFn sortFn) {
    return (Map)call(sortByFn, ds, sortFn);
  }
  public static Map sortByColumn(Object ds, Object cname, Object compareFn, Object options) {
    return (Map)call(sortByColumnFn, ds, cname, compareFn, options);
  }
  public static Map sortByColumn(Object ds, Object cname, Object compareFn) {
    return (Map)call(sortByColumnFn, ds, cname, compareFn, null);
  }
  public static Map sortByColumn(Object ds, Object cname) {
    return (Map)call(sortByColumnFn, ds, cname);
  }
  public static Map filter(Object ds, IFn predicate) {
    return (Map)call(filterFn, ds, predicate);
  }
  public static Map filterColumn(Object ds, Object cname, IFn predicate) {
    return (Map)call(filterColumnFn, ds, cname, predicate);
  }
  public static Map groupBy(Object ds, IFn groupFn) {
    return (Map)call(groupByFn, ds, groupFn);
  }
  public static Map groupByColumn(Object ds, Object cname) {
    return (Map)call(groupByColumnFn, ds, cname);
  }
  public static Map concatCopying(Object datasets) {
    return (Map)call(applyFn, concatCopyingFn, datasets);
  }
  public static Map concatInplace(Object datasets) {
    return (Map)call(applyFn, concatInplaceFn, datasets);
  }
  public static Map concat(Object datasets) {
    return (Map)call(applyFn, concatCopyingFn, datasets);
  }
  public static Map uniqueBy(Object ds, IFn uniqueFn) {
    return (Map)call(uniqueByFn, ds, uniqueFn);
  }
  public static Map uniqueByColumn(Object ds, Object cname) {
    return (Map)call(uniqueByFn, ds, cname);
  }

  public static Map descriptiveStats(Object ds, Object options) {
    return (Map)call(descriptiveStatsFn, ds, options);
  }
  public static Map descriptiveStats(Object ds) {
    return (Map)call(descriptiveStatsFn, ds);
  }

  public static Object toNeanderthal(Object ds, Keyword layout, Keyword datatype) {
    return call(deref(toNeanderthalDelay), ds, layout, datatype);
  }
  public static Object toNeanderthal(Object ds) {
    return call(deref(toNeanderthalDelay), ds);
  }
  public static Map neanderthalToDataset(Object denseMat) {
    return (Map)call(deref(neanderthalToDatasetDelay), denseMat);
  }

  public static NDBuffer toTensor(Object ds, Keyword datatype) {
    return (NDBuffer)call(deref(toTensorDelay), ds, datatype);
  }
  public static NDBuffer toTensor(Object ds) {
    return (NDBuffer)call(deref(toTensorDelay), ds);
  }
  public static Map tensorToDataset(Object tens) {
    return (Map)call(deref(tensorToDatasetDelay), tens);
  }

  public static void writeDataset(Object ds, String path, Object options) {
    call(writeFn, ds, path, options);
  }

  public static void writeDataset(Object ds, String path) {
    writeDataset(ds, path, null);
  }
}
