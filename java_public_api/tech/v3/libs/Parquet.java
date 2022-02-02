package tech.v3.libs;


import static tech.v3.Clj.*;
import clojure.lang.IFn;
import java.util.Map;

/**
 * Read/write parquet files.  Uses the standard hadoop parquet library.  One aspect that
 * may be confusing is that when writing files the parquet system decides when to end
 * the record batch so a single dataset may end up as a single parquet file with many
 * record batches.
 *
 * Note that in the requiring dependencies I remove slf4j.  tmd comes with logback-classic
 * by default which is less featureful but far less of a security disaster than slf4j.  If you
 * have a setup that already uses slf4j then you should exclude logback-classic from
 * tmd's dependencies.
 *
 * You must disable debug logging else the parquet system is unreasonably slow.  See logging
 * section of [parquet namespace](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html).
 *
 * Required dependencies:
 *
 *```clojure
 *org.apache.parquet/parquet-hadoop {:mvn/version "1.12.0"
 *                                    :exclusions [org.slf4j/slf4j-log4j12]}
 *org.apache.hadoop/hadoop-common {:mvn/version "3.3.0"
 *                                 :exclusions [org.slf4j/slf4j-log4j12]}
 *;; We literally need this for 1 POJO formatting object.
 *org.apache.hadoop/hadoop-mapreduce-client-core {:mvn/version "3.3.0"
 *                                                :exclusions [org.slf4j/slf4j-log4j12]}
 *```
 */
public class Parquet
{
  private Parquet(){}
  
  static final IFn dsToParquetFn = requiringResolve("tech.v3.libs.parquet", "ds->parquet");
  static final IFn dsSeqToParquetFn = requiringResolve("tech.v3.libs.parquet", "ds-seq->parquet");
  static final IFn parquetToDsSeqFn = requiringResolve("tech.v3.libs.parquet", "parquet->ds-seq");
  static final IFn parquetToDsFn = requiringResolve("tech.v3.libs.parquet", "parquet->ds");
  static final IFn parquetToMetadataSeq = requiringResolve("tech.v3.libs.parquet", "parquet->metadata-seq");

  public static Iterable parquetMetadata(String path) {
    return (Iterable)parquetToMetadataSeq.invoke(path);
  }
  public static Map parquetToDataset(String path, Object options) {
    return (Map)parquetToDsFn.invoke(path, options);
  }
  public static Iterable parquetToDatasetSeq(String path, Object options) {
    return (Iterable)parquetToDsSeqFn.invoke(path, options);
  }
  public static void datasetToParquet(Object ds, String path, Object options) {
    dsToParquetFn.invoke(ds, path, options);
  }
  public static void datasetSeqToParquet(Iterable dsSeq, String path, Object options) {
    dsSeqToParquetFn.invoke(dsSeq, path, options);
  }
}
