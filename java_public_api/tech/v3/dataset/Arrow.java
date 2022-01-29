package tech.v3.dataset;

import static tech.v3.Clj.*;
import clojure.lang.IFn;
import java.util.Map;


/**
 * Bindings to save/load datasets apache arrow streaming format.  These bindings support
 * JDK-17, memory mapping, and per-column compression.
 *
 * Required Dependencies:
 *
 *```clojure
 *[org.apache.arrow/arrow-vector "6.0.0"]
 *[org.lz4/lz4-java "1.8.0"]
 *[com.github.luben/zstd-jni "1.5.1-1"]
 *```
 */
public class Arrow {

  private Arrow(){}

  static final IFn dsToStreamFn = requiringResolve("tech.v3.libs.arrow", "dataset->stream!");
  static final IFn streamToDsFn = requiringResolve("tech.v3.libs.arrow", "stream->dataset");
  static final IFn dsSeqToStreamFn = requiringResolve("tech.v3.libs.arrow", "dataset-seq->stream!");
  static final IFn streamToDsSeqFn = requiringResolve("tech.v3.libs.arrow", "stream->dataset-seq");

  /**
   * Save a dataset to apache stream format.
   *
   * Options:
   *
   * * `strings-as-text?`: - defaults to false - Save out strings into arrow files without
   *  dictionaries.  This works well if you want to load an arrow file in-place or if
   *  you know the strings in your dataset are either really large or should not be in
   *  string tables.
   *
   * * `:compression` - Either `:zstd` or `:lz4`,  defaults to no compression (nil).
   * Per-column compression of the data can result in some significant size savings
   * (2x+) and thus some significant time savings when transferring over the network.
   * Using compression makes loading via mmap non-in-place - If you are going to use
   * compression mmap probably doesn't make sense on load and most likely will
   * result on slower loading times.  Zstd can also be passed in map form with an
   * addition parameter, `:level` which defaults to 3.
   *
   *
   *```java
   * //Slightly higher compression than the default.
   *datasetToStream(ds, "data.arrow-ipc", hashmap(kw("compression"),
   *                                              hashmap(kw("compression-type"), kw("zstd"),
   *                                                      kw("level"), 5)));
   *```
   */
  public static void datasetToStream(Object ds, Object pathOrInputStream, Object options) {
    dsToStreamFn.invoke(ds, pathOrInputStream, options);
  }
  /**
   * Save a sequence of datasets to a single stream file.  Datasets must either have matching
   * schemas or downstream dataset column datatypes must be able to be widened to the initial
   * dataset column datatypes.
   *
   * For options see `datasetToStream`.
   */
  public static void datasetSeqToStream(Iterable dsSeq, Object pathOrInputStream, Object options) {
    dsSeqToStreamFn.invoke(dsSeq, pathOrInputStream, options);
  }
  /**
   * Load an apache arrow streaming file returning a single dataset.  File must only contain a
   * single record batch.
   *
   * Options:
   *
   * * `:open-type` - Either `:mmap` or `:input-stream` defaulting to the slower but more robust
   * `:input-stream` pathway.  When using `:mmap` resources will be released when the resource
   * system dictates - see documentation for [tech.v3.DType.stackResourceContext](https://cnuernber.github.io/dtype-next/javadoc/index.html).
   * When using `:input-stream` the stream will be closed when the lazy sequence is either fully realized or an
   * exception is thrown.
   *
   * * `close-input-stream?` - When using `:input-stream` `:open-type`, close the input
   * stream upon exception or when stream is fully realized.  Defaults to true.
   *
   * * `:integer-datatime-types?` - when true arrow columns in the appropriate packed
   * datatypes will be represented as their integer types as opposed to their respective
   * packed types.  For example columns of type `:epoch-days` will be returned to the user
   * as datatype `:epoch-days` as opposed to `:packed-local-date`.  This means reading values
   * will return integers as opposed to `java.time.LocalDate`s.
   */
  public static Map streamtoDataset(Object pathOrInputStream, Object options) {
    return (Map)streamToDsFn.invoke(pathOrInputStream, options);
  }

  /**
   * Load an apache arrow streaming file returning a sequence of datasets, one for each record batch.
   * For options see streamToDataset.
   */
  public static Iterable streamToDatasetSeq(Object pathOrInputStream, Object options) {
    return (Iterable)streamToDsSeqFn.invoke(pathOrInputStream, options);
  }
}
