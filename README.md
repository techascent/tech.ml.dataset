# tech.ml.dataset


[![Clojars Project](https://img.shields.io/clojars/v/techascent/tech.ml.dataset.svg)](https://clojars.org/techascent/tech.ml.dataset)


Dataset and ETL pipeline for machine learning.  Datasets are currently in-memory
columnwise databases.  The backing store behind tech.ml.dataset is
[tablesaw](https://github.com/jtablesaw/tablesaw).  Further support is intended in the
near future for [Apache Arrow](https://github.com/apache/arrow).


An example of using the dataset for [advanced regression techniques](https://github.com/cnuernber/ames-house-prices/blob/master/ames-housing-prices-clojure.md).


## Dataset Pipeline Processing

Dataset ETL for this library consists of loading heterogeneous columns of data and then
operating on that data in a mainly columnwise fashion.


[tech.v2.datatype](https://github.com/techascent/tech.datatype) subsystem which is
described on our [blog](http://techascent.com/blog/datatype-library.html).
[Here is a cheatsheet](https://github.com/techascent/tech.datatype/blob/master/docs/cheatsheet.md).

The tablesaw column-level bindings are [here](src/tech/libs/tablesaw.clj).  They use the
generic table support and as such they do not use the actual tablesaw 'table' datatype.

## Examples

* [sequences of maps](test/tech/ml/dataset/mapseq_test.clj)
* [regression pipelines](test/tech/ml/dataset/ames_test.clj)

## License

Copyright © 2019 Complements of TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
