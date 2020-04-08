# tech.ml.dataset


[![Clojars Project](https://img.shields.io/clojars/v/techascent/tech.ml.dataset.svg)](https://clojars.org/techascent/tech.ml.dataset)


Dataset and ETL pipeline for machine learning.  Datasets are currently in-memory
columnwise databases and we support parsing from file or input-stream which means
we support gzipped csv/tsv files.  The backing store behind tech.ml.dataset is
[tech.datatype](https://github.com/techascent/tech.datatype).  We now have support
for datetime types and joins!


For a quick code-oriented intro, please see the [walkthrough](docs/walkthrough.md).


An example of using the dataset for [advanced regression techniques](https://github.com/cnuernber/ames-house-prices/blob/master/ames-housing-prices-clojure.md).


## Dataset Pipeline Processing

Dataset ETL for this library consists of loading heterogeneous columns of data and then
operating on that data in a mainly columnwise fashion.


[tech.v2.datatype](https://github.com/techascent/tech.datatype) subsystem which is
described on our [blog](http://techascent.com/blog/datatype-library.html).
[Here is a cheatsheet](https://github.com/techascent/tech.datatype/blob/master/docs/cheatsheet.md).
## Walkthrough

```clojure
;; You have a seq of maps

user> (take 5 (mapseq-fruit-dataset))
({:fruit-label 1.0,
  :fruit-name :apple,
  :fruit-subtype :granny-smith,
  :mass 192.0,
  :width 8.4,
  :height 7.3,
  :color-score 0.55}
 {:fruit-label 1.0,
  :fruit-name :apple,
  :fruit-subtype :granny-smith,
  :mass 180.0,
  :width 8.0,
  :height 6.8,
  :color-score 0.59}
  ...

;; Here are the namespaces
user> (require '[tech.ml.dataset :as ds])
:tech.resource.gc Reference thread starting
nil
user> (require '[tech.v2.datatype.functional :as dfn])
nil
user> (require '[tech.ml.dataset.pipeline :as dsp])
nil
user> (require '[tech.ml.dataset.pipeline.column-filters :as cf])

;; Making a dataset is easy:
user> (def fruits (ds/->dataset (mapseq-fruit-dataset)))
#'user/fruits
user> (ds/column-names fruits)
(:fruit-label :fruit-name :fruit-subtype :mass :width :height :color-score)

;;Select allows you to grab various rectangles, and println is your friend

user> (println (ds/select fruits [:fruit-name :mass :width] (range 10)))
_unnamed [10 3]:

| :fruit-name |   :mass | :width |
|-------------+---------+--------|
|       apple | 192.000 |  8.400 |
|       apple | 180.000 |  8.000 |
|       apple | 176.000 |  7.400 |
|    mandarin |  86.000 |  6.200 |
|    mandarin |  84.000 |  6.000 |
|    mandarin |  80.000 |  5.800 |
|    mandarin |  80.000 |  5.900 |
|    mandarin |  76.000 |  5.800 |
|       apple | 178.000 |  7.100 |
|       apple | 172.000 |  7.400 |


;;Columns implement dataset reader/writer (see the cheatsheet), so anything goes
;;with them

user> (dfn/+ (dfn/* (ds/column fruits :mass) 0.5) (ds/column fruits :width))
[104.39999961853027 98.0 95.40000009536743 49.19999980926514 48.0
45.80000019073486 45.90000009536743 43.80000019073486 96.09999990463257
93.40000009536743 89.90000009536743 93.09999990463257
...
user> (type *1)
tech.v2.datatype.binary_op$fn$reify__45831


user> (require '[tech.ml.dataset.column :as ds-col])
nil
user> (ds-col/stats (ds/column fruits :mass) [:min :max :mean])
{:min 76.0, :mean 163.11864406779662, :max 362.0}

;; Sometimes you will need to more advanced processing
;; This is where the pipeline concept comes in.

user> (def processed-ds (-> fruits
                            (dsp/string->number)
                            (dsp/->datatype)
                            (dsp/range-scale)))
#'user/processed-ds


user> (println (ds/select processed-ds [:fruit-name :mass :width] (range 10)))
_unnamed [10 3]:

| :fruit-name |  :mass | :width |
|-------------+--------+--------|
|       0.000 | -0.189 |  0.368 |
|       0.000 | -0.273 |  0.158 |
|       0.000 | -0.301 | -0.158 |
|       3.000 | -0.930 | -0.789 |
|       3.000 | -0.944 | -0.895 |
|       3.000 | -0.972 | -1.000 |
|       3.000 | -0.972 | -0.947 |
|       3.000 | -1.000 | -1.000 |
|       0.000 | -0.287 | -0.316 |
|       0.000 | -0.329 | -0.158 |

nil
user> (ds-col/stats (ds/column processed-ds :mass) [:min :max :mean])
{:min -1.0, :mean -0.3907787128126111, :max 1.0}

user> (cf/categorical? processed-ds)
(:fruit-name :fruit-subtype)
user> (cf/numeric? processed-ds)
(:fruit-label :fruit-name :fruit-subtype :mass :width :height :color-score)

;;You can always get any of the columns back as a java array

user> (dtype/->array-copy (ds/column processed-ds :width))
[0.36842068278560225, 0.15789457833668674, -0.15789482930359944, -0.7894738955510845,
 -0.8947369477755422, -1.0, -0.9473684738877711, -1.0, -0.3157896586071989,
 -0.15789482930359944, -0.42105271083165663, -0.3157896586071989, -0.36842118471942775,
...
user> (type *1)
[D
```

## Examples

* [sequences of maps](test/tech/ml/dataset/mapseq_test.clj)
* [regression pipelines](test/tech/ml/dataset/ames_test.clj)
* [real world example](https://github.com/cnuernber/ames-house-prices/blob/d60b18cb13f7d125dba787f23f0a81cac90c8861/src/clj_ml_wkg/ames_house_prices.clj)


## License

Copyright © 2019 Complements of TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
