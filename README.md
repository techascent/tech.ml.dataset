# tech.ml.dataset


[![Clojars Project](https://img.shields.io/clojars/v/techascent/tech.ml.dataset.svg)](https://clojars.org/techascent/tech.ml.dataset)


Dataset and ETL pipeline for machine learning.  Datasets are currently in-memory
columnwise databases.  The backing store behind tech.ml.dataset is
[tablesaw](https://github.com/jtablesaw/tablesaw).  Further support is intended in the
near future for [Apache Arrow](https://github.com/apache/arrow).


## Dataset Pipeline Processing

Dataset ETL is a repeatable processing that stores data so that doing inference later is automatic.

1.  Build your ETL pipeline.
2.  Apply to training dataset.  Result is a new pipeline with things that min,max per column stored or even trained models.
3.  Train, gridsearch, get a model.
4.  Use ETL pipeline returned from (2) with no modification to apply to new inference samples.
5.  Infer.

## Example

```clojure
user> (require '[tech.ml.dataset.etl :as etl])
nil
user> (require '[tech.ml.dataset :as dataset])
nil
user> (require '[clojure.edn :as edn])
nil
user> (require '[clojure.java.io :as io])
nil
user> (require '[camel-snake-kebab.core :refer [->kebab-case]])
nil
user> (require '[clojure.string :as s])
nil

user> (def mapseq-fruit-dataset
  (memoize
   (fn []
     (let [fruit-ds (slurp (io/resource "fruit_data_with_colors.txt"))
           dataset (->> (s/split fruit-ds #"\n")
                        (mapv #(s/split % #"\s+")))
           ds-keys (->> (first dataset)
                        (mapv (comp keyword ->kebab-case)))]
       (->> (rest dataset)
            (map (fn [ds-line]
                   (->> ds-line
                        (map (fn [ds-val]
                               (try
                                 (Double/parseDouble ^String ds-val)
                                 (catch Throwable e
                                   (-> (->kebab-case ds-val)
                                       keyword)))))
                        (zipmap ds-keys)))))))))
#'user/mapseq-fruit-dataset
user> (take 3 (mapseq-fruit-dataset))
({:color-score 0.55,
  :fruit-label 1.0,
  :fruit-name :apple,
  :fruit-subtype :granny-smith,
  :height 7.3,
  :mass 192.0,
  :width 8.4}
 {:color-score 0.59,
  :fruit-label 1.0,
  :fruit-name :apple,
  :fruit-subtype :granny-smith,
  :height 6.8,
  :mass 180.0,
  :width 8.0}
 {:color-score 0.6,
  :fruit-label 1.0,
  :fruit-name :apple,
  :fruit-subtype :granny-smith,
  :height 7.2,
  :mass 176.0,
  :width 7.4})
...


user> (def etl-source-pipeline '[[remove [:fruit-subtype :fruit-label]]
                                 [string->number string?]
                                 ;;Range numeric data to -1 1
                                 [range-scaler (not categorical?)]])
#'user/etl-source-pipeline
user> (def pipeline-result (etl/apply-pipeline (mapseq-fruit-dataset)
                                               etl-source-pipeline
                                               {:target :fruit-name}))
#'user/pipeline-result
user> (:options pipeline-result)

user> (:options pipeline-result)
{:dataset-column-metadata {:post-pipeline [{:categorical? true,
                                            :datatype :float64,
                                            :name :fruit-name,
                                            :size 59,
                                            :target? true}
                                           {:datatype :float64, :name :mass, :size 59}
                                           {:datatype :float64, :name :width, :size 59}
                                           {:datatype :float64, :name :height, :size 59}
                                           {:datatype :float64,
                                            :name :color-score,
                                            :size 59}],
                           :pre-pipeline [{:datatype :float32,
                                           :name :fruit-label,
                                           :size 59}
                                          {:categorical? true,
                                           :datatype :string,
                                           :name :fruit-name,
                                           :size 59}
                                          {:categorical? true,
                                           :datatype :string,
                                           :name :fruit-subtype,
                                           :size 59}
                                          {:datatype :float32, :name :mass, :size 59}
                                          {:datatype :float32, :name :width, :size 59}
                                          {:datatype :float32, :name :height, :size 59}
                                          {:datatype :float32,
                                           :name :color-score,
                                           :size 59}]},
 :feature-columns [:color-score :height :mass :width],
 :label-columns [:fruit-name],
 :label-map {:fruit-name {"apple" 0, "lemon" 2, "mandarin" 3, "orange" 1}},
 :target :fruit-name}
user> (:pipeline pipeline-result)
[{:context {}, :operation [remove [:fruit-subtype :fruit-label]]}
 {:context {:label-map {:fruit-name {"apple" 0, "lemon" 2, "mandarin" 3, "orange" 1}}},
  :operation [string->number (:fruit-name)]}
 {:context {:color-score {:max 0.9300000071525574, :min 0.550000011920929},
            :height {:max 10.5, :min 4.0},
            :mass {:max 362.0, :min 76.0},
            :width {:max 9.600000381469727, :min 5.800000190734863}},
  :operation [range-scaler #{:color-score :height :mass :width}]}]
user> (clojure.pprint/print-table (->> (dataset/->flyweight (:dataset pipeline-result))
                                       (take 10)))

| :fruit-name |                :mass |               :width |               :height |        :color-score |
|-------------+----------------------+----------------------+-----------------------+---------------------|
|         0.0 | -0.18881118881118886 |  0.36842068278560225 |  0.015384674072265625 |                -1.0 |
|         0.0 |  -0.2727272727272727 |  0.15789457833668674 |  -0.13846147977388823 |  -0.789473882342312 |
|         0.0 | -0.30069930069930073 | -0.15789482930359944 | -0.015384674072265625 | -0.7368420392192294 |
|         3.0 |  -0.9300699300699301 |  -0.7894738955510845 |   -0.7846154433030348 | 0.31578949019519276 |
|         3.0 |  -0.9440559440559441 |  -0.8947369477755422 |   -0.8153846447284405 |  0.2631579607807706 |
|         3.0 |   -0.972027972027972 |                 -1.0 |   -0.9076922490046575 | 0.15789458824326608 |
|         3.0 |   -0.972027972027972 |  -0.9473684738877711 |   -0.9076922490046575 |  0.3684210196096147 |
|         3.0 |                 -1.0 |                 -1.0 |                  -1.0 |  0.3684210196096147 |
|         0.0 | -0.28671328671328666 |  -0.3157896586071989 |   0.16923082791841937 |   0.947368470585578 |
|         0.0 | -0.32867132867132864 | -0.15789482930359944 |  -0.07692307692307687 |  0.7894735686336514 |
nil
```


## ETL Language Design


The ETL language is current an implicit piping of a list of dataset->dataset transformations.

Each operator involves a column selection and then an operation, thus there are three sub-languages defined:


* Column Selection - [column-filters.md](docs/column-filters.md).
* Math Operations - [math-ops.md](docs/math-ops.md).
* Pipeline Operators - [pipeline-operators.md](docs/pipeline-operators.md).


## Extension


There is an extensive protocol hierarchy for supporting different data stores.  You can buy into the hierarchy at
several levels:

* [dataset](src/tech/ml/protocols/dataset.clj).
* [column](src/tech/ml/protocols/column.clj) and then use the generic table
  [implementation](src/ml/tech/dataset/generic_columnar_dataset.clj).


The current system is built on top of support for the [tech.datatype](https://github.com/techascent/tech.datatype) subsystem
which is described on our [blog](http://techascent.com/blog/datatype-library.html).  You can see the datatype-level bindings
to [fastutil](src/tech/libs/tablesaw/datatype/fastutil.clj) and [tablesaw](src/tech/libs/tablesaw/datatype/tablesaw.clj).  These bindings enable generic and highly optimized copying simple transformations as well as enabling the tensor subsystem to operate
on the base data in-place efficiently.


Thanks to the integration of datatype, tech.compute, and jna, most operations are
optimized.  Especially [math and transformations](https://github.com/techascent/tech.ml.dataset/blob/9739d72a81350ae5b8688ee9109290a04586b772/src/tech/ml/dataset/etl/pipeline_operators.clj#L228).

The tablesaw column-level bindings are [here](src/tech/libs/tablesaw.clj).  They use the generic table support and as
such they do not use the actual tablesaw 'table' datatype.


## License

Copyright © 2019 Complements of TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
