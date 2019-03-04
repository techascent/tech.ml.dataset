# Pipeline Operators


The heart of the pipeline system is pipeline operators.  Pipeline operators implement
either multiple or simple column [interfaces](..//src/tech/ml/protocols/etl.clj).


They first build context, and the perform an operation.


During inference (or if recorded? flag is true), they will skip the build-context and
instead use stored context saved during training.  In this way a pipeline operator can
gather arbitrary context once during training and this information will be set and used
automatically during any further operations.


Many operators can use the single-column interface as this is simpler but the multiple
column interface is always available for either correctness or for performance
optimizations.


The canonical file for pipeline operators is [pipeline_operators.clj](../src/tech/ml/dataset/etl/pipeline_operators.clj).



## Existing Pipeline Operators


* `set-attribute` - Set an attribute in the metadata.
* `remove` - remove column/columns from dataset.
* `replace-missing` - replace missing values with result of math expression.
* `string->number` - Convert a string column to a numeric column recording conversion
  table for use during inference time.  This operator updates the options map returned
  so the rest of the system has access to this conversion and its inverse.
* `one-hot` - Convert a string column to one or more derived numeric columns.  With no arguments, this operator
does a standard 'one-hot' conversion.  Users are free to pass in several variations which control how new columns
are produced and source values map to which columns:
```clojure
[one-hot :fruit-name]
[one-hot :fruit-name {:main ["apple" "mandarin"]
                      :other :rest}]
[one-hot :fruit-name ["apple" "mandarin" "orange" "lemon"]]
```
* `replace-string` - Replace a string with a new string.
* `->etl-datatype` - force conversion to the datatype specified in the etl defaults.  This is usually :float64.
* `m=` - Perform a math operation.  See [math-ops documentation](math-ops.md) documentation for list of operations.
```clojure
;; Total number of bathrooms
[m= "TotalBath" (+ (col "BsmtFullBath")
                   (* 0.5 (col "BsmtHalfBath"))
                   (col "FullBath")
                   (* 0.5 "HalfBath"))]

;;Help linear models become polynomial models.
(->> (rest aimes-top-columns)
     (mapcat (fn [colname]
                 [['m= (str colname "-s2") ['** ['col colname] 2]]
                  ['m= (str colname "-s3") ['** ['col colname] 3]]
                  ['m= (str colname "-sqrt") ['sqrt ['col colname]]]]))
     (concat full-aimes-pt-1)
     vec))
;; Reduce dataset skew
[m= [and
     [not categorical?]
     [not target?]
     [> [abs [skew [col]]] 0.5]]
    (log1p (col))]
```
* `range-scaler` - Scale values of columns to be within defined range.
* `std-scaler` - Scale values of columns such that mean is 0 and std-dev is 1.
* `impute-missing` - Perform missing-value-aware k-means on training dataset to build clusters, generate means
from clusters and then use the centroids during later operations to find the clusters that a given
column's missing value pertains to and use those means to fill in missing values.
* `pca` - Standard, vanilla PCA.  Eigenvectors, means, and eigenvalues are stored as context along with
the number of result columns.  Users can pass in a map with following keys:
{:method - :svd or :correlation - defaults to svd.
 :n-components - Number of components.  Defaults to not-provided.
 :variance - Fraction of variance to retain.  Only used if n-components is not in the
 map.  Defaults to 0.95.
