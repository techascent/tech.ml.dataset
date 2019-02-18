# Column Filters


Column filters return a list of column names for a pipeline operator to operate on.

Column filtering is a set based order invariant language so the order of columns returned
is irrelevant.  The results of column filters are stored during the training phase so the
filter itself isn't run during inference but the pipeline operator is applied to the
original list of columns.


## Existing Filters


* `\*` - all columns.
* `numeric?` - set of numeric columns.
* `categorical?` - set of categorical columns.
* `target?` - set of columns marked as target (set-attribute or pipeline options).
* `missing?` - set of columns with missing information.
* `[boolean? string? int16? int32? int64? float32? float64?]` - set of columns with matching datatype.
* `[or not and]` - Boolean set logic on returned column sets
   * or = union
   * not = difference with global set
   * and = intersection.
* `[> < >= <= == !=]` - Math logic operators, return columns were the math expressions evaluated on boths sides return true.


## Examples

```clojure
[m= [and
      [not categorical?]
      [not target?]
      [> [abs [skew [col]]] 0.5]]
    (log1p (col))]


[string->number ["ExterQual"
                 "ExterCond"
                 "BsmtQual"
                 "BsmtCond"
                 "HeatingQC"
                 "KitchenQual"
                 "FireplaceQu"
                 "GarageQual"
                 "GarageCond"
                 "PoolQC"]
				 ["Ex" "Gd" "TA" "Fa" "Po" "NA"]]
```


## Registering New Filters


The column filters namepsace contains an API function, `registing-column-filter!`.  The symbol column filter name
is converted to a keyword and looked up in a global map.

The filter-function has an api of `(dataset & arguments)` where the arguments are passed after evaluation to
the filter function.  The return value is expected to be either a sequence of column names or a set of
column names.


## Executing Column Filters

It is useful to execute column filters from the repl at times.  There is a function provided,
`execute-column-filter` that takes as its first argument a dataset and as its second argument the
column filter to execute.

```clojure
(println (column-filters/execute-column-filter dataset '[and [not target?]
                                                             [not categorical?]]))
```
