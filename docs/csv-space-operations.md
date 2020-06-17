# CSV Space Operations

For really large datasets it may be useful to filter/manipulate/sample data in CSV
space before parsing the data into columnar format.  We provide a bit of support for
that type of operation in the form of separating transforming a csv into a sequence
of string-array and then parsing those string-array rows.


### Input Data -> String Array

```clojure
user> (def rows (parse/csv->rows "https://raw.githubusercontent.com/techascent/tech.ml.dataset/master/test/data/stocks.csv"))
#'user/rows
user> (take 10 rows)
(["symbol", "date", "price"]
 ["MSFT", "Jan 1 2000", "39.81"]
 ["MSFT", "Feb 1 2000", "36.35"]
 ["MSFT", "Mar 1 2000", "43.22"]
 ["MSFT", "Apr 1 2000", "28.37"]
 ["MSFT", "May 1 2000", "25.45"]
 ["MSFT", "Jun 1 2000", "32.54"]
 ["MSFT", "Jul 1 2000", "28.4"]
 ["MSFT", "Aug 1 2000", "28.4"]
 ["MSFT", "Sep 1 2000", "24.53"])
```

Note that the header row is included in these rows.  Now you can filter/sample/etc. in
`CSV` space meaning transformations from sequences of string arrays to sequences of
string arrays.


### Various Manipulations


So we save off the header row (probably don't want to manipulate that) and do some
transformations:

```clojure
user> (def header-row (first rows))
#'user/header-row
user> (def rows (->> (rest rows)
                     (take-nth 4)))
#'user/rows
```

### String Array Sequence -> Dataset


Now we produce the final dataset from the concatenated header row
and the rest of the string arrays.  In the example below I show setting a column
datatype and parsing the column name into keywords just to show that the parser options
for ->dataset also apply here.

```clojure
user> (def stocks (parse/rows->dataset {:dataset-name "stocks"
                                        :key-fn keyword
                                        :parser-fn {"date" :packed-local-date}}
                                       (concat [header-row]
                                               rows)))
#'user/stocks
user> stocks
stocks [140 3]:

| :symbol |      :date | :price |
|---------|------------|-------:|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-05-01 |  25.45 |
|    MSFT | 2000-09-01 |  24.53 |
|    MSFT | 2001-01-01 |  24.84 |
...
```

In this way you can do various manipulations in pure string space but still not have
to parse the data any further.  If you decide that you want to parse the data yourself
then it is probably best to simply convert your parsed data into a sequence of maps and
call `tech.ml.dataset/->dataset`.
