# Nippy Rocks!


We are big fans of the [nippy system](https://github.com/ptaoussanis/nippy) for
freezing/thawing data.  So we were pleasantly surprized with how well it performs
with dataset and how easy it was to extend the dataset object to support nippy
natively.


## Nippy Hits One Out Of the Park


We start with a decent size gzipped tabbed-delimited file.

```console
chrisn@chrisn-lt-01:~/dev/tech.all/tech.ml.dataset/nippy-demo$ ls -alh
total 44M
drwxrwxr-x  2 chrisn chrisn 4.0K Jun 18 13:27 .
drwxr-xr-x 13 chrisn chrisn 4.0K Jun 18 13:27 ..
-rw-rw-r--  1 chrisn chrisn  44M Jun 18 13:27 2010.tsv.gz
```


```clojure
user> (def ds-2010 (time (ds/->dataset
                          "nippy-demo/2010.tsv.gz"
                          {:parser-fn {"date" [:packed-local-date "yyyy-MM-dd"]}})))
"Elapsed time: 8588.080218 msecs"
#'user/ds-2010
user> ;;rename column names so the tables print nicely
user> (def ds-2010
        (ds/select-columns ds-2010
                           (->> (ds/column-names ds-2010)
                                (map (fn [oldname]
                                       [oldname (.replace ^String oldname "_" "-")]))
                                (into {}))))
user> ds-2010
nippy-demo/2010.tsv.gz [2769708 12]:

|    low | comp-name-2 |   high | currency-code |  comp-name | m-ticker | ticker |  close |         volume | exchange |       date |   open |
|-------:|-------------|-------:|---------------|------------|----------|--------|-------:|---------------:|----------|------------|-------:|
|        |             |        |           USD | ALCOA CORP |      AA2 |     AA | 48.365 |                |     NYSE | 2010-01-01 |        |
| 49.355 |             | 51.065 |           USD | ALCOA CORP |      AA2 |     AA | 51.065 | 1.10618840E+07 |     NYSE | 2010-01-08 | 49.385 |
|        |             |        |           USD | ALCOA CORP |      AA2 |     AA | 46.895 |                |     NYSE | 2010-01-18 |        |
| 39.904 |             | 41.854 |           USD | ALCOA CORP |      AA2 |     AA | 40.624 | 1.46292500E+07 |     NYSE | 2010-01-26 | 40.354 |
| 40.294 |             | 41.674 |           USD | ALCOA CORP |      AA2 |     AA | 40.474 | 1.20107520E+07 |     NYSE | 2010-02-03 | 40.804 |
| 39.304 |             | 40.504 |           USD | ALCOA CORP |      AA2 |     AA | 39.844 | 1.46702890E+07 |     NYSE | 2010-02-09 | 40.084 |
| 39.574 |             | 40.264 |           USD | ALCOA CORP |      AA2 |     AA | 39.844 | 1.53728400E+07 |     NYSE | 2010-02-12 | 39.994 |
| 40.324 |             | 41.104 |           USD | ALCOA CORP |      AA2 |     AA | 40.624 | 7.72947100E+06 |     NYSE | 2010-02-22 | 41.044 |
| 39.664 |             | 40.564 |           USD | ALCOA CORP |      AA2 |     AA | 39.724 | 1.08365810E+07 |     NYSE | 2010-03-02 | 40.234 |
```


Our 44MB gzipped tsv produced 2.7 million rows and 12 columns.

Let's check the ram usage:
```clojure
user> (require '[clj-memory-meter.core :as mm])
nil
user> (mm/measure ds-2010)
"121.5 MB"
```

Now, let's save to an uncompressed nippy file:

```clojure
user> (require '[tech.io :as io])
nil
user> (time (tech.io/put-nippy! "test.nippy" ds-2010))
"Elapsed time: 1069.781703 msecs"
nil
```

One second, pretty nice :-).

What is the file size?
```console
chrisn@chrisn-lt-01:~/dev/tech.all/tech.ml.dataset/nippy-demo$ ls -alh
total 95M
drwxrwxr-x  2 chrisn chrisn 4.0K Jun 18 13:38 .
drwxr-xr-x 13 chrisn chrisn 4.0K Jun 18 13:36 ..
-rw-rw-r--  1 chrisn chrisn  51M Jun 18 13:38 2010.nippy
-rw-rw-r--  1 chrisn chrisn  44M Jun 18 13:27 2010.tsv.gz
```

Not bad, just a slight bit larger.

The load performance, however, is spectacular:
```clojure
user> (def loaded-2010 (time (io/get-nippy "nippy-demo/2010.nippy")))
"Elapsed time: 314.502715 msecs"
#'user/loaded-2010
user> (mm/measure loaded-2010)
"93.9 MB"
user> loaded-2010
nippy-demo/2010.tsv.gz [2769708 12]:

|    low | comp-name-2 |   high | currency-code |  comp-name | m-ticker | ticker |  close |         volume | exchange |       date |   open |
|-------:|-------------|-------:|---------------|------------|----------|--------|-------:|---------------:|----------|------------|-------:|
|        |             |        |           USD | ALCOA CORP |      AA2 |     AA | 48.365 |                |     NYSE | 2010-01-01 |        |
| 49.355 |             | 51.065 |           USD | ALCOA CORP |      AA2 |     AA | 51.065 | 1.10618840E+07 |     NYSE | 2010-01-08 | 49.385 |
|        |             |        |           USD | ALCOA CORP |      AA2 |     AA | 46.895 |                |     NYSE | 2010-01-18 |        |
| 39.904 |             | 41.854 |           USD | ALCOA CORP |      AA2 |     AA | 40.624 | 1.46292500E+07 |     NYSE | 2010-01-26 | 40.354 |
| 40.294 |             | 41.674 |           USD | ALCOA CORP |      AA2 |     AA | 40.474 | 1.20107520E+07 |     NYSE | 2010-02-03 | 40.804 |
| 39.304 |             | 40.504 |           USD | ALCOA CORP |      AA2 |     AA | 39.844 | 1.46702890E+07 |     NYSE | 2010-02-09 | 40.084 |
| 39.574 |             | 40.264 |           USD | ALCOA CORP |      AA2 |     AA | 39.844 | 1.53728400E+07 |     NYSE | 2010-02-12 | 39.994 |
```

It takes 8 seconds to load the tsv.  It takes 315 milliseconds to load the nippy!
That is great :-).


The resulting dataset is somewhat smaller in memory.  This is because when we
parse a dataset we use fastutil lists and append elements to them and then return a
dataset that sits directly on top of those lists as the column storage mechanism.  Those lists have a bit
more capacity than absolutely necessary.

When we save the data, we convert the data into base java/clojure datastructures
such as primitive arrays.  This is what makes things smaller: converting from a list
with a bit of extra capacity allocated to an exact sized array.  This operation is
optimized and hits System/arraycopy under the covers as fastutil lists use arrays as
the backing store and we make sure of the rest with `tech.datatype`.


## Gzipping The Nippy


We can do a bit better.  If you are really concerned about dataset size on disk, we
can save out a gzipped nippy:


```clojure
user> (time (io/put-nippy! (io/gzip-output-stream! "nippy-demo/2010.nippy.gz") ds-2010))
"Elapsed time: 7026.500505 msecs"
nil
```

This beats the gzipped tsv in terms of size by 10%:
```console
chrisn@chrisn-lt-01:~/dev/tech.all/tech.ml.dataset/nippy-demo$ ls -alh
total 134M
drwxrwxr-x  2 chrisn chrisn 4.0K Jun 18 13:47 .
drwxr-xr-x 13 chrisn chrisn 4.0K Jun 18 13:36 ..
-rw-rw-r--  1 chrisn chrisn  51M Jun 18 13:38 2010.nippy
-rw-rw-r--  1 chrisn chrisn  40M Jun 18 13:47 2010.nippy.gz
-rw-rw-r--  1 chrisn chrisn  44M Jun 18 13:27 2010.tsv.gz
```

And now it takes twice the time to load:

```clojure
user> (def loaded-gzipped-2010 (time (io/get-nippy (io/gzip-input-stream "nippy-demo/2010.nippy.gz"))))
"Elapsed time: 680.165118 msecs"
#'user/loaded-gzipped-2010
user> (mm/measure loaded-gzipped-2010)
"93.9 MB"
```

You can probably handle load times in the 700ms range if you have a strong reason to
have data compressed on disc.


## Intermix With Clojure Data

Another aspect of nippy that is really valuable is that it can save/load datasets that
are parts of arbitrary datastructures.  So for example you can save
the result of `group-by-column`:

```clojure

user> (def tickers (ds/group-by-column "ticker" ds-2010))
#'user/tickers
user> (type tickers)
clojure.lang.PersistentHashMap
user> (count tickers)
11532
user> (first tickers)
["RBYCF" RBYCF [261 12]:

|     low | comp_name_2 |    high | currency_code |     comp_name | m_ticker | ticker |   close |   volume | exchange |       date |    open |
|--------:|-------------|--------:|---------------|---------------|----------|--------|--------:|---------:|----------|------------|--------:|
|         |             |         |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 759.677 |          |      OTC | 2010-01-01 |         |
| 795.161 |             | 827.419 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 800.000 | 3596.775 |      OTC | 2010-01-12 | 816.129 |
| 741.935 |             | 779.032 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 758.064 | 5490.292 |      OTC | 2010-01-20 | 779.032 |
| 645.161 |             | 688.710 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 682.258 | 6201.953 |      OTC | 2010-01-28 | 669.355 |
| 685.484 |             | 725.806 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 687.097 | 3491.220 |      OTC | 2010-02-08 | 714.516 |
| 750.000 |             | 783.871 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 770.968 | 2927.057 |      OTC | 2010-02-17 | 780.645 |
...
```

`group-by and `group-by-column` both return persistent maps of key->dataset.

```clojure
user> (tech.io/put-nippy! "ticker-sorted.nippy" tickers)
nil
user> (def loaded-tickers (tech.io/get-nippy "ticker-sorted.nippy"))
#'user/loaded-tickers
user> (count loaded-tickers)
11532
user> (first loaded-tickers)
["RBYCF" RBYCF [261 12]:

|     low | comp_name_2 |    high | currency_code |     comp_name | m_ticker | ticker |   close |   volume | exchange |       date |    open |
|--------:|-------------|--------:|---------------|---------------|----------|--------|--------:|---------:|----------|------------|--------:|
|         |             |         |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 759.677 |          |      OTC | 2010-01-01 |         |
| 795.161 |             | 827.419 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 800.000 | 3596.775 |      OTC | 2010-01-12 | 816.129 |
| 741.935 |             | 779.032 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 758.064 | 5490.292 |      OTC | 2010-01-20 | 779.032 |
| 645.161 |             | 688.710 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 682.258 | 6201.953 |      OTC | 2010-01-28 | 669.355 |
| 685.484 |             | 725.806 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 687.097 | 3491.220 |      OTC | 2010-02-08 | 714.516 |
| 750.000 |             | 783.871 |           USD | RUBICON MNRLS |     RUBI |  RBYCF | 770.968 | 2927.057 |      OTC | 2010-02-17 | 780.645 |
```

Thus datasets can be used in maps, vectors, you name it and you can load/save those
really complex datastructures.  That can be a big help for complex dataflows.


## Simple Implementation


Our implementation of save/load for this pathway goes through two public functions:


* [dataset->data](https://github.com/techascent/tech.ml.dataset/blob/343f93a775975ff02704dcbaa205580fbbed3ef5/src/tech/ml/dataset.clj#L889) - Convert a dataset into a pure
clojure/java datastructure suitable for serialization.  Data is in arrays and string
tables have been slightly deconstructed.

* [data->dataset](https://github.com/techascent/tech.ml.dataset/blob/343f93a775975ff02704dcbaa205580fbbed3ef5/src/tech/ml/dataset.clj#L916) - Given a data-description of a
dataset create a new dataset.  This is mainly a zero copy operation so it should be
quite quick.

Near those functions you can see how easy it was to implement direct nippy support for
the dataset object itself.  Really nice, Nippy is truly a great library :-).
