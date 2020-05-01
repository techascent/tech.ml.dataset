# tech.ml.dataset


[![Clojars Project](https://img.shields.io/clojars/v/techascent/tech.ml.dataset.svg)](https://clojars.org/techascent/tech.ml.dataset)


`tech.ml.dataset` is a Clojure library for data processing and machine learning.  Datasets are
currently in-memory columnwise databases and we support parsing from file or
input-stream.  We support these formats: **raw/gzipped csv/tsv, xls, xlsx, json,
and sequences of maps** as input sources.

Data storage is efficient (primitive arrays), datetime types are often converted to
an integer representation and strings are loaded into string tables.  These features
together dramatically decrease the working set size in memory.  Because data is stored
in columnar fashion columnwise operations on the dataset are very fast.

Conversion back into sequences of maps is very efficient and we have support for
writing the dataset back out to csv, tsv, and gzipped varieties of those.

## Mini Walkthrough

```clojure
user> (require '[tech.ml.dataset :as ds])
nil
;; We support many file formats
user> (def csv-data (ds/->dataset "test/data/stocks.csv"))
#'user/csv-data
user> (ds/head csv-data)
test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------+------------+-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
user> (def xls-data (ds/->dataset "test/data/file_example_XLS_1000.xls"))
#'user/xls-data
user> (ds/head xls-data)
Sheet1 [5 8]:

|     0 | First Name | Last Name | Gender |       Country |   Age |       Date |   Id |
|-------+------------+-----------+--------+---------------+-------+------------+------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.00 | 15/10/2017 | 1562 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.00 | 16/08/2016 | 1582 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.00 | 21/05/2015 | 2587 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.00 | 15/10/2017 | 3549 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.00 | 16/08/2016 | 2468 |

;;And you can have fine grained control over parsing

user> (ds/head (ds/->dataset "test/data/file_example_XLS_1000.xls"
                             {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]}}))
Sheet1 [5 8]:

|     0 | First Name | Last Name | Gender |       Country |   Age |       Date |   Id |
|-------+------------+-----------+--------+---------------+-------+------------+------|
| 1.000 |      Dulce |     Abril | Female | United States | 32.00 | 2017-10-15 | 1562 |
| 2.000 |       Mara | Hashimoto | Female | Great Britain | 25.00 | 2016-08-16 | 1582 |
| 3.000 |     Philip |      Gent |   Male |        France | 36.00 | 2015-05-21 | 2587 |
| 4.000 |   Kathleen |    Hanner | Female | United States | 25.00 | 2017-10-15 | 3549 |
| 5.000 |    Nereida |   Magwood | Female | United States | 58.00 | 2016-08-16 | 2468 |
user>


;;Loading from the web is no problem
user> (def airports (ds/->dataset "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat" 
                                  {:header-row? false}))
#'user/airports
user> (ds/head airports)
https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat [5 14]:

| 0 |                                           1 |            2 |                3 |   4 |    5 |      6 |     7 |    8 |    9 | 10 |                   11 |      12 |          13 |
|---+---------------------------------------------+--------------+------------------+-----+------+--------+-------+------+------+----+----------------------+---------+-------------|
| 1 |                              Goroka Airport |       Goroka | Papua New Guinea | GKA | AYGA | -6.082 | 145.4 | 5282 | 10.0 |  U | Pacific/Port_Moresby | airport | OurAirports |
| 2 |                              Madang Airport |       Madang | Papua New Guinea | MAG | AYMD | -5.207 | 145.8 |   20 | 10.0 |  U | Pacific/Port_Moresby | airport | OurAirports |
| 3 |                Mount Hagen Kagamuga Airport |  Mount Hagen | Papua New Guinea | HGU | AYMH | -5.827 | 144.3 | 5388 | 10.0 |  U | Pacific/Port_Moresby | airport | OurAirports |
| 4 |                              Nadzab Airport |       Nadzab | Papua New Guinea | LAE | AYNZ | -6.570 | 146.7 |  239 | 10.0 |  U | Pacific/Port_Moresby | airport | OurAirports |
| 5 | Port Moresby Jacksons International Airport | Port Moresby | Papua New Guinea | POM | AYPY | -9.443 | 147.2 |  146 | 10.0 |  U | Pacific/Port_Moresby | airport | OurAirports |

;;At any point you can get a sequence of maps back.  We implement a special version
;;of Clojure's APersistentMap that is much more efficient than even records and shares
;;the backing store with the dataset.

user> (take 2 (ds/mapseq-reader csv-data))
({"date" #object[java.time.LocalDate 0x4a998af0 "2000-01-01"],
  "symbol" "MSFT",
  "price" 39.81}
 {"date" #object[java.time.LocalDate 0x6d8c0bcd "2000-02-01"],
  "symbol" "MSFT",
  "price" 36.35})


;;Data is stored in primitive arrays (even most datetimes!) and strings are stored
;;in string tables.  You can load really large datasets with this thing!

;;Datasets are sequence of columns.  Dataset and columns implement the clojure metdata
;;interfaces.
user> (->> csv-data
           (map (fn [column]
                  (meta column))))
({:categorical? true, :name "symbol", :size 560, :datatype :string}
 {:name "date", :size 560, :datatype :packed-local-date}
 {:name "price", :size 560, :datatype :float32})

 ;;Columns themselves are sequences of their entries.
user> (csv-data "symbol")
#tech.ml.dataset.column<string>[560]
symbol
[MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, ...]
user> (xls-data "Gender")
#tech.ml.dataset.column<string>[1000]
Gender
[Female, Female, Male, Female, Female, Male, Female, Female, Female, Female, Female, Male, Female, Male, Female, Female, Female, Female, Female, Female, ...]
user> (take 5 (xls-data "Gender"))
("Female" "Female" "Male" "Female" "Female")

;;We can get a brief description of the dataset:

user> (ds/brief csv-data)
({:min #object[java.time.LocalDate 0x60dcad01 "2000-01-01"],
  :col-name "date",
  :max #object[java.time.LocalDate 0x5fbc8662 "2010-03-01"],
  :n-missing 0,
  :mean #object[java.time.LocalDate 0x3d13058c "2005-05-12"],
  :datatype :packed-local-date,
  :n-valid 560}
 {:min 5.96999979019165,
  :n-missing 0,
  :col-name "price",
  :mean 100.73428564752851,
  :datatype :float32,
  :skew 2.4130946312809254,
  :standard-deviation 132.55477064785,
  :n-valid 560,
  :max 707.0}
 {:col-name "symbol",
  :mode "MSFT",
  :n-missing 0,
  :values ["MSFT" "AMZN" "IBM" "AAPL" "GOOG"],
  :n-values 5,
  :datatype :string,
  :n-valid 560})

;;Another view of that brief:


user> (ds/descriptive-stats csv-data)
test/data/stocks.csv: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |      :mean | :mode |       :min |       :max | :standard-deviation | :skew |
|-----------+--------------------+----------+------------+------------+-------+------------+------------+---------------------+-------|
|      date | :packed-local-date |      560 |          0 | 2005-05-12 |       | 2000-01-01 | 2010-03-01 |                     |       |
|     price |           :float32 |      560 |          0 |      100.7 |       |      5.970 |      707.0 |               132.6 | 2.413 |
|    symbol |            :string |      560 |          0 |            |  MSFT |            |            |                     |       |


;;There are analogues of the clojure.core functions that apply to dataset:
;;filter, group-by, sort-by.  These are all implemented efficiently.

;;You can add/remove/update columns
;;You can write out the result back to csv, tsv, and gzipped variations of those.

;;Joins (left, right, inner) are all implemented.

;;Columnwise arithmetic manipulations are provided via the
;;tech.v2.datatype.functional namespace.

;;There is much more.  Please checkout the walkthough and try it out!
```

## More Documentation

* Quick code-oriented [walkthrough](docs/walkthrough.md)
* [Summary of Functions](https://github.com/genmeblog/techtest/wiki/Summary-of-functions)
* [Comparison](https://github.com/genmeblog/techtest/blob/master/src/techtest/datatable_dplyr.clj) between R's `data.table`, R's `dplyr`, and `tech.ml.dataset`


## Questions, Community
* [zulip stream](https://clojurians.zulipchat.com/#narrow/stream/236259-tech.2Eml.2Edataset.2Edev)
* [slack (data science channel)](https://clojurians.slack.com/archives/C0BQDEJ8M)


## Examples

* [sequences of maps](test/tech/ml/dataset/mapseq_test.clj)
* [regression pipelines](test/tech/ml/dataset/ames_test.clj)
* [tech.v2.datatype](https://github.com/techascent/tech.datatype) numeric subsystem
   which is described on our [blog](http://techascent.com/blog/datatype-library.html)
* [datatype cheatsheet](https://github.com/techascent/tech.datatype/blob/master/docs/cheatsheet.md)


## Keywords
 - csv, xlsx, pandas, dataframe, dplyr, data.table, modelling 


## License

Copyright © 2019 Complements of TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
