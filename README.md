# tech.ml.dataset


[![Clojars Project](https://img.shields.io/clojars/v/techascent/tech.ml.dataset.svg)](https://clojars.org/techascent/tech.ml.dataset)


* [API Documentation](https://techascent.github.io/tech.ml.dataset/)


`tech.ml.dataset` is a Clojure library for data processing and machine learning.  Datasets are
currently in-memory columnwise databases and we support parsing from file or
input-stream.  We support these formats: **raw/gzipped csv/tsv, xls, xlsx, json,
and sequences of maps** as input sources.  [SQL bindings](https://github.com/techascent/tech.ml.dataset.sql)
are provided as a separate library. We also support [efficient conversion](src/tech/libs/smile/data.clj)
to/from smile DataFrames and thus we have transitive support for Apache Arrow and Parquet files.  Load
a DataFrame and then call ->dataset on the dataframe :-).

Data size in memory is [minimized](https://gist.github.com/cnuernber/26b88ed259dd1d0dc6ac2aa138eecf37)
(primitive arrays), datetime types are often converted to an integer representation
and strings are loaded into string tables.  These features together dramatically
decrease the working set size in memory.  Because data is stored in columnar fashion
columnwise operations on the dataset are very fast.

Conversion back into sequences of maps is very efficient and we have support for
writing the dataset back out to csv, tsv, and gzipped varieties of those.

Upgraded support for [Apache Arrow](src/tech.libs/arrow.clj).  We support copying pathway using
the standard api -- data is copied from disk into buffers.  We also
support a more or less [from-scratch implementation](src/tech/libs/arrow/in_place.clj) of an in-place
pathway built expressly to enable both datasets that are larger than machine
RAM and purely for performance on top of the
['tech.v3.datatype.mmap'](https://github.com/cnuernber/dtype-next/blob/152f09f925041d41782e05009bbf84d7d6cfdbc6/src/tech/v3/datatype/mmap.clj#L16)
namespace.

An alternative cutting-edge api with some important extra features is available via [tablecloth](https://github.com/scicloj/tablecloth).





## Mini Walkthrough

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
;; We support many file formats
user> (def csv-data (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"))
#'user/csv-data
user> (ds/head csv-data)
test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------|------------|-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |

user> (require '[tech.v3.libs.poi])
nil
user> (def xls-data (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"))
#'user/xls-data
user> (ds/head xls-data)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/file_example_XLS_1000.xls [5 8]:

| column-0 | First Name | Last Name | Gender |       Country |  Age |       Date |     Id |
|----------|------------|-----------|--------|---------------|------|------------|--------|
|      1.0 |      Dulce |     Abril | Female | United States | 32.0 | 15/10/2017 | 1562.0 |
|      2.0 |       Mara | Hashimoto | Female | Great Britain | 25.0 | 16/08/2016 | 1582.0 |
|      3.0 |     Philip |      Gent |   Male |        France | 36.0 | 21/05/2015 | 2587.0 |
|      4.0 |   Kathleen |    Hanner | Female | United States | 25.0 | 15/10/2017 | 3549.0 |
|      5.0 |    Nereida |   Magwood | Female | United States | 58.0 | 16/08/2016 | 2468.0 |

;;And you have fine grained control over parsing

user> (ds/head (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/file_example_XLS_1000.xls"
                             {:parser-fn {"Date" [:local-date "dd/MM/yyyy"]}}))
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/file_example_XLS_1000.xls [5 8]:

| column-0 | First Name | Last Name | Gender |       Country |  Age |       Date |     Id |
|----------|------------|-----------|--------|---------------|------|------------|--------|
|      1.0 |      Dulce |     Abril | Female | United States | 32.0 | 2017-10-15 | 1562.0 |
|      2.0 |       Mara | Hashimoto | Female | Great Britain | 25.0 | 2016-08-16 | 1582.0 |
|      3.0 |     Philip |      Gent |   Male |        France | 36.0 | 2015-05-21 | 2587.0 |
|      4.0 |   Kathleen |    Hanner | Female | United States | 25.0 | 2017-10-15 | 3549.0 |
|      5.0 |    Nereida |   Magwood | Female | United States | 58.0 | 2016-08-16 | 2468.0 |
user>


;;Loading from the web is no problem
user>
user> (def airports (ds/->dataset "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
                                  {:header-row? false :file-type :csv}))
#'user/airports
user> (ds/head airports)
https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat [5 14]:

| column-0 |                                    column-1 |     column-2 |         column-3 | column-4 | column-5 |    column-6 |     column-7 | column-8 | column-9 | column-10 |            column-11 | column-12 |   column-13 |
|----------|---------------------------------------------|--------------|------------------|----------|----------|-------------|--------------|----------|----------|-----------|----------------------|-----------|-------------|
|        1 |                              Goroka Airport |       Goroka | Papua New Guinea |      GKA |     AYGA | -6.08168983 | 145.39199829 |     5282 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        2 |                              Madang Airport |       Madang | Papua New Guinea |      MAG |     AYMD | -5.20707989 | 145.78900147 |       20 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        3 |                Mount Hagen Kagamuga Airport |  Mount Hagen | Papua New Guinea |      HGU |     AYMH | -5.82678986 | 144.29600525 |     5388 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        4 |                              Nadzab Airport |       Nadzab | Papua New Guinea |      LAE |     AYNZ | -6.56980300 | 146.72597700 |      239 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |
|        5 | Port Moresby Jacksons International Airport | Port Moresby | Papua New Guinea |      POM |     AYPY | -9.44338036 | 147.22000122 |      146 |     10.0 |         U | Pacific/Port_Moresby |   airport | OurAirports |

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

;;Datasets are comprised of named columns, and provide a Clojure hashmap-compatible
;;collection.  Datasets allow reading and updating column data associated with a column name,
;;and provide a sequential view of [column-name column] entries.

;;You can look up columns via `get`, keyword lookup, and invoking the dataset as a function on
;;a key (a column name). `keys` and `vals` retrieve respective sequences of column names and columns.
;;The functions `assoc` and `dissoc` work to define new associations to conveniently
;;add, update, or remove columns, with add/update semantics defined by`tech.v3.dataset/add-or-update-column`.

;;Column data is stored in primitive arrays (even most datetimes!) and strings are stored
;;in string tables.  You can load really large datasets with this thing!

;;Columns themselves are sequences of their entries.
user> (csv-data "symbol")
#tech.v3.dataset.column<string>[560]
symbol
[MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, ...]
user> (xls-data "Gender")
#tech.v3.dataset.column<string>[1000]
Gender
[Female, Female, Male, Female, Female, Male, Female, Female, Female, Female, Female, Male, Female, Male, Female, Female, Female, Female, Female, Female, ...]
user> (take 5 (xls-data "Gender"))
("Female" "Female" "Male" "Female" "Female")


;;Datasets and columns implement the clojure metadata interfaces (`meta`, `with-meta`, `vary-meta`)

;;You can access a sequence of columns of a dataset with `ds/columns`, or `vals` like a map,
;;and access the metadata with `meta`:

user> (->> csv-data
           vals  ;synonymous with ds/columns
           (map (fn [column]
                  (meta column))))
({:categorical? true, :name "symbol", :size 560, :datatype :string}
 {:name "date", :size 560, :datatype :packed-local-date}
 {:name "price", :size 560, :datatype :float32})

;;We can similarly destructure datasets like normal clojure
;;maps:

user> (for [[k column] csv-data]
        [k (meta column)])
(["symbol" {:categorical? true, :name "symbol", :size 560, :datatype :string}]
 ["date" {:name "date", :size 560, :datatype :packed-local-date}]
 ["price" {:name "price", :size 560, :datatype :float64}])

user> (let [{:strs [symbol date]} csv-data]
        [symbol (meta date)])
[#tech.v3.dataset.column<string>[560]
symbol
[MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, MSFT, ...]
 {:name "date", :size 560, :datatype :packed-local-date}]

;;We can get a brief description of the dataset:

user> (ds/brief csv-data)
({:min #object[java.time.LocalDate 0x5b2ea1d5 "2000-01-01"],
  :n-missing 0,
  :col-name "date",
  :mean #object[java.time.LocalDate 0x729b7395 "2005-05-12"],
  :datatype :packed-local-date,
  :quartile-3 #object[java.time.LocalDate 0x6c75fa43 "2007-11-23"],
  :n-valid 560,
  :quartile-1 #object[java.time.LocalDate 0x13d9aabe "2002-11-08"],
  :max #object[java.time.LocalDate 0x493bf7ef "2010-03-01"]}
 {:min 5.97,
  :n-missing 0,
  :col-name "price",
  :mean 100.7342857142857,
  :datatype :float64,
  :skew 2.4130946430619233,
  :standard-deviation 132.55477114107083,
  :quartile-3 100.88,
  :n-valid 560,
  :quartile-1 24.169999999999998,
  :max 707.0}
 {:mode "MSFT",
  :values ["MSFT" "AMZN" "IBM" "AAPL" "GOOG"],
  :n-values 5,
  :n-valid 560,
  :col-name "symbol",
  :n-missing 0,
  :datatype :string,
  :histogram (["MSFT" 123] ["AMZN" 123] ["IBM" 123] ["AAPL" 123] ["GOOG" 68])})

;;Another view of that brief:

user> (ds/descriptive-stats csv-data)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv: descriptive-stats [3 10]:

| :col-name |          :datatype | :n-valid | :n-missing |       :min |      :mean | :mode |       :max | :standard-deviation |      :skew |
|-----------|--------------------|----------|------------|------------|------------|-------|------------|---------------------|------------|
|      date | :packed-local-date |      560 |          0 | 2000-01-01 | 2005-05-12 |       | 2010-03-01 |                     |            |
|     price |           :float64 |      560 |          0 |      5.970 |      100.7 |       |      707.0 |        132.55477114 | 2.41309464 |
|    symbol |            :string |      560 |          0 |            |            |  MSFT |            |                     |            |


;;There are analogues of the clojure.core functions that apply to dataset:
;;filter, group-by, sort-by.  These are all implemented efficiently.

;;You can add/remove/update columns, or use the map idioms of `assoc` and `dissoc`

user> (-> csv-data
          (assoc "always-ten" 10) ;scalar values are expanded as needed
          (assoc "random"   (repeatedly (ds/row-count csv-data) #(rand-int 100)))
          ds/head)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv [5 5]:

| symbol |       date | price | always-ten | random |
|--------|------------|-------|------------|--------|
|   MSFT | 2000-01-01 | 39.81 |         10 |     47 |
|   MSFT | 2000-02-01 | 36.35 |         10 |     35 |
|   MSFT | 2000-03-01 | 43.22 |         10 |     54 |
|   MSFT | 2000-04-01 | 28.37 |         10 |      6 |
|   MSFT | 2000-05-01 | 25.45 |         10 |     52 |

user> (-> csv-data
          (dissoc "price")
          ds/head)
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv [5 2]:

| symbol |       date |
|--------|------------|
|   MSFT | 2000-01-01 |
|   MSFT | 2000-02-01 |
|   MSFT | 2000-03-01 |
|   MSFT | 2000-04-01 |
|   MSFT | 2000-05-01 |


;;since `conj` works as with clojure maps and sequences of map-entries or pairs,
;;you can use idioms like `reduce conj` or `into` to construct new datasets on the
;;fly with familiar clojure idioms:

user> (let [new-cols [["always-ten" 10] ["new-price" (map inc (csv-data "price"))]]
            new-data (into (dissoc csv-data "price") new-cols)]
            (ds/head new-data))
https://github.com/techascent/tech.v3.dataset/raw/master/test/data/stocks.csv [5 4]:

| symbol |       date | always-ten | new-price |
|--------|------------|------------|-----------|
|   MSFT | 2000-01-01 |         10 |     40.81 |
|   MSFT | 2000-02-01 |         10 |     37.35 |
|   MSFT | 2000-03-01 |         10 |     44.22 |
|   MSFT | 2000-04-01 |         10 |     29.37 |
|   MSFT | 2000-05-01 |         10 |     26.45 |

;;You can write out the result back to csv, tsv, and gzipped variations of those.

;;Joins (left, right, inner) are all implemented.

;;Columnwise arithmetic manipulations (+,-, and many more) are provided via the
;;tech.v2.datatype.functional namespace.

;;Datetime columns can be operated on - plus,minus, get-years, get-days, and
;;many more - uniformly via the tech.v2.datatype.datetime.operations namespace.

;;There is much more.  Please checkout the walkthough and try it out!
```

### Arrow Dependencies

```clojure
;; project-clj -

[org.apache.arrow/arrow-memory-netty "1.0.0"]
[org.apache.arrow/arrow-memory-core "1.0.0"]
[org.apache.arrow/arrow-vector "1.0.0" :exclusions [commons-codec]]

;;require -
(require '[tech.v3.libs.arrow])
```

### Parquet Support

This support comes in via the smile pathway and thus there is currently not great
support for missing values for those two formats.  You will need to rescan the data
most likely to know where the missing values lie.

#### Parquet Dependencies

```clojure
org.apache.parquet/parquet-hadoop {:mvn/version "1.10.1"}
org.apache.hadoop/hadoop-common {:mvn/version "3.1.1"}
```


## More Documentation

* Code-oriented [walkthrough](topics/walkthrough.md) and [quick reference](topics/quick-reference.md).
* [Comparison](https://github.com/genmeblog/techtest/blob/master/src/techtest/datatable_dplyr.clj) between R's `data.table`, R's `dplyr`, and an older version of `tech.v3.dataset`
* [Summary of Comparison Functions](https://github.com/genmeblog/techtest/wiki/Summary-of-functions)
* [Simple Data Exploration Example](https://github.com/cnuernber/simpledata)


## Questions, Community

* [zulip stream](https://clojurians.zulipchat.com/#narrow/stream/236259-tech.2Eml.2Edataset.2Edev)
* [slack (data science channel)](https://clojurians.slack.com/archives/C0BQDEJ8M)


## Further Reading

* [sequences of maps](test/tech/ml/dataset/mapseq_test.clj)
* [regression pipelines](test/tech/ml/dataset/ames_test.clj)
* [tech.v3.datatype](https://github.com/cnuernber/dtype-next) numeric subsystem
* [datatype cheatsheet](https://github.com/techascent/tech.datatype/blob/master/topics/cheatsheet.md)


## Keywords
 - csv, xlsx, pandas, dataframe, dplyr, data.table, modelling


## License

Copyright © 2019 Complements of TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
