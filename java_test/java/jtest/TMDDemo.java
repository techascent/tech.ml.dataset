package jtest;


import static tech.v3.Clj.*;
import static tech.v3.TMD.*;
import tech.v3.DType; //access to clone method
import static tech.v3.DType.*;
import tech.v3.datatype.Buffer;
import java.util.Map;


public class TMDDemo {
  public static void main(String[] args) {
    //Make dataset can take a string, inputStream, a sequence of maps or a map of columns with
    //the map of columns being the most efficient.
    //Default file formats:
    //csv, tsv, csv.gz, tsv.gz, (compressed, general, and surprisingly fast) .nippy
    Map ds = makeDataset("https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv");
    println(head(ds));
    // https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:
    // | symbol |       date | price |
    // |--------|------------|------:|
    // |   MSFT | 2000-01-01 | 39.81 |
    // |   MSFT | 2000-02-01 | 36.35 |
    // |   MSFT | 2000-03-01 | 43.22 |
    // |   MSFT | 2000-04-01 | 28.37 |
    // |   MSFT | 2000-05-01 | 25.45 |
    println(head(sortByColumn(ds, "date")));
    // https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

    // | symbol |       date |  price |
    // |--------|------------|-------:|
    // |   AAPL | 2000-01-01 |  25.94 |
    // |    IBM | 2000-01-01 | 100.52 |
    // |   MSFT | 2000-01-01 |  39.81 |
    // |   AMZN | 2000-01-01 |  64.56 |
    // |   AAPL | 2000-02-01 |  28.66 |
    println(ds.get("date"));
    // #tech.v3.dataset.column<packed-local-date>[560]
    // date
    // [2000-01-01, 2000-02-01, 2000-03-01, 2000-04-01, 2000-05-01, 2000-06-01, 2000-07-01, 2000-08-01, 2000-09-01, 2000-10-01, 2000-11-01, 2000-12-01, 2001-01-01, 2001-02-01, 2001-03-01, 2001-04-01, 2001-05-01, 2001-06-01, 2001-07-01, 2001-08-01...]

    Object priceCol = ds.get("price");
    println("first value:", call(priceCol, 0), ", last value:", call(priceCol, -1));
    //first value: 39.81 , last value: 223.02

    Map colmapDs = makeDataset(hashmap(kw("a"), range(10),
				       kw("b"), toDoubleArray(range(9,-1,-1))),
			       hashmap(kw("dataset-name"), "testds"));
    println(colmapDs);
    // testds [10 2]:

    // |  :b | :a |
    // |----:|---:|
    // | 9.0 |  0 |
    // | 8.0 |  1 |
    // | 7.0 |  2 |
    // | 6.0 |  3 |
    // | 5.0 |  4 |
    // | 4.0 |  5 |
    // | 3.0 |  6 |
    // | 2.0 |  7 |
    // | 1.0 |  8 |
    // | 0.0 |  9 |

    println(meta(colmapDs));
    // {:name testds}


    println(meta(call(colmapDs, kw("a"))), meta(call(colmapDs, kw("b"))));
    // {:name :a, :datatype :int64, :n-elems 10} {:name :b, :datatype :float64, :n-elems 10}

    Buffer rows = rows(colmapDs);
    println("First row:", call(rows,0), ", last row:", call(rows,-1));
    // First row: {:b 9.0, :a 0} , last row: {:b 0.0, :a 9}

    Buffer rowvecs = rowvecs(colmapDs);
    println("First rowvec:", call(rowvecs,0), ", last rowvec:", call(rowvecs,-1));
    // First rowvec: [9.0 0] , last rowvec: [0.0 9]
    shutdownAgents();
  }
}
