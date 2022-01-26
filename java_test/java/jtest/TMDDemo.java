package jtest;


import static tech.v3.Clj.*;
import static tech.v3.TMD.*;
import tech.v3.datatype.VecMath;
import tech.v3.DType; //access to clone method
import static tech.v3.DType.*;
import tech.v3.dataset.Rolling;
import tech.v3.datatype.Buffer;
import java.util.Map;


public class TMDDemo {
  public static void main(String[] args) {
    //Front-loading requires so when the code starts to run everyting is compiled.
    require("tech.v3.dataset");
    require("tech.v3.dataset.neanderthal");
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

    println("Tensor format:", toTensor(colmapDs));
    // Tensor format: #tech.v3.tensor<float64>[10 2]
    // [[9.000 0.000]
    //  [8.000 1.000]
    //  [7.000 2.000]
    //  [6.000 3.000]
    //  [5.000 4.000]
    //  [4.000 5.000]
    //  [3.000 6.000]
    //  [2.000 7.000]
    //  [1.000 8.000]
    //  [0.000 9.000]]

    println("Neanderthal format:", toNeanderthal(colmapDs));
    //Neanderthal format: #RealGEMatrix[double, mxn:10x2, layout:column, offset:0]
    //   ▥       ↓       ↓       ┓
    //   →       9.00    0.00
    //   →       8.00    1.00
    //   →       ⁙       ⁙
    //   →       1.00    8.00
    //   →       0.00    9.00
    //   ┗                       ┛

    Map stocks = makeDataset("https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv");
    //Variable rolling window reductions require the target column to be monotonically
    //increasing - for each val x(n), x(n+1) is greater or equal.
    stocks = sortByColumn(stocks, "date");
    println(head(stocks));
    // https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:
    // | symbol |       date |  price |
    // |--------|------------|-------:|
    // |   AAPL | 2000-01-01 |  25.94 |
    // |    IBM | 2000-01-01 | 100.52 |
    // |   MSFT | 2000-01-01 |  39.81 |
    // |   AMZN | 2000-01-01 |  64.56 |
    // |   AAPL | 2000-02-01 |  28.66 |


    Map variableWin = Rolling.rolling(stocks,
				      Rolling.variableWindow("date", 3, kw("months")),
				      hashmap("price-mean-3m", Rolling.mean("price"),
					      "price-max-3m", Rolling.max("price"),
					      "price-min-3m", Rolling.min("price")));
    println(head(variableWin, 10));
    //https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [10 6]:
    //| symbol |       date |  price | price-max-3m | price-mean-3m | price-min-3m |
    //|--------|------------|-------:|-------------:|--------------:|-------------:|
    //|   AAPL | 2000-01-01 |  25.94 |       106.11 |   58.92500000 |        25.94 |
    //|    IBM | 2000-01-01 | 100.52 |       106.11 |   61.92363636 |        28.66 |
    //|   MSFT | 2000-01-01 |  39.81 |       106.11 |   58.06400000 |        28.66 |
    //|   AMZN | 2000-01-01 |  64.56 |       106.11 |   60.09222222 |        28.66 |
    //|   AAPL | 2000-02-01 |  28.66 |       106.11 |   57.56583333 |        28.37 |
    //|   MSFT | 2000-02-01 |  36.35 |       106.11 |   60.19363636 |        28.37 |
    //|    IBM | 2000-02-01 |  92.11 |       106.11 |   62.57800000 |        28.37 |
    //|   AMZN | 2000-02-01 |  68.87 |       106.11 |   59.29666667 |        28.37 |
    //|   AMZN | 2000-03-01 |  67.00 |       106.11 |   54.65583333 |        21.00 |
    //|   MSFT | 2000-03-01 |  43.22 |       106.11 |   53.53363636 |        21.00 |

    //Create a vector from 0->6*PI in 90 increments.
    Object radians = VecMath.mul(2.0*Math.PI, VecMath.div(range(33), 32.0));
    Map sinds = makeDataset(hashmap("radians", radians, "sin", VecMath.sin(radians)));
    Map fixedWin = Rolling.rolling(sinds,
				   Rolling.fixedWindow(4),
				   hashmap("sin-roll-mean", Rolling.mean("sin"),
					   "sin-roll-max", Rolling.max("sin"),
					   "sin-roll-min", Rolling.min("sin")));
    println(head(fixedWin, 8));
    //_unnamed [8 5]:

    //|        sin |    radians | sin-roll-max | sin-roll-min | sin-roll-mean |
    //|-----------:|-----------:|-------------:|-------------:|--------------:|
    //| 0.00000000 | 0.00000000 |   0.19509032 |   0.00000000 |    0.04877258 |
    //| 0.19509032 | 0.19634954 |   0.38268343 |   0.00000000 |    0.14444344 |
    //| 0.38268343 | 0.39269908 |   0.55557023 |   0.00000000 |    0.28333600 |
    //| 0.55557023 | 0.58904862 |   0.70710678 |   0.19509032 |    0.46011269 |
    //| 0.70710678 | 0.78539816 |   0.83146961 |   0.38268343 |    0.61920751 |
    //| 0.83146961 | 0.98174770 |   0.92387953 |   0.55557023 |    0.75450654 |
    //| 0.92387953 | 1.17809725 |   0.98078528 |   0.70710678 |    0.86081030 |
    //| 0.98078528 | 1.37444679 |   1.00000000 |   0.83146961 |    0.93403361 |
    shutdownAgents();
  }
}
