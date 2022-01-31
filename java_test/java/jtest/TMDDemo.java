package jtest;


import static tech.v3.Clj.*;
import static tech.v3.TMD.*;
import tech.v3.dataset.Rolling;
import tech.v3.dataset.Modelling;
import tech.v3.DType; //access to clone method
import static tech.v3.DType.*;
import tech.v3.datatype.Pred;
import tech.v3.datatype.VecMath;
import tech.v3.datatype.Stats;
import tech.v3.datatype.Buffer;
import tech.v3.datatype.IFnDef;
import clojure.lang.RT;
import java.util.Map;
import java.util.stream.StreamSupport;



public class TMDDemo {
  public static void main(String[] args) {
    println("Loading/compiling library code.  Time here can be mitigated with a precompilation step.");
    //Front-loading requires so when the code starts to run everyting is compiled.
    //For precompilation see tech.v3.Clj.compile.
    require("tech.v3.dataset");
    require("tech.v3.dataset.neanderthal");

    println("Compilation finished.");
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

    //It is also trivial to add a virtual column by instantiating a Buffer object
    println(head(assoc(colmapDs, kw("c"), new tech.v3.datatype.LongReader() {
	public long lsize() { return 10; }
	public long readLong( long idx) {
	  return 2*idx;
	}
      })));
    //testds [5 3]:

    //|  :b | :a | :c |
    //|----:|---:|---:|
    //| 9.0 |  0 |  0 |
    //| 8.0 |  1 |  2 |
    //| 7.0 |  2 |  4 |
    //| 6.0 |  3 |  6 |
    //| 5.0 |  4 |  8 |


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


    //Filtering by a column is faster than the generalized row-by-row filter
    //and it allows us to make an assumption that if the predicate is a constant
    println(head(filterColumn(stocks, "symbol", Pred.eq("MSFT"))));
    //https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

    //| symbol |       date | price |
    //|--------|------------|------:|
    //|   MSFT | 2000-01-01 | 39.81 |
    //|   MSFT | 2000-02-01 | 36.35 |
    //|   MSFT | 2000-03-01 | 43.22 |
    //|   MSFT | 2000-04-01 | 28.37 |
    //|   MSFT | 2000-05-01 | 25.45 |

    //Grouping returns a map of key to dataset.  This can serve as a pre-aggregation
    //step or as a simple index.
    Map bySymbol = groupByColumn(stocks, "symbol");
    println(keys(bySymbol));
    //(MSFT AMZN IBM GOOG AAPL)

    //Construct a new dataset by scanning a sequence of maps.  This performs the aggregation
    //step after grouping by symbol.  There is a higher performance way of doing this
    //described later but this method is most likely sufficient for many many use
    //cases.
    println(makeDataset(map(new IFnDef() {
	public Object invoke(Object kv) {
	  Map.Entry item = (Map.Entry)kv;
	  return hashmap("symbol", item.getKey(),
			 "meanPrice", Stats.mean(column(item.getValue(), "price")));
	}}, bySymbol)));
    // _unnamed [5 2]:
    //| symbol |    meanPrice |
    //|--------|-------------:|
    //|   MSFT |  24.73674797 |
    //|   AMZN |  47.98707317 |
    //|    IBM |  91.26121951 |
    //|   GOOG | 415.87044118 |
    //|   AAPL |  64.73048780 |


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

    //Join algorithm is a fast in-memory hash-based join
    Map dsa = makeDataset(hashmap("a", vector("a", "b", "b", "a", "c"),
				  "b", range(5),
				  "c", range(5)));
    println(dsa);
    //_unnamed [5 3]:

    //| a | b | c |
    //|---|--:|--:|
    //| a | 0 | 0 |
    //| b | 1 | 1 |
    //| b | 2 | 2 |
    //| a | 3 | 3 |
    //| c | 4 | 4 |


    Map dsb = makeDataset(hashmap("a", vector("a", "b", "a", "b", "d"),
				  "b", range(5),
				  "c", range(6,11)));
    println(dsb);
    //_unnamed [5 3]:

    //| a | b |  c |
    //|---|--:|---:|
    //| a | 0 |  6 |
    //| b | 1 |  7 |
    //| a | 2 |  8 |
    //| b | 3 |  9 |
    //| d | 4 | 10 |


    //Join on the columns a,b.  Default join mode is inner
    println(join(dsa, dsb, hashmap(kw("on"), vector("a", "b"))));
    //inner-join [2 4]:

    //| a | b | c | right.c |
    //|---|--:|--:|--------:|
    //| a | 0 | 0 |       6 |
    //| b | 1 | 1 |       7 |


    //Outer join on same columns
    println(join(dsa, dsb, hashmap(kw("on"), vector("a", "b"),
				   kw("how"), kw("outer"))));
    //outer-join [8 4]:

    //| a | b | c | right.c |
    //|---|--:|--:|--------:|
    //| a | 0 | 0 |       6 |
    //| b | 1 | 1 |       7 |
    //| b | 2 | 2 |         |
    //| a | 3 | 3 |         |
    //| c | 4 | 4 |         |
    //| a | 2 |   |       8 |
    //| b | 3 |   |       9 |
    //| d | 4 |   |      10 |

    //Specific to timeseries-type information, there is a special join operator
    //named leftJoinAsof where every column of the left dataset is represented and it is
    //matched with the 'nearest' of a column of the right dataset.

    Map googPrices = sortByColumn(bySymbol.get("GOOG"), "price");
    //Print all entries
    println(head(googPrices, 200));
    //GOOG [68 3]:
    //| symbol |       date |  price |
    //|--------|------------|-------:|
    //|   GOOG | 2004-08-01 | 102.37 |
    //|   GOOG | 2004-09-01 | 129.60 |
    //|   GOOG | 2005-03-01 | 180.51 |
    //|   GOOG | 2004-11-01 | 181.98 |
    //|   GOOG | 2005-02-01 | 187.99 |
    //|   GOOG | 2004-10-01 | 190.64 |
    //|   GOOG | 2004-12-01 | 192.79 |
    //|   GOOG | 2005-01-01 | 195.62 |
    //|   GOOG | 2005-04-01 | 220.00 |
    //|   GOOG | 2005-05-01 | 277.27 |
    //|   GOOG | 2005-08-01 | 286.00 |
    //|   GOOG | 2005-07-01 | 287.76 |
    //|   GOOG | 2008-11-01 | 292.96 |
    //|   GOOG | 2005-06-01 | 294.15 |
    //|   GOOG | 2008-12-01 | 307.65 |
    //|   GOOG | 2005-09-01 | 316.46 |
    //|   GOOG | 2009-02-01 | 337.99 |
    //|   GOOG | 2009-01-01 | 338.53 |
    //|   GOOG | 2009-03-01 | 348.06 |
    //|   GOOG | 2008-10-01 | 359.36 |
    //|   GOOG | 2006-02-01 | 362.62 |
    //|   GOOG | 2006-05-01 | 371.82 |
    //|   GOOG | 2005-10-01 | 372.14 |
    //|   GOOG | 2006-08-01 | 378.53 |
    //|   GOOG | 2006-07-01 | 386.60 |
    //|   GOOG | 2006-03-01 | 390.00 |
    //|   GOOG | 2009-04-01 | 395.97 |
    //|   GOOG | 2008-09-01 | 400.52 |
    //|   GOOG | 2006-09-01 | 401.90 |
    //|   GOOG | 2005-11-01 | 404.91 |
    //|   GOOG | 2005-12-01 | 414.86 |
    //|   GOOG | 2009-05-01 | 417.23 |
    //|   GOOG | 2006-04-01 | 417.94 |
    //|   GOOG | 2006-06-01 | 419.33 |
    //|   GOOG | 2009-06-01 | 421.59 |
    //|   GOOG | 2006-01-01 | 432.66 |
    //|   GOOG | 2008-03-01 | 440.47 |
    //|   GOOG | 2009-07-01 | 443.05 |
    //|   GOOG | 2007-02-01 | 449.45 |
    //|   GOOG | 2007-03-01 | 458.16 |
    //|   GOOG | 2006-12-01 | 460.48 |
    //|   GOOG | 2009-08-01 | 461.67 |
    //|   GOOG | 2008-08-01 | 463.29 |
    //|   GOOG | 2008-02-01 | 471.18 |
    //|   GOOG | 2007-04-01 | 471.38 |
    //|   GOOG | 2008-07-01 | 473.75 |
    //|   GOOG | 2006-10-01 | 476.39 |
    //|   GOOG | 2006-11-01 | 484.81 |
    //|   GOOG | 2009-09-01 | 495.85 |
    //|   GOOG | 2007-05-01 | 497.91 |
    //|   GOOG | 2007-01-01 | 501.50 |
    //|   GOOG | 2007-07-01 | 510.00 |
    //|   GOOG | 2007-08-01 | 515.25 |
    //|   GOOG | 2007-06-01 | 522.70 |
    //|   GOOG | 2008-06-01 | 526.42 |
    //|   GOOG | 2010-02-01 | 526.80 |
    //|   GOOG | 2010-01-01 | 529.94 |
    //|   GOOG | 2009-10-01 | 536.12 |
    //|   GOOG | 2010-03-01 | 560.19 |
    //|   GOOG | 2008-01-01 | 564.30 |
    //|   GOOG | 2007-09-01 | 567.27 |
    //|   GOOG | 2008-04-01 | 574.29 |
    //|   GOOG | 2009-11-01 | 583.00 |
    //|   GOOG | 2008-05-01 | 585.80 |
    //|   GOOG | 2009-12-01 | 619.98 |
    //|   GOOG | 2007-12-01 | 691.48 |
    //|   GOOG | 2007-11-01 | 693.00 |
    //|   GOOG | 2007-10-01 | 707.00 |

    Map targetPrices = makeDataset(hashmap("price", new Double[] { 200.0, 300.0, 400.0 }));

    println(leftJoinAsof("price", targetPrices, googPrices, hashmap(kw("asof-op"), kw("<="))));
    //asof-<= [3 4]:
    //| price | symbol |       date | GOOG.price |
    //|------:|--------|------------|-----------:|
    //| 200.0 |   GOOG | 2005-04-01 |     220.00 |
    //| 300.0 |   GOOG | 2008-12-01 |     307.65 |
    //| 400.0 |   GOOG | 2008-09-01 |     400.52 |
    println(leftJoinAsof("price", targetPrices, googPrices, hashmap(kw("asof-op"), kw(">"))));
    //asof-> [3 4]:
    //| price | symbol |       date | GOOG.price |
    //|------:|--------|------------|-----------:|
    //| 200.0 |   GOOG | 2005-01-01 |     195.62 |
    //| 300.0 |   GOOG | 2005-06-01 |     294.15 |
    //| 400.0 |   GOOG | 2009-04-01 |     395.97 |


    //tech.v3.dataset.Modelling moves us more into machine learning pathways
    //We can do things like PCA transformations or train/test pathways.
    Object categoricalFit = Modelling.fitCategorical(stocks, "symbol");
    println(head(Modelling.transformCategorical(stocks, categoricalFit)));
      
    
    // If we load clojure.core.async - which neanderthal does - or we use
    // clojure.core/pmap then we have to shutdown agents else we get a 1 minute hang
    // on shutdown.    
    shutdownAgents();
  }
}
