package jtest;


import static tech.v3.Clj.*;
import static tech.v3.TMD.*;
import tech.v3.dataset.Rolling;
import tech.v3.dataset.Modelling;
import tech.v3.dataset.Reductions;
import tech.v3.libs.Arrow;
import tech.v3.libs.Parquet;
import tech.v3.DType; //access to clone method
import static tech.v3.DType.*;
import tech.v3.datatype.Pred;
import tech.v3.datatype.VecMath;
import tech.v3.datatype.Stats;
import tech.v3.datatype.Buffer;
import tech.v3.libs.Nippy;
import tech.v3.datatype.IFnDef;
//Fast map creation when you know you will have to create many maps.
import tech.v3.dataset.FastStruct;
import clojure.lang.RT;
import clojure.lang.IFn;
import java.util.Map;
import java.util.function.Function;

//Imports for the advanced reduction example at the end.
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import java.util.function.BiFunction;
import java.util.function.BiConsumer;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Random;



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

    Map colmapDs = makeDataset(hashmap("a", range(10),
				       "b", toDoubleArray(range(9,-1,-1))),
			       hashmap(kw("dataset-name"), "testds"));
    println(colmapDs);
    // testds [10 2]:

    // |  b  |  a |
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
    //One thing to note is that colmapDs itself wasn't changed.  Assoc create a new
    //dataset that shared the unchanged portions with the original dataset
    println(assoc(colmapDs, "c", new tech.v3.datatype.LongReader() {
	public long lsize() { return 10; }
	public long readLong( long idx) {
	  return 2*idx;
	}
      }));
    //testds [5 3]:

    //|  b  | a  | c  |
    //|----:|---:|---:|
    //| 9.0 |  0 |  0 |
    //| 8.0 |  1 |  2 |
    //| 7.0 |  2 |  4 |
    //| 6.0 |  3 |  6 |
    //| 5.0 |  4 |  8 |


    // The metadata on columns has quite a bit of useful informatio in it.
    println(meta(call(colmapDs, "a")), meta(call(colmapDs, "b")));
    // {:name a, :datatype :int64, :n-elems 10} {:name b, :datatype :float64, :n-elems 10}

    Buffer rows = rows(colmapDs);
    println("First row:", call(rows,0), ", last row:", call(rows,-1));
    // First row: {b 9.0, a 0} , last row: {b 0.0, a 9}

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
    //increasing - for each val x(n), x(n+1) is greater or equal.  So for financial data
    //this usually means ordered by date.
    Map goog = sortByColumn(bySymbol.get("GOOG"), "date");
    println(head(goog));
    //GOOG [5 3]:

    //| symbol |       date |  price |
    //|--------|------------|-------:|
    //|   GOOG | 2004-08-01 | 102.37 |
    //|   GOOG | 2004-09-01 | 129.60 |
    //|   GOOG | 2004-10-01 | 190.64 |
    //|   GOOG | 2004-11-01 | 181.98 |
    //|   GOOG | 2004-12-01 | 192.79 |

    //If we want our column of dates to be in epoch-days which is a lot more friendly to
    //machine learning we can easily do so:
    Buffer dateBuf = toBuffer(column(goog, "date"));
    //There are many ways to do this but here is a low-level way
    println(head(assoc(goog, "date",
		       //all integer types funnel through LongBuffer/LongReader pathways.
		       new tech.v3.datatype.LongReader() {
			 //Aside from :int32, kw("epoch-days") is another valid datatype for
			 //precisely this data.
			 public Object elemwiseDatatype() { return int32; }
			 public long lsize() { return dateBuf.lsize(); }
			 public long readLong(long idx) {
			   LocalDate ld = (LocalDate)dateBuf.readObject(idx);
			   //Missing values will be null when using the readObject pathway.
			   //The stocks dataset has no missing values.  We strongly encourage
			   //you to deal with missing values before getting into your
			   //pipeline processing pathways.
			   return ld.toEpochDay();
			 }
		       })));
    //GOOG [5 3]:

    //| symbol |  date |  price |
    //|--------|------:|-------:|
    //|   GOOG | 12631 | 102.37 |
    //|   GOOG | 12662 | 129.60 |
    //|   GOOG | 12692 | 190.64 |
    //|   GOOG | 12723 | 181.98 |
    //|   GOOG | 12753 | 192.79 |



    Map variableWin = Rolling.rolling(goog,
				      Rolling.variableWindow("date", 3, kw("months")),
				      hashmap("price-mean-3m", Rolling.mean("price"),
					      "price-max-3m", Rolling.max("price"),
					      "price-min-3m", Rolling.min("price")));
    println(head(variableWin, 10));
    //GOOG [10 6]:

    //| symbol |       date |  price | price-max-3m | price-mean-3m | price-min-3m |
    //|--------|------------|-------:|-------------:|--------------:|-------------:|
    //|   GOOG | 2004-08-01 | 102.37 |       190.64 |  140.87000000 |       102.37 |
    //|   GOOG | 2004-09-01 | 129.60 |       190.64 |  167.40666667 |       129.60 |
    //|   GOOG | 2004-10-01 | 190.64 |       192.79 |  188.47000000 |       181.98 |
    //|   GOOG | 2004-11-01 | 181.98 |       195.62 |  190.13000000 |       181.98 |
    //|   GOOG | 2004-12-01 | 192.79 |       195.62 |  192.13333333 |       187.99 |
    //|   GOOG | 2005-01-01 | 195.62 |       195.62 |  188.04000000 |       180.51 |

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


    //Single column join doesn't require column names wrapped in vectors
    println(join(dsa, dsb, hashmap(kw("on"), "a")));
    //inner-join [8 5]:

    //| a | b | c | right.b | right.c |
    //|---|--:|--:|--------:|--------:|
    //| a | 0 | 0 |       0 |       6 |
    //| a | 3 | 3 |       0 |       6 |
    //| b | 1 | 1 |       1 |       7 |
    //| b | 2 | 2 |       1 |       7 |
    //| a | 0 | 0 |       2 |       8 |
    //| a | 3 | 3 |       2 |       8 |
    //| b | 1 | 1 |       3 |       9 |
    //| b | 2 | 2 |       3 |       9 |


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

    Map targetPrices = makeDataset(hashmap("price", new Double[] { 200.0, 300.0, 400.0 }));

    println(leftJoinAsof("price", targetPrices, goog, hashmap(kw("asof-op"), kw("<="))));
    //asof-<= [3 4]:
    //| price | symbol |       date | GOOG.price |
    //|------:|--------|------------|-----------:|
    //| 200.0 |   GOOG | 2005-04-01 |     220.00 |
    //| 300.0 |   GOOG | 2008-12-01 |     307.65 |
    //| 400.0 |   GOOG | 2008-09-01 |     400.52 |
    println(leftJoinAsof("price", targetPrices, goog, hashmap(kw("asof-op"), kw(">"))));
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
    //https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 3]:

    //| symbol |       date |  price |
    //|-------:|------------|-------:|
    //|    1.0 | 2000-01-01 |  25.94 |
    //|    4.0 | 2000-01-01 | 100.52 |
    //|    3.0 | 2000-01-01 |  39.81 |
    //|    2.0 | 2000-01-01 |  64.56 |
    //|    1.0 | 2000-02-01 |  28.66 |


    //Remember the rolling sinewave dataset from before?
    //let's run PCA on the dataset.
    //This pathway will use the slightly slow covariance based method that has the distinct
    //advantage of producing accurate variances in the eigenvalues member.

    Object pcaFit = Modelling.fitPCA(fixedWin, hashmap(kw("n-components"), 2));
    println(head(Modelling.transformPCA(fixedWin, pcaFit)));
    //_unnamed [5 2]:

    //|           0 |           1 |
    //|------------:|------------:|
    //| -2.68909118 | -1.63147765 |
    //| -2.65664577 | -1.31993055 |
    //| -2.63001624 | -0.99954776 |
    //| -2.65746329 | -0.60134499 |
    //| -2.66466548 | -0.23414574 |


    //We can save out pipeline data alltogether into a byte array using the Nippy namespace.
    byte[] data = Nippy.freeze(hashmap("catFit", categoricalFit, "pcaFit", pcaFit));
    println("pipeline data byte length:", data.length);
    //pipeline data byte length: 864

    //We can serialize *just* datasets to arrow which gives us an interesting possibility.
    Arrow.datasetToStream(stocks, "test.arrow", null);

    //We can mmap them back.  This step will fail if you are on an m-1 mac unless you add
    //the memory module.  See deps.clj for example command line.
    try(AutoCloseable resCtx = stackResourceContext()) {
      //This dataset is loaded in-place.  This means that aside from string tables
      //the columns are just loaded from the mmap pointers.
      Map mmapds = Arrow.streamToDataset("test.arrow", hashmap(kw("open-type"), kw("mmap")));
      println(head(mmapds));
      //test.arrow [5 3]:

      //| symbol |       date |  price |
      //|--------|------------|-------:|
      //|   AAPL | 2000-01-01 |  25.94 |
      //|    IBM | 2000-01-01 | 100.52 |
      //|   MSFT | 2000-01-01 |  39.81 |
      //|   AMZN | 2000-01-01 |  64.56 |
      //|   AAPL | 2000-02-01 |  28.66 |

      //Cloning a dataset serves to both realize any lazy columns
      //and copy the dataset into jvm-heap memory thus allowing you to return
      //something from the stack resource context.
      println(head(tech.v3.DType.clone(mmapds)));
    }
    catch(Exception e){
      println(e);
      e.printStackTrace(System.out);
    }
    //Finally we can load/safe to parquet if that is your thing.
    Parquet.datasetToParquet(stocks, "test.parquet", null);
    //Specifying a subset of columns to load makes this *much* faster.
    //To do this use :column-whitelist - see dataset api docs for `->dataset`.
    //NOTE - If you don't disable debug logging then serializing to/from parquet is
    //unreasonably slow.  See logging section of https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html.
    println(head(Parquet.parquetToDataset("test.parquet", null)));
    //_unnamed [5 3]:

    //| symbol |       date |  price |
    //|--------|------------|-------:|
    //|   AAPL | 2000-01-01 |  25.94 |
    //|    IBM | 2000-01-01 | 100.52 |
    //|   MSFT | 2000-01-01 |  39.81 |
    //|   AMZN | 2000-01-01 |  64.56 |
    //|   AAPL | 2000-02-01 |  28.66 |


    //Here is a somewhat advanced example.  We have a dataset composed of events where each
    //row has a start,end date.  We want to tally information based the days per a given month
    //that the event happened which means we need to expand the dataset into days then reduce
    //it to tally over months.
    int nSims = 100;
    int nPlacements = 50;
    int nExpansion = 20;
    long nRows = 1000000;
    LocalDate today = LocalDate.now();
    Random rand = new Random();
    Object startDates = vec(repeatedly(nRows, new IFnDef() { public Object invoke() { return today.minusDays(400 + rand.nextInt(100)); } }));
    //Dataset with 1 million rows
    Map srcds = makeDataset(hashmap("simulation", repeatedly(nRows, new IFnDef() { public Object invoke() { return rand.nextInt(nSims); }}),
				    "placement", repeatedly(nRows, new IFnDef() { public Object invoke() { return rand.nextInt(nPlacements); }}),
				    "start", startDates,
				    "end", map(new IFnDef() { public Object invoke(Object sd) { return ((LocalDate)sd).plusDays(rand.nextInt(nExpansion)); }},
					       startDates)));
    println(head(srcds));
    //_unnamed [5 4]:

    //| placement  |      start | simulation  |        end |
    //|-----------:|------------|------------:|------------|
    //|         14 | 2020-09-28 |          86 | 2020-09-29 |
    //|         32 | 2020-12-17 |          20 | 2021-01-03 |
    //|         23 | 2020-10-15 |          37 | 2020-10-24 |
    //|         49 | 2020-10-07 |          18 | 2020-10-22 |
    //|          6 | 2020-12-08 |          48 | 2020-12-08 |




    //We are going to be creating a lot of these.
    Function<List,Map> mapConstructor = FastStruct.createFactory(vector("year-month", "count"));
    //We want to produce map of yearmonth to day counts.
    BiFunction<YearMonth,Long,Long> incrementor = new BiFunction<YearMonth,Long,Long>() {
	public Long apply(YearMonth k, Long v) {
	  if (v != null) {
	    return ((long)v) + 1;
	  } else {
	    return 1L;
	  }
	}
      };
    //Tally the days between start/end, record in map of yearMonth to day tally
    //Returns a list of maps of "year-month", "count".
    IFn tallyDays = new IFnDef() {
	public Object invoke(Object row) {
	  Map rowMap = (Map) row;
	  LocalDate sd = (LocalDate)rowMap.get("start");
	  LocalDate ed = (LocalDate)rowMap.get("end");
	  long ndays = sd.until(ed, java.time.temporal.ChronoUnit.DAYS);
	  HashMap<YearMonth,Long> tally = new HashMap<YearMonth,Long>();
	  for (long idx = 0; idx < ndays; ++idx) {
	    LocalDate cur = sd.plusDays(idx);
	    YearMonth rm = YearMonth.from(cur);
	    tally.compute(rm, incrementor);
	  }
	  ArrayList<Map> retval = new ArrayList<Map>(tally.size());
	  tally.forEach(new BiConsumer<YearMonth,Long>() {
	      public void accept(YearMonth k, Long v) {
		retval.add(mapConstructor.apply(vector(k,v)));
	      }
	    });
	  return retval;
	}
      };

    println(vec(tallyDays.invoke(hashmap("start", LocalDate.parse("2020-12-17"),
					 "end", LocalDate.parse("2021-01-03")))));
    //[{year-month #object[java.time.YearMonth 0x5eafef3a 2020-12], count 15} {year-month #object[java.time.YearMonth 0x3bcfebf6 2021-01], count 2}]

    //Next we expand our original dataset to be year-month tallies in addition to
    //to start/end dates.
    println(rowMapcat(head(srcds), tallyDays, null));
    //_unnamed [7 6]:

    //| placement |      start | simulation |        end | count | year-month |
    //|----------:|------------|-----------:|------------|------:|------------|
    //|        11 | 2020-10-29 |         41 | 2020-11-02 |     1 |    2020-11 |
    //|        11 | 2020-10-29 |         41 | 2020-11-02 |     3 |    2020-10 |
    //|        13 | 2020-10-11 |          5 | 2020-10-19 |     8 |    2020-10 |
    //|        16 | 2020-12-08 |         10 | 2020-12-11 |     3 |    2020-12 |
    //|         1 | 2020-10-15 |         52 | 2020-10-19 |     4 |    2020-10 |

    //Begin parallelized expansion
    Iterable dsSeq = (Iterable)rowMapcat(srcds, tallyDays, hashmap(kw("result-type"), kw("as-seq")));

    //The first aggregation is to summarize by placement and simulation the year-month tallies.
    //We are essentially replacing count with a summarized count.  After this statement
    //we can guarantee that the dataset has unique tuples of [simulation, placement, year-month]
    Map initAgg = Reductions.groupByColumnsAgg(dsSeq, vector("simulation", "placement", "year-month"),
					       hashmap("count", Reductions.sum("count")),
					       null);
    println(head(initAgg));
    //["simulation" "placement" "year-month"]-aggregation [5 4]:

    //| simulation | placement | year-month | count |
    //|-----------:|----------:|------------|------:|
    //|          0 |         0 |    2020-12 | 622.0 |
    //|          0 |         1 |    2020-12 | 591.0 |
    //|          0 |         2 |    2020-12 | 500.0 |
    //|          0 |         3 |    2020-12 | 549.0 |
    //|          0 |         4 |    2020-12 | 595.0 |

    // The second aggregation allows us to build of statistics over each placement/year-month
    // pair thus finding out the distribution of a given placement, year-month across simluations
    Map result = Reductions.groupByColumnsAgg(vector(initAgg), vector("placement", "year-month"),
					      hashmap("min-count",     Reductions.probQuantile("count", 0.0),
						      "low-95-count",  Reductions.probQuantile("count", 0.05),
						      "q1-count",      Reductions.probQuantile("count", 0.25),
						      "median-count",  Reductions.probQuantile("count", 0.5),
						      "q3-count",      Reductions.probQuantile("count", 0.75),
						      "high-95-count", Reductions.probQuantile("count", 0.95),
						      "max-count",     Reductions.probQuantile("count", 1.0),
						      "count",         Reductions.sum("count")),
					      null);
    //Take a million row dataset, expand it, then perform two grouping aggregations.
    println(head(result));
    //["placement" "year-month"]-aggregation [5 10]:

    //| q3-count | median-count | min-count | high-95-count | placement | max-count |   count | low-95-count | q1-count | year-month |
    //|---------:|-------------:|----------:|--------------:|----------:|----------:|--------:|-------------:|---------:|------------|
    //|    646.0 |        593.0 |     366.0 |         716.0 |        36 |     809.0 | 58920.0 |        475.0 |    536.0 |    2020-12 |
    //|    621.0 |        560.0 |     376.0 |         739.0 |        36 |     782.0 | 57107.0 |        459.0 |    512.0 |    2020-10 |
    //|    168.0 |        139.0 |      25.0 |         211.0 |         0 |     246.0 | 13875.0 |         76.0 |    112.0 |    2021-01 |
    //|    658.0 |        607.0 |     384.0 |         745.0 |         0 |     825.0 | 60848.0 |        486.0 |    561.0 |    2020-12 |
    //|    628.0 |        581.0 |     422.0 |         693.0 |         0 |     802.0 | 58148.0 |        468.0 |    539.0 |    2020-11 |


    //Let's do a quick file size comparison of the original simulation dataset.
    //We have four columns, placement simulation startdate enddate.  We know, however,
    //that placement and simulation will fit into byte data as they are integers 0-49 and 0-99,
    //respectively.  So let's start there.
    Map simds = (Map)assoc(srcds,
			   //These are checked casts.
			   "simulation", makeContainer(kw("uint8"), srcds.get("simulation")),
			   "placement", makeContainer(kw("uint8"), srcds.get("placement")));
    writeDataset(simds, "simulation.csv.gz");
    Arrow.datasetToStream(simds, "simulation.arrow-ipc", null);
    Arrow.datasetToStream(simds, "simulation-compressed.arrow-ipc", hashmap(kw("compression"), kw("zstd")));
    Parquet.datasetToParquet(simds, "simulation.parquet", null);


    IFn fileLen = new IFnDef() {
	public Object invoke(Object fname) {
	  return new java.io.File(str(fname)).length();
	}
      };
    println(makeDataset(vector(hashmap("file-type", "gzipped csv",
				       "length", fileLen.invoke("simulation.csv.gz")),
			       hashmap("file-type", "arrow ipc",
				       "length", fileLen.invoke("simulation.arrow-ipc")),
			       hashmap("file-type", "arrow ipc-compressed",
				       "length", fileLen.invoke("simulation-compressed.arrow-ipc")),
			       hashmap("file-type", "parquet",
				       "length", fileLen.invoke("simulation.parquet")))));
    //_unnamed [4 2]:

    //|            file-type |   length |
    //|----------------------|---------:|
    //|          gzipped csv |  5748193 |
    //|            arrow ipc | 10500800 |
    //| arrow ipc-compressed |  3900904 |
    //|              parquet |  3396375 |


    // If we load clojure.core.async - which neanderthal does - or we use
    // clojure.core/pmap then we have to shutdown agents else we get a 1 minute hang
    // on shutdown.
    shutdownAgents();
  }
}
