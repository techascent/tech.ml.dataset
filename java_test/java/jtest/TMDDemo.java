package jtest;


import static tech.v3.Clj.*;
import static tech.v3.TMD.*;
import tech.v3.DType; //access to clone method
import static java.lang.System.out;


public class TMDDemo {
  public static void main(String[] args) {
    //Make dataset can take a string, inputStream, a sequence of maps or a map of columns with
    //the map of columns being the most efficient.
    //Default file formats:
    //csv, tsv, csv.gz, tsv.gz, (compressed, general, and surprisingly fast) .nippy
    Map ds = makeDataset("https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv");
    println(head(ds));
  }
}
