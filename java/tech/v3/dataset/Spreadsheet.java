package tech.v3.dataset;

import clojure.lang.Keyword;
import java.lang.Iterable;
import java.lang.AutoCloseable;

public class Spreadsheet
{
  public interface Workbook extends Iterable, AutoCloseable
  {
  }
  public interface Sheet extends Iterable
  {
    public String name();

    public String id();

    public String stableId();
  }
  public interface Row extends Iterable
  {
    public int getRowNum();
  }
  public interface Cell
  {
    boolean missing();
    int getColumnNum();
    Object value();
    double doubleValue();
    boolean boolValue();
  }
}
