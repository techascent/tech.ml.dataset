package tech.v3.dataset;

import ham_fisted.Casts;


public interface PParser {
  public void addValue(long idx, Object value);
  public default void addDouble(long idx, double value) {
    if(Double.isNaN(value))
      addMissing(idx);
    else
      addValue(idx, value);
  }
  public default void addLong(long idx, long value) {
    addValue(idx, value);
  }
  public default void addMissing(long idx) {
    addValue(idx, null);
  }
  public Object finalize(long rowcount);

  public interface DoubleParser extends PParser {
    public default void addValue(long idx, Object value) {
      addDouble(idx, Casts.doubleCast(value));
    }
    public default void addLong(long idx, long value) {
      addDouble(idx, (double)value);
    }
  }
  public interface LongParser extends PParser {
    public default void addValue(long idx, Object value) {
      addLong(idx, Casts.longCast(value));
    }
    public default void addDouble(long idx, Object value) {
      addLong(idx, Casts.longCast(value));
    }
  }
}
