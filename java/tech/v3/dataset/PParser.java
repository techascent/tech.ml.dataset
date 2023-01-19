package tech.v3.dataset;


public interface PParser {
  public void addValue(long idx, Object value);
  public Object finalize(long rowcount);
}
