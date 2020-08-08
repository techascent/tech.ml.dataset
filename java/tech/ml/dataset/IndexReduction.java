package tech.ml.dataset;


import java.util.function.BiFunction;

public interface IndexReduction
{
  public default Object datasetContext(Object dataset)
  {
    return dataset;
  }
  public Object reduceIndex(Object dsCtx, Object ctx, long idx);
  public Object reduceReductions(Object lhsCtx, Object rhsCtx);
  public default Object finalize(Object ctx)
  {
    return ctx;
  }

  public class IndexedBiFunction implements BiFunction<Object,Object,Object>
  {
    public long index;
    public Object dsCtx;
    public IndexReduction reducer;
    public IndexedBiFunction(Object _dsCtx, IndexReduction rd)
    {
      index = 0;
      dsCtx = _dsCtx;
      reducer = rd;
    }
    public void setIndex(long idx) {this.index = idx;}
    public Object apply(Object lhs, Object rhs) {
      return reducer.reduceIndex(dsCtx, rhs, index);
    }
  }
}
