package smile.data;

import smile.data.vector.BaseVector;

public class DataFrameFactory
{
  public static DataFrame construct(BaseVector... columns)
  {
    return new DataFrameImpl(columns);
  }
}
