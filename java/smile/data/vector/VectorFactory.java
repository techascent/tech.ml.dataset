package smile.data.vector;

import smile.data.type.StructField;

public class VectorFactory
{
  public static BooleanVector booleanVector(StructField name, boolean[] data)
  {
    return new BooleanVectorImpl(name, data);
  }
  public static ByteVector byteVector(StructField name, byte[] data)
  {
    return new ByteVectorImpl(name, data);
  }
  public static ShortVector shortVector(StructField name, short[] data)
  {
    return new ShortVectorImpl(name, data);
  }
  public static IntVector intVector(StructField name, int[] data)
  {
    return new IntVectorImpl(name, data);
  }
  public static LongVector longVector(StructField name, long[] data)
  {
    return new LongVectorImpl(name, data);
  }
  public static FloatVector floatVector(StructField name, float[] data)
  {
    return new FloatVectorImpl(name, data);
  }
  public static DoubleVector doubleVector(StructField name, double[] data)
  {
    return new DoubleVectorImpl(name, data);
  }
  public static StringVector stringVector(StructField name, String[] data)
  {
    return new StringVectorImpl(name, data);
  }
  public static Vector genericVector(StructField name, Object[] data)
  {
    return new VectorImpl(name, data);
  }
}
