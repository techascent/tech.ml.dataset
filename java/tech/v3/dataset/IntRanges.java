package tech.v3.dataset;



public class IntRanges {
  public static boolean byteRange(long v) {
    return v <= Byte.MAX_VALUE && v >= Byte.MIN_VALUE;
  }
  public static boolean shortRange(long v) {
    return v <= Short.MAX_VALUE && v >= Short.MIN_VALUE;
  }
}
