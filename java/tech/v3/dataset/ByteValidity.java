package tech.v3.dataset;

import clojure.lang.RT;
import clojure.lang.IDeref;
import java.util.function.LongConsumer;
import ham_fisted.ArrayLists;
import org.roaringbitmap.RoaringBitmap;


public class ByteValidity {
  public static int trimIndexes(int[] indexes, int nIndexes, long maxIdx) {
    while(nIndexes > 0) {
      if(Integer.toUnsignedLong(indexes[nIndexes-1]) >= maxIdx)
	--nIndexes;
      else
	break;
    }
    return nIndexes;
  }
  public static abstract class ValidityBase implements LongConsumer, IDeref {
    long nElems;
    int idx;
    int nIndexes;
    int[] indexes;
    public ValidityBase(long nElems, long maxIndexes) {
      this.nElems = nElems;
      indexes = new int[(int)maxIndexes];
      nIndexes = 0;
      idx = 0;
    }
  }
  public static class ValidityIndexReducer extends ValidityBase {
    public ValidityIndexReducer(long nElems, long maxIndexes) {
      super(nElems, maxIndexes);
    }
    public void accept(long value) {
      if(value != 0) {
	int intVal = (int)value;
	int offset = idx * 8;
	if( (intVal & 1) == 1) indexes[nIndexes++] = offset;
	if( (intVal & 2) == 2) indexes[nIndexes++] = offset+1;
	if( (intVal & 4) == 4) indexes[nIndexes++] = offset+2;
	if( (intVal & 8) == 8) indexes[nIndexes++] = offset+3;
	if( (intVal & 16) == 16) indexes[nIndexes++] = offset+4;
	if( (intVal & 32) == 32) indexes[nIndexes++] = offset+5;
	if( (intVal & 64) == 64) indexes[nIndexes++] = offset+6;
	if( (intVal & 128) == 128) indexes[nIndexes++] = offset+7;
      }
      ++idx;
    }
    public Object deref() {
      return ArrayLists.toList(indexes).subList(0, trimIndexes(indexes, nIndexes, nElems)); }
  }
  public static class MissingIndexReducer extends ValidityBase {
    public MissingIndexReducer(long nElems, long maxIndexes) {
      super(nElems, maxIndexes);
    }
    public void accept(long value) {
      if(value != -1) {
	int intVal = (int)value;
	int offset = idx * 8;
	if( (intVal & 1) != 1) indexes[nIndexes++] = offset;
	if( (intVal & 2) != 2) indexes[nIndexes++] = offset+1;
	if( (intVal & 4) != 4) indexes[nIndexes++] = offset+2;
	if( (intVal & 8) != 8) indexes[nIndexes++] = offset+3;
	if( (intVal & 16) != 16) indexes[nIndexes++] = offset+4;
	if( (intVal & 32) != 32) indexes[nIndexes++] = offset+5;
	if( (intVal & 64) != 64) indexes[nIndexes++] = offset+6;
	if( (intVal & 128) != 128) indexes[nIndexes++] = offset+7;
      }
      ++idx;
    }
    public Object deref() {
      RoaringBitmap rb = new RoaringBitmap();
      rb.addN(indexes, 0, trimIndexes(indexes, nIndexes, nElems));
      return rb;
    }
  }
}
