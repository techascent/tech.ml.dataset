package tech.v3.dataset;


import tech.v3.datatype.IndexConsumer;
import tech.v3.datatype.ECount;
import ham_fisted.Reducible;
import ham_fisted.IMutList;
import org.roaringbitmap.RoaringBitmap;
import clojure.lang.IDeref;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

public class IntColParser
  implements IDeref, PParser, ECount {

  public final IndexConsumer data;
  public final RoaringBitmap missing;
  public final Object colname;
  long lastidx;

  public IntColParser(IFn rangeFn, IMutList dlist, Object colname) {
    data = new IndexConsumer(rangeFn, dlist);
    missing = new RoaringBitmap();
    this.colname = colname;
  }

  public long lsize() { return lastidx; }

  public void addMissing(long idx) {
    if(lastidx < idx) {
      missing.add(lastidx, idx);
    }
    lastidx = idx+1;
  }

  public void addValue(long idx, Object val) {
    addMissing(idx);

    if (val instanceof Long)
      data.accept((Long)val);
    else if (val instanceof Integer)
      data.accept((Integer)val);
    else if (val instanceof Short)
      data.accept((Short)val);
    else if (val instanceof Byte)
      data.accept((Byte)val);
    else if (val == null)
      missing.add((int)idx);
    else
      throw new RuntimeException("Value " + String.valueOf(val) + " is not an integer value");
  }

  public Object deref() {
    return new PersistentArrayMap(new Object[] {
	Keyword.intern("tech.v3.dataset", "data"), data.deref(),
	Keyword.intern("tech.v3.dataset", "missing"), missing,
	Keyword.intern("tech.v3.dataset", "name"), colname});
  }

  public Object finalize(long rowcount) {
    addMissing(rowcount);
    return deref();
  }
}
