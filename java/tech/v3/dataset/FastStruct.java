package tech.v3.dataset;


import clojure.lang.PersistentStructMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.APersistentMap;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentCollection;
import clojure.lang.IObj;
import clojure.lang.Obj;
import clojure.lang.ISeq;
import clojure.lang.MapEntry;
import clojure.lang.IMapEntry;
import clojure.lang.ASeq;
import clojure.lang.Util;
import clojure.lang.RT;
import clojure.lang.IFn;
import com.github.ztellman.primitive_math.Primitives;
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Function;
import tech.v3.datatype.IFnDef;



public class FastStruct extends APersistentMap implements IObj{
  public final Map slots; //HashMaps are faster than persistent maps.
  public final List vals;
  public final IPersistentMap ext;
  public final IPersistentMap meta;

  public FastStruct(IPersistentMap _meta, Map _slots,
		    List _vals, IPersistentMap _ext) {
    this.meta = _meta;
    this.ext = _ext;
    this.slots = _slots;
    this.vals = _vals;
  }

  public FastStruct(Map _slots, List _vals) {
    this( PersistentHashMap.EMPTY, _slots, _vals,
	  PersistentHashMap.EMPTY);
  }

  public int columnIndex(Object key) throws Exception {
    Object v = slots.get(key);
    if (v != null) {
      return RT.uncheckedIntCast(v);
    }
    throw new Exception("Key was not found");
  }

  public IObj withMeta(IPersistentMap _meta){
    if(meta == _meta)
      return this;
    return new FastStruct (_meta, slots, vals, ext);
  }

  public IPersistentMap meta(){
    return meta;
  }

  public boolean containsKey(Object key){
    return slots.containsKey(key) || ext.containsKey(key);
  }

  public IMapEntry entryAt(Object key){
    Object v = slots.get(key);
    if(v != null)
      return MapEntry.create(key, vals.get(RT.uncheckedIntCast(v)));
    return ext.entryAt(key);
  }

  public IPersistentMap assoc(Object key, Object val){
    Object v = slots.get(key);
    if(v != null) {
      int i = RT.uncheckedIntCast( v );
      List newVals = new ArrayList(vals);
      newVals.set(i,val);
      return new FastStruct(meta, slots, Collections.unmodifiableList(newVals), ext);
    }
    return new FastStruct(meta, slots, vals, ext.assoc(key, val));
  }

  public Object valAt(Object key){
    Object i = slots.get(key);
    if(i != null) {
      return vals.get(RT.uncheckedIntCast(i));
    }
    return ext.valAt(key);
  }

  public Object valAt(Object key, Object notFound){
    Object i = slots.get(key);
    if(i != null) {
      return vals.get(RT.uncheckedIntCast(i));
    }
    return ext.valAt(key, notFound);
  }

  public IPersistentMap assocEx(Object key, Object val) {
    if(containsKey(key))
      throw Util.runtimeException("Key already present");
    return assoc(key, val);
  }

  public IPersistentMap without(Object key) {
    if(slots.containsKey(key)) {
      HashMap newSlots = new HashMap(slots);
      newSlots.remove(key);
      return new FastStruct(meta, Collections.unmodifiableMap(newSlots), vals, ext);
    }
    IPersistentMap newExt = ext.without(key);
    if(newExt == ext)
      return this;
    return new FastStruct(meta, slots, vals, newExt);
  }

  public Iterator iterator(){
    return new Iterator(){
      private Iterator ks = slots.entrySet().iterator();
      private Iterator extIter = ext == null ? null : ext.iterator();
      public boolean hasNext(){
	return (ks != null && ks.hasNext() || (extIter != null && extIter.hasNext()));
      }

      public Object next(){
	if(ks != null) {
	  Map.Entry data = (Map.Entry) ks.next();
	  ks = ks.hasNext() ? ks : null;
	  int valIdx = RT.uncheckedIntCast(data.getValue());
	  return new MapEntry( data.getKey(), vals.get(valIdx));
	}
	else if(extIter != null && extIter.hasNext()) {
	  Object data = extIter.next();
	  extIter = extIter.hasNext() ? extIter : null;
	  return data;
	}
	else
	  throw new NoSuchElementException();
      }
      public void remove(){
	throw new UnsupportedOperationException();
      }
    };
  }

  public int count(){
    return slots.size() + RT.count(ext);
  }

  public ISeq seq(){
    return RT.chunkIteratorSeq(iterator());
  }

  public IPersistentCollection empty(){
    ArrayList newData = new ArrayList(slots.size());
    for (int idx = 0; idx < slots.size(); ++idx)
      newData.add(null);
    return new FastStruct(slots, newData);
  }

  /**
   * Create a factory that will create map implementations based on a single list of values.
   * Values have to be in the same order as column names.
   */
  public static Function<List,Map> createFactory(List colnames) {
    int nEntries = colnames.size();
    if( nEntries == 0 ) {
      throw new RuntimeException("No column names provided");
    }
    HashMap slots = new HashMap(nEntries);
    for (int idx = 0; idx < nEntries; ++idx ) {
      slots.put(colnames.get(idx), idx);
    }
    Map constSlots = (Map)Collections.unmodifiableMap(slots);
    if( colnames.size() != slots.size() ) {
      throw new RuntimeException("Duplicate colname name: " + String.valueOf(slots));
    }
    return new Function<List,Map>() {
      public Map apply(List valList) {
	if( slots.size() != valList.size() ) {
	  throw new RuntimeException("Number of values: " + String.valueOf(valList.size()) +
				     " doesn't equal the number of keys: " + String.valueOf(slots.size()));
	}
	return new FastStruct(constSlots, valList);
      }
    };
  }

  public static FastStruct createFromColumnNames(List colnames, List vals) {
    return (FastStruct)createFactory(colnames).apply(vals);
  }
}
