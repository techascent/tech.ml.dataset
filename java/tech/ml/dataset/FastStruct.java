package tech.ml.dataset;


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
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;



public class FastStruct extends APersistentMap implements IObj{
  public final IPersistentMap slots;
  public final List vals;
  public final IPersistentMap ext;
  public final IPersistentMap meta;

  public FastStruct(IPersistentMap _meta, IPersistentMap _slots,
		    List _vals, IPersistentMap _ext){
	this.meta = _meta;
	this.ext = _ext;
	this.slots = _slots;
	this.vals = _vals;
  }
  public FastStruct(IPersistentMap _slots, List _vals){
    this( PersistentHashMap.EMPTY, _slots, _vals,
	  PersistentHashMap.EMPTY);
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
    return slots.containsKey(key);
  }

  public IMapEntry entryAt(Object key){
	Map.Entry e = slots.entryAt(key);
	if(e != null)
	  return MapEntry.create(e.getKey(), vals.get(RT.intCast(e.getValue())));
	return ext.entryAt(key);
  }

  public IPersistentMap assoc(Object key, Object val){
    Map.Entry e = slots.entryAt(key);
    if(e != null) {
      int i = RT.intCast( e.getValue() );
      List newVals = new ArrayList(vals);
      newVals.set(i,val);
      return new FastStruct(meta, slots, Collections.unmodifiableList(newVals), ext);
    }
    return new FastStruct(meta, slots, vals, ext.assoc(key, val));
  }

  public Object valAt(Object key){
    Object i = slots.valAt(key);
    if(i != null) {
      return vals.get(RT.intCast(i));
    }
    return ext.valAt(key);
  }

  public Object valAt(Object key, Object notFound){
    Object i = slots.valAt(key);
    if(i != null) {
      return vals.get(RT.intCast(i));
    }
    return ext.valAt(key, notFound);
  }

  public IPersistentMap assocEx(Object key, Object val) {
    if(containsKey(key))
      throw Util.runtimeException("Key already present");
    return assoc(key, val);
  }

  public IPersistentMap without(Object key) {
    Map.Entry e = slots.entryAt(key);
    if(e != null)
      return new FastStruct(meta, slots.without(key), vals, ext);
    IPersistentMap newExt = ext.without(key);
    if(newExt == ext)
      return this;
    return new FastStruct(meta, slots, vals, newExt);
  }

  public Iterator iterator(){
    return new Iterator(){
      private Iterator ks = slots.iterator();
      private Iterator extIter = ext == null ? null : ext.iterator();
      public boolean hasNext(){
	return (ks != null && ks.hasNext() || (extIter != null && extIter.hasNext()));
      }

      public Object next(){
	if(ks != null) {
	  IMapEntry data = (IMapEntry) ks.next();
	  ks = ks.hasNext() ? ks : null;
	  int valIdx = RT.intCast(data.getValue());
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
    return vals.size() + RT.count(ext);
  }

  public ISeq seq(){
    return RT.chunkIteratorSeq(iterator());
  }

  public IPersistentCollection empty(){
    ArrayList newData = new ArrayList(slots.count());
    for (int idx = 0; idx < slots.count(); ++idx)
      newData.add(null);
    return new FastStruct(slots, newData);
  }
}
