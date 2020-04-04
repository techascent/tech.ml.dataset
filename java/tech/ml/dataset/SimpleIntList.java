package tech.ml.dataset;

import it.unimi.dsi.fastutil.ints.IntList;
import tech.v2.datatype.Countable;

public interface SimpleIntList extends IntList, Countable
{
  boolean addInt(int idx, int value);
  default boolean add(int value) {
    return addInt(size(), value);
  }
  default void add(int idx, int value) {
    addInt(idx, value);
  }
  int setInt(int index, int value);
  default int set(int index, int value) {
    return setInt(index, value);
  }
  default int size() {
    return (int)lsize();
  }
}
