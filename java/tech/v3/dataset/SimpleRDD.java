package tech.v3.dataset;


import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.Dependency;
import org.apache.spark.util.NextIterator;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import java.util.function.Function;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import clojure.lang.IDeref;
import clojure.java.api.Clojure;
import clojure.lang.IFn;


public class SimpleRDD
  extends RDD<Row> {
  //Each partition is a thing that can resolve to an iterable of rows
  final List partitions;
  final String partitionToRows;

  public class SimplePartition implements Partition {
    public final int idx;
    public SimplePartition(int _idx) { idx = _idx; }
    public int index() { return idx; }
    public boolean equals(Object other) {
      if (other instanceof SimplePartition) {
	return idx == ((SimplePartition)other).idx;
      } else {
	return false;
      }
    }
  }

  public SimpleRDD(SparkContext _sc, List _partitions, String _partitionToRows) {
    super(_sc,
	  JavaConverters.asScalaBuffer(new ArrayList<Dependency<?>>()).toSeq(),
	  scala.reflect.ClassTag$.MODULE$.apply(Row.class));
    partitions = _partitions;
    partitionToRows = _partitionToRows;
  }

  public Partition[] getPartitions() {
    Partition[] retval = new Partition[partitions.size()];
    for (int idx = 0; idx < partitions.size(); ++idx ) {
      retval[idx] = new SimplePartition(idx);
    }
    return retval;
  }

  public Iterator<Row> compute(Partition partition, TaskContext tc) {
    IFn partToRows = Clojure.var(partitionToRows);
    Object data = partitions.get(partition.index());
    Iterable concrete = (Iterable)partToRows.invoke( data );
    @SuppressWarnings("unchecked")
    java.util.Iterator<Row> src_iter = (java.util.Iterator<Row>)concrete.iterator();
    return JavaConverters.asScalaIterator(src_iter);
  }
}
