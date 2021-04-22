package tech.v3.dataset;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.io.OutputFile;
import clojure.lang.IFn;
import java.util.Map;

public class ParquetRowWriter extends WriteSupport<Long>
{
  public final IFn rowWriter;
  public final MessageType schema;
  public final Map<String,String> metadata;
  public RecordConsumer consumer;
  public Object dataset;
  public ParquetRowWriter(IFn _writer, MessageType _schema, Map<String,String> _meta) {
    rowWriter = _writer;
    schema = _schema;
    metadata = _meta;
    consumer = null;
    //Outside forces must set dataset
    dataset = null;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext( schema, metadata );
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    consumer = recordConsumer;
  }

  @Override
  public void write(Long record) {
    rowWriter.invoke(dataset,record,consumer);
  }  
  public static class WriterBuilder extends ParquetWriter.Builder<Long,WriterBuilder>
  {
    public final ParquetRowWriter writer;
    public WriterBuilder(OutputFile outf, ParquetRowWriter _writer) {
      super(outf);
      writer = _writer;
    }
    public WriterBuilder self() { return this; }
    protected WriteSupport<Long> getWriteSupport(Configuration conf) {
      return writer;
    }
  }
  
}
