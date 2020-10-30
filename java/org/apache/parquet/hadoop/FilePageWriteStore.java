package org.apache.parquet.hadoop;



import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import java.io.IOException;


public class FilePageWriteStore implements PageWriteStore {
  final ColumnChunkPageWriteStore pageWriteStore;
  public FilePageWriteStore( BytesCompressor compressor, MessageType schema, ByteBufferAllocator allocator,
			     int columnIndexTruncateLength, boolean pageWriteChecksumEnabled) {
    pageWriteStore = new ColumnChunkPageWriteStore(compressor, schema, allocator,
						   columnIndexTruncateLength, pageWriteChecksumEnabled);
  }
  public PageWriter getPageWriter(ColumnDescriptor path) {
    return pageWriteStore.getPageWriter(path);
  }
  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    pageWriteStore.flushToFileWriter(writer);
  }
}
