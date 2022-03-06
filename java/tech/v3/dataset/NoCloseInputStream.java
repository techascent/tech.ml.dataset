package tech.v3.dataset;


import java.io.InputStream;
import java.io.IOException;

public class NoCloseInputStream extends InputStream {
  public final InputStream stream;
  public NoCloseInputStream(InputStream _stream) {
    stream = _stream;
  }
  public int available() throws IOException { return stream.available(); }
  //Explicitly do not forward close call
  public void close(){}
  public void mark(int maxBytes) { stream.mark(maxBytes); }
  public boolean markSupported() { return stream.markSupported(); }
  public int read() throws IOException { return stream.read(); }
  public int read(byte[] data) throws IOException { return stream.read(data); }
  public int read(byte[] data, int off, int len) throws IOException { return stream.read(data,off,len); }
  public void reset() throws IOException { stream.reset(); }
  public long skip(long n) throws IOException { return stream.skip(n); }
}
