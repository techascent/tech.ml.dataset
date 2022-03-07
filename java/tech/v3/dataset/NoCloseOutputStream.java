package tech.v3.dataset;


import java.io.OutputStream;
import java.io.IOException;


public class NoCloseOutputStream extends OutputStream {
  public final OutputStream stream;
  public NoCloseOutputStream(OutputStream os) {
    stream = os;
  }
  public void close() throws IOException {}
  public void fluse() throws IOException { stream.flush(); }
  public void write(byte[] b) throws IOException { stream.write(b); }
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b,off,len);
  }
  public void write(int b) throws IOException { stream.write(b); }
}
