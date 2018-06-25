package mmux.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import mmux.kv.KVClient;
import mmux.kv.KVClient.RedirectException;
import mmux.kv.KVClient.RedoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

public class MMuxOutputStream extends OutputStream {

  private int blockSize;
  private boolean closed;
  private long filePos = 0;
  private int pos;
  private byte[] block;
  private long blockNum;
  private KVClient client;

  MMuxOutputStream(KVClient client, Configuration conf)
      throws TException, RedoException, RedirectException {
    this.pos = 0;
    this.blockNum = 0;
    this.client = client;
    this.blockSize = conf.getInt("mmfs.block.size", 64 * 1024);
    this.block = new byte[this.blockSize];
    if (client.put("LastBlock", String.valueOf(blockNum)) == null) {
      client.update("LastBlock", String.valueOf(blockNum));
    }
  }

  public long getPos() {
    return filePos;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (pos >= blockSize) {
      flush();
    }
    block[pos++] = (byte) b;
    filePos++;
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    while (len > 0) {
      int remaining = blockSize - pos;
      int toWrite = Math.min(remaining, len);
      System.arraycopy(b, off, block, pos, toWrite);
      pos += toWrite;
      off += toWrite;
      len -= toWrite;
      filePos += toWrite;

      if (pos == blockSize) {
        flush();
      }
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    try {
      String key = String.valueOf(blockNum);
      String value = new String(block);
      if (client.put(key, value) == null) {
        client.update(key, value);
      }
      if (pos == blockSize) {
        pos = 0;
        blockNum++;
        client.update("LastBlock", String.valueOf(blockNum));
      }
    } catch (TException | RedirectException | RedoException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    flush();
    super.close();
    closed = true;
  }

}
