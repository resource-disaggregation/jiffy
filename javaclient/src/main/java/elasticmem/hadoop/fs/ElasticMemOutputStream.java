package elasticmem.hadoop.fs;

import elasticmem.kv.KVClient;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

public class ElasticMemOutputStream extends OutputStream {

  private int blockSize;
  private boolean closed;
  private int pos = 0;
  private long filePos = 0;
  private ByteBuffer key;
  private byte[] block;
  private long blockNum = 0;
  private KVClient client;

  public ElasticMemOutputStream(KVClient client, Configuration conf) {
    this.key = ByteBuffer.allocate(8);
    this.client = client;
    this.blockSize = conf.getInt("emfs.block_size", 64 * 1024);
    this.block = new byte[this.blockSize];
  }

  public long getPos() throws IOException {
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
      key.putLong(blockNum).rewind();
      ByteBuffer value = ByteBuffer.wrap(block);
      if (client.put(key, value).toString().equals("key_already_exists")) {
        client.update(key, value);
      }
      if (pos == blockSize) {
        pos = 0;
        blockNum++;
      }
    } catch (TException e) {
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
