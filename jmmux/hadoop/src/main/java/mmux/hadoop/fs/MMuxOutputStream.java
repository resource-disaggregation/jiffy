package mmux.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import mmux.kv.KVClient;
import mmux.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class MMuxOutputStream extends OutputStream {

  private int blockSize;
  private boolean closed;
  private long filePos = 0;
  private int pos;
  private byte[] block;
  private long blockNum;
  private KVClient client;
  private ByteBuffer lastBlockKey;

  MMuxOutputStream(KVClient client, int blockSize) throws TException {
    this.pos = 0;
    this.blockNum = 0;
    this.client = client;
    this.blockSize = blockSize;
    this.block = new byte[this.blockSize];
    this.lastBlockKey = ByteBufferUtils.fromString("LastBlock");
    ByteBuffer lastBlockValue = ByteBufferUtils.fromString(String.valueOf(blockNum));
    if (client.put(lastBlockKey, lastBlockValue) == ByteBufferUtils.fromString("!key_already_exists")) {
      client.update(lastBlockKey, lastBlockValue);
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
      ByteBuffer key = ByteBufferUtils.fromString(String.valueOf(blockNum));
      ByteBuffer value = ByteBuffer.wrap(block);
      if (client.put(key, value) == ByteBufferUtils.fromString("!key_already_exists")) {
        client.update(key, value);
      }
      if (pos == blockSize) {
        pos = 0;
        blockNum++;
        client.update(lastBlockKey, ByteBufferUtils.fromString(String.valueOf(blockNum)));
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
