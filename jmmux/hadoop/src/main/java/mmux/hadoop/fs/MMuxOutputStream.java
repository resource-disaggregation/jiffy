package mmux.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import mmux.kv.KVClient;
import mmux.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class MMuxOutputStream extends OutputStream {

  private boolean closed;
  private MMuxBlock block;
  private long blockNum;
  private KVClient client;
  private ByteBuffer lastBlockKey;

  MMuxOutputStream(KVClient client, int blockSize) throws TException {
    this.blockNum = 0;
    this.client = client;
    this.block = new MMuxBlock(blockSize);
    this.lastBlockKey = ByteBufferUtils.fromString("LastBlock");
    ByteBuffer lastBlockValue = ByteBufferUtils.fromString(String.valueOf(blockNum));
    if (client.put(lastBlockKey, lastBlockValue) == ByteBufferUtils.fromString("!key_already_exists")) {
      client.update(lastBlockKey, lastBlockValue);
    }
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (block.remaining() == 0) {
      flush();
    }
    block.write((byte) b);
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    while (len > 0) {
      int toWrite = Math.min(block.remaining(), len);
      block.write(b, off, toWrite);
      off += toWrite;
      len -= toWrite;

      if (block.remaining() == 0) {
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
      ByteBuffer value = block.getData();
      if (client.put(key, value) == ByteBufferUtils.fromString("!key_already_exists")) {
        client.update(key, value);
      }
      if (block.remaining() == 0) {
        block.reset();
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
