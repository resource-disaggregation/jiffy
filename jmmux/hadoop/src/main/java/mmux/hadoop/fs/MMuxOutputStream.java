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
  private int blockSize;
  private KVClient client;
  private ByteBuffer fileSizeKey;

  MMuxOutputStream(KVClient client, int blockSize) throws TException {
    this.blockNum = 0;
    this.blockSize = blockSize;
    this.client = client;
    this.block = new MMuxBlock(blockSize);
    this.fileSizeKey = ByteBufferUtils.fromString("FileSize");
    ByteBuffer lastBlockValue = ByteBufferUtils.fromString(String.valueOf(0));
    client.upsert(fileSizeKey, lastBlockValue);
  }

  private long filePos() {
    return blockNum * blockSize + block.usedBytes();
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
      client.upsert(ByteBufferUtils
          .fromByteBuffers(ByteBufferUtils.fromString(String.valueOf(blockNum)), block.getData(),
              fileSizeKey, ByteBufferUtils.fromString(String.valueOf(filePos()))));
      if (block.remaining() == 0) {
        block.reset();
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
