package elasticmem.hadoop.fs;

import elasticmem.kv.KVClient;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.thrift.TException;

public class ElasticMemInputStream extends FSInputStream {

  private boolean closed;
  private long fileLength;
  private long filePos = 0;

  private int blockSize;
  private ByteBuffer key;
  private long currentBlockNum;
  private ByteBuffer currentBuf;

  private KVClient client;

  ElasticMemInputStream(KVClient client, Configuration conf) {
    this.key = ByteBuffer.allocate(8);
    this.client = client;
    this.blockSize = conf.getInt("emfs.block_size", 64 * 1024 * 1024);
    this.currentBlockNum = -1;
    this.currentBuf = null;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return filePos;
  }

  @Override
  public synchronized int available() throws IOException {
    return (int) (fileLength - filePos);
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > fileLength) {
      throw new IOException("Cannot seek after EOF");
    }
    filePos = targetPos;
    resetBuf();
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    int result = -1;
    if (filePos < fileLength) {
      if (currentBuf.position() >= blockSize) {
        resetBuf();
      }
      result = currentBuf.get();
      filePos++;
    }
    return result;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (filePos < fileLength) {
      if (currentBuf.position() >= blockSize) {
        resetBuf();
      }
      int realLen = Math.min(len, blockSize - currentBuf.position() + 1);
      currentBuf.get(buf, off, realLen);
      filePos += realLen;
      return realLen;
    }
    return -1;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    super.close();
    closed = true;
  }

  /**
   * We don't support marks.
   */
  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // marks not supported
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }

  private long currentBlockNum() {
    return filePos / blockSize;
  }

  private void resetBuf() throws IOException {
    if (currentBlockNum != currentBlockNum()) {
      try {
        key.putLong(currentBlockNum()).rewind();
        currentBlockNum = currentBlockNum();
        currentBuf = client.get(key);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
  }

}
