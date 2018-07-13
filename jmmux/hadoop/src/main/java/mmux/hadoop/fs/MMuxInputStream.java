package mmux.hadoop.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import mmux.kv.KVClient;
import mmux.util.ByteBufferUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.thrift.TException;

public class MMuxInputStream extends FSInputStream {

  private boolean closed;
  private long filePos;

  private int blockSize;
  private long fileLength;
  private long currentBlockNum;
  private ByteBuffer currentBuf;

  private KVClient client;
  private ByteBuffer lastBlockKey;

  MMuxInputStream(KVClient client, Configuration conf) throws TException {
    this.filePos = 0;
    this.client = client;
    this.blockSize = conf.getInt("emfs.block_size", 64 * 1024 * 1024);
    this.currentBlockNum = -1;
    this.currentBuf = null;
    this.lastBlockKey = ByteBufferUtils.fromString("LastBlock");
    ByteBuffer lastBlock = client.get(lastBlockKey);
    long lastBlockNum = Long.parseLong(ByteBufferUtils.toString(lastBlock));
    this.fileLength = lastBlockNum * blockSize;
    ByteBuffer value;
    if ((value = client.get(lastBlock)) != ByteBufferUtils.fromString("!key_not_found")) {
       this.fileLength += value.array().length;
    }
  }

  private ByteBuffer getLastBlockNum() throws TException {
    return client.get(lastBlockKey);
  }

  @Override
  public synchronized long getPos() {
    return filePos;
  }

  @Override
  public synchronized int available() {
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
  public synchronized boolean seekToNewSource(long targetPos) {
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
        currentBlockNum = currentBlockNum();
        ByteBuffer value = client.get(ByteBufferUtils.fromString(String.valueOf(currentBlockNum)));
        if (value == ByteBufferUtils.fromString("!key_not_found")) {
          throw new IOException("EOF");
        }
        currentBuf = value;
      } catch (TException e) {
        throw new IOException(e);
      }
    }
  }

}
