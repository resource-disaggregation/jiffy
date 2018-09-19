package mmux.hadoop.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import mmux.MMuxClient;
import mmux.kv.KVClient;
import mmux.util.ByteBufferUtils;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.thrift.TException;

public class MMuxInputStream extends FSInputStream {

  private final MMuxClient mm;
  private boolean closed;
  private long filePos;

  private long blockSize;
  private long fileLength;
  private long currentBlockNum;
  private MMuxBlock currentBlock;

  private KVClient client;
  private String path;

  MMuxInputStream(MMuxClient mm, String path, KVClient client) throws TException {
    this.mm = mm;
    this.path = path;
    this.filePos = 0;
    this.client = client;
    this.currentBlockNum = -1;
    this.currentBlock = null;
    ByteBuffer fileSizeKey = ByteBufferUtils.fromString("FileSize");
    this.fileLength = Long.parseLong(ByteBufferUtils.toString(client.get(fileSizeKey)));
    ByteBuffer blockSizeKey = ByteBufferUtils.fromString("BlockSize");
    this.blockSize = Long.parseLong(ByteBufferUtils.toString(client.get(blockSizeKey)));
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
    long bufferPos = filePos % blockSize;
    currentBlock.seek(bufferPos);

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
      if (currentBlock == null || !currentBlock.hasRemaining()) {
        resetBuf();
      }
      result = currentBlock.get() & 0xFF;
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
      if (currentBlock == null || !currentBlock.hasRemaining()) {
        resetBuf();
      }
      int realLen = Math.min(len, currentBlock.usedBytes() - currentBlock.position());
      currentBlock.get(buf, off, realLen);
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
    mm.close(path);
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
    throw new UnsupportedOperationException("Mark not supported");
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
        if (currentBlock == null) {
          currentBlock = new MMuxBlock(value);
        } else {
          currentBlock.setData(value);
        }
      } catch (TException e) {
        throw new IOException(e);
      }
    }
  }

}
