package jiffy.hadoop.fs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import jiffy.JiffyClient;
import jiffy.storage.HashTableClient;
import jiffy.util.ByteBufferUtils;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.thrift.TException;

public class JiffyInputStream extends FSInputStream {

  private final JiffyClient mm;
  private boolean closed;
  private long filePos;

  private long blockSize;
  private long fileLength;
  private long currentBlockNum;
  private JiffyBlock currentBlock;

  private HashTableClient client;
  private String path;

  JiffyInputStream(JiffyClient mm, String path, HashTableClient client, long blockSize,
      long fileLength) {
    this.mm = mm;
    this.path = path;
    this.filePos = 0;
    this.client = client;
    this.currentBlockNum = -1;
    this.currentBlock = null;
    this.fileLength = fileLength;
    this.blockSize = blockSize;
  }

  @Override
  public synchronized long getPos() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    return filePos;
  }

  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    return (int) (fileLength - filePos);
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (targetPos > fileLength) {
      throw new EOFException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new EOFException("Cannot seek to negative position");
    }
    filePos = targetPos;
    if (filePos == 0 && fileLength == 0) {
      return;
    }
    resetBuf();
    long bufferPos = filePos % blockSize;
    currentBlock.seek(bufferPos);
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
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
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (len == 0) {
      return 0;
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
        if (ByteBufferUtils.toString(value).equals("!key_not_found")) {
          throw new EOFException("EOF");
        }
        if (currentBlock == null) {
          currentBlock = new JiffyBlock(value);
        } else {
          currentBlock.setData(value);
        }
      } catch (TException e) {
        throw new IOException(e);
      }
    }
  }

}
