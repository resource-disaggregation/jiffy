package jiffy.hadoop.fs;

import java.nio.ByteBuffer;

class JiffyBlock {
  private static final int METADATA_OFFSET = 0;

  private ByteBuffer ref;
  private ByteBuffer data;

  JiffyBlock(long blockSize) {
    ref = data = ByteBuffer.allocate((int) (blockSize + Integer.BYTES));
    data.putInt(0);
  }

  JiffyBlock(ByteBuffer buf) {
    ref = buf;
    data = ref.slice();
    data.position(Integer.BYTES);
  }

  void seek(long targetPos) {
    data.position((int)targetPos + Integer.BYTES);
  }

  static int usedBytes(ByteBuffer buf) {
    return buf.getInt(METADATA_OFFSET);
  }

  int usedBytes() {
    return data.getInt(METADATA_OFFSET);
  }

  void setData(ByteBuffer buf) {
    ref = buf;
    data = ref.slice();
    data.position(Integer.BYTES);
  }

  int position() {
    return data.position() - Integer.BYTES;
  }

  int capacity() {
    return data.capacity() - Integer.BYTES;
  }

  int remaining() {
    return data.remaining();
  }

  boolean hasRemaining() {
    return data.hasRemaining();
  }

  byte get() {
    return data.get();
  }

  void get(byte[] b, int off, int len) {
    data.get(b, off, len);
  }

  void reset() {
    data.position(METADATA_OFFSET);
    data.putInt(0);
  }

  void write(byte b[], int off, int len) {
    data.put(b, off, len);
    data.putInt(METADATA_OFFSET, position());
  }

  void write(byte b) {
    data.put(b);
    data.putInt(METADATA_OFFSET, position());
  }

  ByteBuffer getData() {
    return (ByteBuffer) data.duplicate().clear();
  }
}
