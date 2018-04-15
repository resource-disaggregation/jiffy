package elasticmem.emfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class ElasticMemByteChannel implements SeekableByteChannel {
  private long currentOffset;

  public ElasticMemByteChannel() {

    this.currentOffset = 0;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return 0;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return 0;
  }

  @Override
  public long position() throws IOException {
    return 0;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    return null;
  }

  @Override
  public long size() throws IOException {
    return 0;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    return null;
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void close() throws IOException {

  }
}
