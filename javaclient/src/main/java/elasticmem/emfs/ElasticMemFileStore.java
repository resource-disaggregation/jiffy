package elasticmem.emfs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

public class ElasticMemFileStore extends FileStore {

  private final ElasticMemFileSystem fileSystem;

  public ElasticMemFileStore(ElasticMemFileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @Override
  public String name() {
    return fileSystem.getHost() + ":" + fileSystem.getPort();
  }

  @Override
  public String type() {
    return fileSystem.provider().getScheme();
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public long getTotalSpace() throws IOException {
    return 0; // TODO
  }

  @Override
  public long getUsableSpace() throws IOException {
    return 0; // TODO
  }

  @Override
  public long getUnallocatedSpace() throws IOException {
    return 0; // TODO
  }

  @Override
  public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
    return false; // TODO
  }

  @Override
  public boolean supportsFileAttributeView(String name) {
    return false; // TODO
  }

  @Override
  public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
    return null; // TODO
  }

  @Override
  public Object getAttribute(String attribute) throws IOException {
    return null; // TODO
  }
}
