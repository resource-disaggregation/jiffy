package elasticmem.emfs;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ElasticMemFileSystemProvider extends FileSystemProvider {

  private final Map<String, ElasticMemFileSystem> fileSystems = new HashMap<>();
  public static final String SCHEME = "emfs";

  private ElasticMemPath toEmPath(Path path) {
    if (path == null) {
      throw new NullPointerException();
    }
    if (!(path instanceof ElasticMemPath)) {
      throw new ProviderMismatchException();
    }
    return (ElasticMemPath) path;
  }

  private ElasticMemFileSystem getFileSystem(Path path) {
    ElasticMemPath emPath = toEmPath(path);
    FileSystem fileSystem = emPath.getFileSystem();
    if (fileSystem == null) {
      throw new NullPointerException();
    }
    if (!(fileSystem instanceof ElasticMemFileSystem)) {
      throw new ProviderMismatchException();
    }
    return (ElasticMemFileSystem) fileSystem;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
    synchronized (fileSystems) {
      String host = uri.getHost();
      int port = uri.getPort();
      String key = host + ":" + port;
      ElasticMemFileSystem fileSystem = fileSystems.get(key);
      if (fileSystem != null) {
        throw new FileSystemAlreadyExistsException(key);
      }
      fileSystem = new ElasticMemFileSystem(this, host, port);
      fileSystems.put(key, fileSystem);
      return fileSystem;
    }
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    synchronized (fileSystems) {
      String host = uri.getHost();
      int port = uri.getPort();
      String key = host + ":" + port;
      ElasticMemFileSystem fileSystem = fileSystems.get(key);
      if (fileSystem == null) {
        throw new FileSystemNotFoundException(key);
      }
      return fileSystem;
    }
  }

  @Override
  public Path getPath(URI uri) {
    return getFileSystem(uri).getPath(uri.getPath());
  }

  @Override
  public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options,
      FileAttribute<?>... attrs) throws IOException {
    return null; // TODO
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter)
      throws IOException {
    return getFileSystem(dir).newDirectoryStream(dir, filter);
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    getFileSystem(dir).createDirectory(dir, attrs);
  }

  @Override
  public void delete(Path path) throws IOException {
    getFileSystem(path).delete(path);
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    getFileSystem(source).copy(source, target, options);
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    getFileSystem(source).move(source, target, options);
  }

  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    return getFileSystem(path).isSameFile(path, path2);
  }

  @Override
  public boolean isHidden(Path path) throws IOException {
    return getFileSystem(path).isHidden(path);
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    return getFileSystem(path).getFileStore(path);
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    getFileSystem(path).checkAccess(path, modes);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type,
      LinkOption... options) {
    return getFileSystem(path).getFileAttributeView(path, type, options);
  }

  @Override
  public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type,
      LinkOption... options) throws IOException {
    return (A) getFileSystem(path).readAttributes(path);
  }

  @Override
  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
      throws IOException {
    return getFileSystem(path).readAttributes(path, attributes, options);
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
      throws IOException {
    getFileSystem(path).setAttribute(path, attribute, value, options);
  }
}
