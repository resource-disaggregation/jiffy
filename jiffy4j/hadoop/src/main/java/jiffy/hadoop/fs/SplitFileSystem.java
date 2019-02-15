package jiffy.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* SplitFS allows a filesystem to split their IO between two separate file systems. One for
 * persistent files and one for ephemeral files.
 *
 * These filesystems are configured by specifying:
 * splitfs.ephemeral.fs - the name of the ephemeral file system class,
 * splitfs.persistent.fs - the name of the persistent file system class,
 * splitfs.ephemeral.fs.impl - the class path of the ephemeral file system class,
 * splitfs.persistent.fs.impl - the class path of the persistent file system class
 */
public class SplitFileSystem extends FileSystem {

  // Filesystem used for persistently stored (non temporary) files.
  private FileSystem persistentFileSystem;

  // Filesystem used for ephemeral (temporary) files.
  private FileSystem ephemeralFileSystem;

  private URI uri;

  private final static String EPHEMERAL_PREFIX = "tmp";
  private final static String SCHEME = "splitfs";

  /**
   * The SLF4J logger to use in logging within the FileSystem class itself.
   */
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SplitFileSystem.class);

  private FileSystem initializeFS(URI uri, Configuration conf)
      throws IOException {
    FileSystem fs;
    Class<?> persistentClazz = getFileSystemsFromConf(String.format("fs.%s.impl",
        uri.getScheme()), conf);
    fs = (FileSystem) ReflectionUtils.newInstance(persistentClazz, conf);
    fs.initialize(uri, conf);
    return fs;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.uri = uri;

    try {
      URI persistentUri = new URI(conf.get("splitfs.persistent.fs"));
      persistentFileSystem = initializeFS(persistentUri, conf);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    try {
      URI ephemeralUri = new URI(conf.get("splitfs.ephemeral.fs"));
      ephemeralFileSystem = initializeFS(ephemeralUri, conf);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    persistentFileSystem.close();
    ephemeralFileSystem.close();
    super.close();
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.open(path, bufferSize);
    } else {
      return persistentFileSystem.open(path, bufferSize);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    path = prefixWithFsURI(path);
    if (ephemeralFileSystem == null || persistentFileSystem == null) {
      System.out.println("One of the filesystems is null");
    }
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem
          .create(path, fsPermission, overwrite, bufferSize, replication, blockSize, progress);
    } else {
      return persistentFileSystem
          .create(path, fsPermission, overwrite, bufferSize, replication, blockSize, progress);
    }
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.append(path, i, progressable);
    } else {
      return persistentFileSystem.append(path, i, progressable);
    }
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.rename(path, path1);
    } else {
      return persistentFileSystem.rename(path, path1);
    }
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.delete(path, recursive);
    } else {
      return persistentFileSystem.delete(path, recursive);
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.listStatus(path);
    } else {
      return persistentFileSystem.listStatus(path);
    }
  }

  @Override
  public void setWorkingDirectory(Path path) {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      ephemeralFileSystem.setWorkingDirectory(path);
    } else {
      persistentFileSystem.setWorkingDirectory(path);
    }
  }

  public void setEphemeralWorkingDirectory(Path path) throws IOException {
    path = prefixWithFsURI(path);
    ephemeralFileSystem.setWorkingDirectory(path);
  }

  public void setPersistentWorkingDirectory(Path path) throws IOException {
    path = prefixWithFsURI(path);
    persistentFileSystem.setWorkingDirectory(path);
  }

  public Path getEphemeralWorkingDirectory() {
    return ephemeralFileSystem.getWorkingDirectory();
  }

  public Path getPersistentWorkingDirectory() {
    return persistentFileSystem.getWorkingDirectory();
  }

  @Override
  public Path getWorkingDirectory() {
    // What working directory to return here?
    return persistentFileSystem.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.mkdirs(path, fsPermission);
    } else {
      return persistentFileSystem.mkdirs(path, fsPermission);
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    path = prefixWithFsURI(path);
    if (isEphemeralPath(path)) {
      return ephemeralFileSystem.getFileStatus(path);
    } else {
      return persistentFileSystem.getFileStatus(path);
    }
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends FileSystem> getFileSystemsFromConf(String field,
      Configuration conf) throws UnsupportedFileSystemException {
    Class<? extends FileSystem> clazz = null;
    if (conf != null) {
      clazz = (Class<? extends FileSystem>) conf.getClass(field, null);
    }
    if (clazz == null) {
      throw new UnsupportedFileSystemException("No FileSystem for field: " + field);
    }
    return clazz;
  }

  private boolean isEphemeralPath(Path p) {
    if (p.getParent() == null) {
      return false;
    } else {
      return p.getParent().toUri().getPath().startsWith(EPHEMERAL_PREFIX);
    }
  }

  private Path prefixWithFsURI(Path input) {
    try {
      URI uri = new URI(input.toString());
      String path = uri.getPath();
      if (isEphemeralPath(input)) {
        if (input.isAbsolute()) {
          System.out.println(ephemeralFileSystem.getUri().toString() + path);
          return new Path(ephemeralFileSystem.getUri().toString() + path);
        } else {
          return new Path(ephemeralFileSystem.getWorkingDirectory() + "/" + path);
        }
      } else {
        if (input.isAbsolute()) {
          return new Path(persistentFileSystem.getUri().toString() + path);
        } else {
          return new Path(persistentFileSystem.getWorkingDirectory() + "/" + path);
        }
      }
    } catch (URISyntaxException ignored) {
      return input;
    }
  }

  @Override
  protected void checkPath(Path path) {
    // Empty to allow for splitfs:///
  }
}
