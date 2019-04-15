package jiffy.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

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
  private static URI persistentURI = null;
  private static FileSystem persistentFileSystem = null;

  // Filesystem used for ephemeral (temporary) files.
  private static URI ephemeralURI = null;
  private static FileSystem ephemeralFileSystem = null;

  private URI uri;

  private final static String EPHEMERAL_PREFIX = "/tmp";
  private final static String SCHEME = "splitfs";

  private FileSystem initializeFS(URI uri, Configuration conf)
      throws IOException {
    FileSystem fs;
    Class<?> persistentClazz = getFileSystemsFromConf(String.format("fs.%s.impl",
        uri.getScheme()), conf);
    fs = (FileSystem) ReflectionUtils.newInstance(persistentClazz, conf);
    fs.initialize(uri, conf);
    return fs;
  }

  private FileSystem ephemeralFS() throws IOException {
    if (ephemeralFileSystem == null) {
      ephemeralFileSystem = initializeFS(ephemeralURI, getConf());
    }
    return ephemeralFileSystem;
  }

  private FileSystem persistentFS() throws IOException {
    if (persistentFileSystem == null) {
      persistentFileSystem = initializeFS(persistentURI, getConf());
    }
    return persistentFileSystem;
  }

  private FileSystem getFS(Path path) throws IOException {
    if (isEphemeralPath(path)) {
      return ephemeralFS();
    } else {
      return persistentFS();
    }
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.uri = uri;
    try {
      URI newEphemeralURI = new URI(conf.get("splitfs.ephemeral.fs"));
      if (ephemeralURI == null || !(ephemeralURI.getHost().equals(newEphemeralURI.getHost())
          && ephemeralURI.getPort() == newEphemeralURI.getPort()
          && ephemeralURI.getScheme().equals(newEphemeralURI.getScheme()))) {
        ephemeralURI = newEphemeralURI;
        ephemeralFileSystem = null;
      }

      URI newPersistentURI = new URI(conf.get("splitfs.persistent.fs"));
      if (persistentURI == null || !(persistentURI.getHost().equals(newPersistentURI.getHost())
          && persistentURI.getPort() == newPersistentURI.getPort()
          && persistentURI.getScheme().equals(newPersistentURI.getScheme()))) {
        persistentURI = newPersistentURI;
        persistentFileSystem = null;
      }
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (persistentFileSystem != null) {
      persistentFileSystem.close();
    }
    if (ephemeralFileSystem != null) {
      ephemeralFileSystem.close();
    }
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
    return getFS(path).open(path, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path)
        .create(path, fsPermission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path).append(path, i, progressable);
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path).rename(path, path1);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path).delete(path, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path).listStatus(path);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    path = prefixWithFsURI(path);
    try {
      getFS(path).setWorkingDirectory(path);
    } catch (IOException ignored) {
    }
  }

  void setEphemeralWorkingDirectory(Path path) throws IOException {
    path = prefixWithFsURI(path);
    ephemeralFS().setWorkingDirectory(path);
  }

  void setPersistentWorkingDirectory(Path path) throws IOException {
    path = prefixWithFsURI(path);
    persistentFS().setWorkingDirectory(path);
  }

  Path getEphemeralWorkingDirectory() {
    try {
      return ephemeralFS().getWorkingDirectory();
    } catch (IOException e) {
      return null;
    }
  }

  Path getPersistentWorkingDirectory() {
    try {
      return persistentFS().getWorkingDirectory();
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Path getWorkingDirectory() {
    // What working directory to return here?
    try {
      return persistentFS().getWorkingDirectory();
    } catch (IOException ignored) {
      return null;
    }
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path).mkdirs(path, fsPermission);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    path = prefixWithFsURI(path);
    return getFS(path).getFileStatus(path);
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
      return p.toUri().getPath().startsWith(EPHEMERAL_PREFIX)
          && !p.toUri().getPath().endsWith("jar")
          && !p.toUri().getPath().endsWith("xml");
    }
  }

  private Path prefixWithFsURI(Path input) {
    try {
      URI uri = new URI(input.toString());
      String path = uri.getPath();
      if (isEphemeralPath(input)) {
        if (input.isAbsolute()) {
          return new Path(ephemeralFS().getUri().toString() + path);
        } else {
          return new Path(ephemeralFS().getWorkingDirectory() + "/" + path);
        }
      } else {
        if (input.isAbsolute()) {
          return new Path(persistentFS().getUri().toString() + path);
        } else {
          return new Path(persistentFS().getWorkingDirectory() + "/" + path);
        }
      }
    } catch (URISyntaxException | IOException ignored) {
      return input;
    }
  }

  @Override
  protected void checkPath(Path path) {
    // Empty to allow for splitfs:///
  }
}
