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
  private URI persistentURI;
  private FileSystem persistentFileSystem = null;

  // Filesystem used for ephemeral (temporary) files.
  private URI ephemeralURI;
  private FileSystem ephemeralFileSystem = null;

  private URI uri;
  private Path workingDir;

  private final static String EPHEMERAL_PREFIX = "/tmp";
  private final static String SCHEME = "splitfs";

  static class PathInfo {

    private FileSystem fs;
    private Path path;

    PathInfo(FileSystem fs, Path path) {
      this.fs = fs;
      this.path = path;
    }

    Path getPath() {
      return path;
    }

    FileSystem getFs() {
      return fs;
    }
  }

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

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.uri = uri;
    try {
      ephemeralURI = new URI(conf.get("splitfs.ephemeral.fs"));
      ephemeralFileSystem = null;

      persistentURI = new URI(conf.get("splitfs.persistent.fs"));
      persistentFileSystem = null;
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();

    if (ephemeralFileSystem != null) {
      ephemeralFileSystem.close();
    }

    if (persistentFileSystem != null) {
      persistentFileSystem.close();
    }
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
    PathInfo info = getPathInfo(path);
    return info.getFs().open(info.getPath(), bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    PathInfo info = getPathInfo(path);
    return info.getFs()
        .create(info.getPath(), fsPermission, overwrite, bufferSize, replication, blockSize,
            progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    PathInfo info = getPathInfo(path);
    return info.getFs().append(info.getPath(), i, progressable);
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    PathInfo info = getPathInfo(path);
    PathInfo info1 = getPathInfo(path1);
    if (info.getFs() != info1.getFs()) {
      throw new IOException("Cannot rename path from one FS to another");
    }
    return info.getFs().rename(info.getPath(), info1.getPath());
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    PathInfo info = getPathInfo(path);
    return info.getFs().delete(info.getPath(), recursive);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    PathInfo info = getPathInfo(path);
    FileStatus[] fileStatuses = info.getFs().listStatus(info.getPath());
    for (int i = 0; i < fileStatuses.length; i++) {
      fileStatuses[i] = makeStatus(fileStatuses[i]);
    }
    return fileStatuses;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    this.workingDir = path;
  }

  void setEphemeralWorkingDirectory(Path path) throws IOException {
    ephemeralFS().setWorkingDirectory(path);
  }

  void setPersistentWorkingDirectory(Path path) throws IOException {
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
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    PathInfo info = getPathInfo(path);
    return info.getFs().mkdirs(info.getPath(), fsPermission);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    PathInfo info = getPathInfo(path);
    return makeStatus(info.getFs().getFileStatus(info.getPath()));
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
      return p.toUri().getPath().startsWith(EPHEMERAL_PREFIX);
    }
  }

  private PathInfo getPathInfo(Path p) throws IOException {
    if (isEphemeralPath(p)) {
      return new PathInfo(ephemeralFS(), makeAbsoluteEphemeral(p));
    } else {
      return new PathInfo(persistentFS(), makeAbsolutePersistent(p));
    }
  }

  private Path makeAbsoluteEphemeral(Path input) {
    return input.makeQualified(ephemeralURI, getEphemeralWorkingDirectory());
  }

  private Path makeAbsolutePersistent(Path input) {
    return input.makeQualified(persistentURI, getPersistentWorkingDirectory());
  }

  private Path makeAbsolute(Path input) {
    return input.makeQualified(uri, workingDir);
  }

  private FileStatus makeStatus(FileStatus status) throws IOException {
    return new FileStatus(status.getLen(), status.isDirectory(), status.getReplication(),
        status.getBlockSize(), status.getModificationTime(), status.getAccessTime(),
        status.getPermission(), status.getOwner(), status.getGroup(),
        (status.isSymlink() ? status.getSymlink() : null), makeAbsolute(status.getPath()));
  }

  @Override
  protected void checkPath(Path path) {
    // Empty to allow for splitfs:///
  }
}
