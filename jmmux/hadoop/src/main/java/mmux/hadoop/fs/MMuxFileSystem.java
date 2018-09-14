package mmux.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import mmux.MMuxClient;
import mmux.directory.rpc_data_status;
import mmux.directory.rpc_dir_entry;
import mmux.directory.rpc_file_status;
import mmux.directory.rpc_file_type;
import mmux.kv.KVClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;

public class MMuxFileSystem extends FileSystem {

  private static final int DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024;
  private static final String DEFAULT_PERSISTENT_PATH = "local://tmp";
  private static final String DEFAULT_GROUP = "defaultgroup";
  private static final String DEFAULT_USER = System.getProperty("user.name");

  private URI uri;
  private String dirHost;
  private int dirPort;
  private int leasePort;
  private Path workingDir;
  private MMuxClient client;
  private int blockSize;
  private String persistentPath;
  private String group;
  private String user;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.dirHost = uri.getHost();
    this.dirPort = uri.getPort();
    this.leasePort = dirPort + 1;
    try {
      this.client = new MMuxClient(dirHost, dirPort, leasePort);
    } catch (TException e) {
      throw new IOException(e);
    }
    this.uri = URI.create(String.format("mmfs://%s:%d", uri.getHost(), uri.getPort()));
    String path = uri.getPath();
    if (path == null || path.equals("")) {
      path = "/fsdir/";
      try {
        client.fs().createDirectories(path);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
    this.workingDir = new Path(path);

    this.blockSize = conf.getInt("mmux.blocksize", DEFAULT_BLOCK_SIZE);
    this.persistentPath = conf.get("mmux.persistent_path", DEFAULT_PERSISTENT_PATH);
    this.group = conf.get("mmux.group", DEFAULT_GROUP);
    this.user = conf.get("mmux.user", DEFAULT_USER);
  }

  public String getDirHost() {
    return dirHost;
  }

  public int getDirPort() {
    return dirPort;
  }

  public int getLeasePort() {
    return leasePort;
  }

  public MMuxClient getClient() {
    return client;
  }

  @Override
  public void close() throws IOException {
    client.close();
    super.close();
  }

  @Override
  public String getScheme() {
    return "mmfs";
  }

  @Override
  public int getDefaultPort() {
    return 9090;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    try {
      KVClient kv = client.open(pathStr);
      return new FSDataInputStream(new MMuxInputStream(kv, blockSize));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    // TODO: Set permissions
    // TODO: Make block size configurable per file
    String pathStr = makeAbsolute(path).toString();
    try {
      KVClient kv;
      if (overwrite) {
        kv = client.openOrCreate(pathStr, persistentPath, 1, replication);
      } else {
        kv = client.create(pathStr, persistentPath, 1, replication);
      }
      return new FSDataOutputStream(new MMuxOutputStream(kv, this.blockSize), statistics);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) {
    throw new UnsupportedOperationException(
        "Append is not supported by " + MMuxFileSystem.class.getName());
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    try {
      client.rename(makeAbsolute(path).toString(), makeAbsolute(path1).toString());
    } catch (mmux.directory.directory_service_exception directory_service_exception) {
      return false;
    } catch (TException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      if (recursive) {
        client.removeAll(makeAbsolute(path).toString());
      } else {
        client.remove(makeAbsolute(path).toString());
      }
    } catch (mmux.directory.directory_service_exception directory_service_exception) {
      return false;
    } catch (TException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    FileStatus status = getFileStatus(absolutePath);

    if (status.isDirectory()) {
      try {
        List<rpc_dir_entry> entries = client.fs().directoryEntries(absolutePath.toString());
        FileStatus[] statuses = new FileStatus[entries.size()];
        int i = 0;
        for (rpc_dir_entry entry : entries) {
          Path child = new Path(absolutePath, entry.name);
          rpc_file_status fileStatus = entry.status;
          // FIXME: Remove hardcoded parameter: permissions
          FsPermission perm = new FsPermission("777");
          long fileTS = 100;
          if (fileStatus.getType() == rpc_file_type.rpc_regular) {
            rpc_data_status dataStatus = client.fs().dstatus(child.toString());
            // FIXME: Remove hardcoded parameter: access_time
            // FIXME: Support storing username & groups in MemoryMUX
            statuses[i] = new FileStatus(dataStatus.getDataBlocksSize(), false,
                dataStatus.getChainLength(), blockSize, fileTS, fileTS, perm, user, group,
                child);
          } else {
            statuses[i] = new FileStatus(0, true, 0, 0, fileTS, fileTS, perm, user,
                group, child);
          }
          i++;
        }
        return statuses;
      } catch (TException e) {
        throw new FileNotFoundException(path.toUri().getRawPath());
      }
    }
    return new FileStatus[]{status};
  }

  @Override
  public void setWorkingDirectory(Path path) {
    workingDir = makeAbsolute(path);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    try {
      client.fs().createDirectories(makeAbsolute(path).toString());
    } catch (mmux.directory.directory_service_exception directory_service_exception) {
      return false;
    } catch (TException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    try {
      Path absolutePath = makeAbsolute(path);
      rpc_file_status fileStatus = client.fs().status(absolutePath.toString());
      // FIXME: Remove hardcoded parameter: permissions
      FsPermission perm = new FsPermission("777");
      long fileTS = 100;
      if (fileStatus.getType() == rpc_file_type.rpc_regular) {
        rpc_data_status dataStatus = client.fs().dstatus(absolutePath.toString());
        return new FileStatus(dataStatus.getDataBlocksSize(), false, dataStatus.getChainLength(),
            blockSize, fileTS, fileTS, perm, user, group, absolutePath);
      } else {
        return new FileStatus(0, true, 0, 0, fileTS, fileTS, perm, user, group, absolutePath);
      }
    } catch (TException e) {
      throw new FileNotFoundException();
    }
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
  }

  private String removeMmfsPrefix(String s) {
    URI uri = URI.create(s);
    return uri.getPath();
  }

  private Path makeAbsolute(Path path) {
    String pathString = removeMmfsPrefix(path.toString());

    if (path.isAbsolute()) {
      return new Path(pathString);
    } else if (pathString.equals("")) {
      return workingDir;
    } else {
      return new Path(workingDir, pathString);
    }
  }
}
