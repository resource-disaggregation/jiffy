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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;

public class MMuxFileSystem extends FileSystem {

  private URI uri;
  private String dirHost;
  private int dirPort;
  private int leasePort;
  private Path workingDir;
  private MMuxClient client;

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
    if (path == null || path.equals(""))
    {
      path = "/fsdir/";
    }
    this.workingDir = new Path(path);
    try {
      client.fs().createDirectories(this.workingDir.toString());
    } catch (TException e) {
      throw new IOException(e);
    }
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
      return new FSDataInputStream(new MMuxInputStream(kv, getConf()));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    String persistentPath = getConf().get("mmfs.persistent_prefix", "local://tmp");
    try {
      KVClient kv;
      if (overwrite) {
        kv = client.openOrCreate(pathStr, persistentPath, 1, replication);
      } else {
        kv = client.create(pathStr, persistentPath, 1, replication);
      }
      return new FSDataOutputStream(new MMuxOutputStream(kv, getConf()), statistics);
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
      client.fs().rename(makeAbsolute(path).toString(), makeAbsolute(path1).toString());
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
        client.fs().removeAll(makeAbsolute(path).toString());
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
          if (fileStatus.getType() == rpc_file_type.rpc_regular) {
            rpc_data_status dataStatus = client.fs().dstatus(child.toString());
            statuses[i] = new FileStatus(0, false, dataStatus.chain_length, 32 * 1024 * 1024,
                fileStatus.last_write_time, child);
          } else {
            statuses[i] = new FileStatus(0, true, 0, 0, fileStatus.last_write_time, child);
          }
          i++;
        }
        return statuses;
      } catch (TException e) {
        throw new IOException(e);
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
      if (fileStatus.getType() == rpc_file_type.rpc_regular) {
        rpc_data_status dataStatus = client.fs().dstatus(absolutePath.toString());
        return new FileStatus(0, false, dataStatus.chain_length, 64 * 1024 * 1024,
            fileStatus.last_write_time, absolutePath);
      } else {
        return new FileStatus(0, true, 0, 0, fileStatus.last_write_time, absolutePath);
      }
    } catch (TException e) {
      throw new FileNotFoundException();
    }
  }

  private String removeMmfsPrefix(String s) {
    URI uri = URI.create(s);
    return uri.getPath();
  }


  private Path makeAbsolute(Path path) {
    String pathString = removeMmfsPrefix(path.toString());

    if (path.isAbsolute()) {
      return new Path(pathString);
    }
    return new Path(workingDir, pathString);
  }
}
