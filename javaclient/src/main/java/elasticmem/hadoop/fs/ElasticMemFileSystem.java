package elasticmem.hadoop.fs;

import elasticmem.directory.LeaseWorker;
import elasticmem.directory.directory_service;
import elasticmem.directory.rpc_data_status;
import elasticmem.directory.rpc_dir_entry;
import elasticmem.directory.rpc_file_status;
import elasticmem.directory.rpc_file_type;
import elasticmem.kv.KVClient;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ElasticMemFileSystem extends FileSystem {

  private URI uri;
  private String dirHost;
  private int dirPort;
  private Path workingDir;
  private directory_service.Client client;
  private List<String> toRenew;
  private LeaseWorker leaseWorker;
  private TTransport transport;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    this.dirHost = uri.getHost();
    this.dirPort = uri.getPort();
    int leasePort = dirPort + 1;
    this.toRenew = Collections.synchronizedList(new ArrayList<String>());
    this.leaseWorker = new LeaseWorker(this.dirHost, leasePort, toRenew);
    this.uri = URI.create(String.format("emfs://%s:%d", uri.getHost(), uri.getPort()));
    String genPath = "/" + RandomStringUtils.random(10);
    this.workingDir = new Path(conf.get("emfs.app.name", genPath));
    this.transport = new TSocket(dirHost, dirPort);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);
    client = new directory_service.Client(protocol);
    leaseWorker.run();
  }

  public String getHost() {
    return dirHost;
  }

  public int getPort() {
    return dirPort;
  }

  public directory_service.Client getClient() {
    return client;
  }

  @Override
  public void close() throws IOException {
    leaseWorker.stop();
    transport.close();
    super.close();
  }

  @Override
  public String getScheme() {
    return "emfs";
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
      rpc_data_status status = client.open(pathStr);
      if (!toRenew.contains(pathStr)) {
        toRenew.add(pathStr);
      }
      return new FSDataInputStream(new ElasticMemInputStream(new KVClient(status), getConf()));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    try {
      rpc_data_status status;
      if (overwrite) {
        status = client
            .openOrCreate(pathStr, getConf().get("emfs.persistent_prefix", "/tmp"), 1, replication);
      } else {
        status = client
            .create(pathStr, getConf().get("emfs.persistent_prefix", "/tmp"), 1, replication);
      }
      toRenew.add(pathStr);
      return new FSDataOutputStream(new ElasticMemOutputStream(new KVClient(status), getConf()),
          statistics);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) {
    throw new UnsupportedOperationException(
        "Append is not supported by " + ElasticMemFileSystem.class.getName());
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    try {
      client.rename(makeAbsolute(path).toString(), makeAbsolute(path1).toString());
    } catch (elasticmem.directory.directory_service_exception directory_service_exception) {
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
    } catch (elasticmem.directory.directory_service_exception directory_service_exception) {
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
        List<rpc_dir_entry> entries = client.directoryEntries(absolutePath.toString());
        FileStatus[] statuses = new FileStatus[entries.size()];
        int i = 0;
        for (rpc_dir_entry entry : entries) {
          Path child = new Path(absolutePath, entry.name);
          rpc_file_status fileStatus = entry.status;
          if (fileStatus.getType() == rpc_file_type.rpc_regular) {
            rpc_data_status dataStatus = client.dstatus(absolutePath.toString());
            statuses[i] = new FileStatus(0, false, dataStatus.chain_length, 64 * 1024 * 1024,
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
      client.createDirectories(makeAbsolute(path).toString());
    } catch (elasticmem.directory.directory_service_exception directory_service_exception) {
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
      rpc_file_status fileStatus = client.status(absolutePath.toString());
      if (fileStatus.getType() == rpc_file_type.rpc_regular) {
        rpc_data_status dataStatus = client.dstatus(absolutePath.toString());
        return new FileStatus(0, false, dataStatus.chain_length, 64 * 1024 * 1024,
            fileStatus.last_write_time, absolutePath);
      } else {
        return new FileStatus(0, true, 0, 0, fileStatus.last_write_time, absolutePath);
      }
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }
}
