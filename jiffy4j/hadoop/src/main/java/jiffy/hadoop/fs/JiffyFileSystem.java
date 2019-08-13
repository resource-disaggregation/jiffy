package jiffy.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jiffy.JiffyClient;
import jiffy.directory.directory_service_exception;
import jiffy.directory.rpc_data_status;
import jiffy.directory.rpc_dir_entry;
import jiffy.directory.rpc_file_status;
import jiffy.directory.rpc_file_type;
import jiffy.storage.HashTableClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;

public class JiffyFileSystem extends FileSystem {

  private static final int DEFAULT_NUM_BLOCKS = 1;
  private static final int MAXIMUM_BLOCK_SIZE = 16384;
  private static final String DEFAULT_PERSISTENT_PATH = "local://tmp";
  private static final String DEFAULT_GROUP = "defaultgroup";
  private static final String DEFAULT_USER = System.getProperty("user.name");
  private static final String DEFAULT_WORKING_DIR = "/";

  private URI uri;
  private String dirHost;
  private int dirPort;
  private Path workingDir;
  private JiffyClient client;
  private String persistentPath;
  private String group;
  private String user;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.dirHost = uri.getHost();
    this.dirPort = uri.getPort();
    int leasePort = conf.getInt("jiffy.lease_port", dirPort + 1);
    try {
      this.client = new JiffyClient(dirHost, dirPort, leasePort);
    } catch (Exception e) {
      throw new IOException(e);
    }
    this.uri = URI.create(String.format("jfs://%s:%d", uri.getHost(), uri.getPort()));
    String path = uri.getPath();
    if (path == null || path.equals("")) {
      path = DEFAULT_WORKING_DIR;
    }
    this.workingDir = new Path(path);
    this.persistentPath = conf.get("jiffy.persistent_path", DEFAULT_PERSISTENT_PATH);
    this.group = conf.get("jiffy.group", DEFAULT_GROUP);
    this.user = conf.get("jiffy.user", DEFAULT_USER);
  }

  JiffyClient getClient() {
    return client;
  }

  @Override
  public void close() throws IOException {
    client.close();
    super.close();
  }

  @Override
  public String getScheme() {
    return "jfs";
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
      HashTableClient kv = client.openHashTable(pathStr);
      long fileLength = Long.parseLong(kv.getDataStatus().tags.get("FileLength"));
      long blockSize = Long.parseLong(kv.getDataStatus().tags.get("BlockSize"));
      return new FSDataInputStream(new JiffyInputStream(client, pathStr, kv, blockSize, fileLength));
    } catch (directory_service_exception e) {
      String msg = e.getMsg();
      if (msg.startsWith("Path corresponds to a directory")) {
        throw new FileNotFoundException(pathStr);
      }
      throw new IOException(e.getMsg());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    int actualBlockSize = Math.toIntExact(Math.min(MAXIMUM_BLOCK_SIZE, blockSize));
    try {
      Map<String, String> tags = new HashMap<>();
      tags.put("BlockSize", String.valueOf(actualBlockSize));
      tags.put("FileLength", String.valueOf(0));
      HashTableClient kv;
      if (overwrite) {
        kv = client.openOrCreateHashTable(pathStr, persistentPath, DEFAULT_NUM_BLOCKS, replication, 0,
            fsPermission.toShort(), tags);
      } else {
        kv = client.createHashTable(pathStr, persistentPath, DEFAULT_NUM_BLOCKS, replication, 0,
            fsPermission.toShort(), tags);
      }
      return new FSDataOutputStream(new JiffyOutputStream(client, pathStr, kv, actualBlockSize),
          statistics);
    } catch (directory_service_exception e) {
      String msg = e.getMsg();
      if (msg.endsWith("is a directory")) {
        throw new FileAlreadyExistsException(pathStr + " is a directory");
      } else if (msg.startsWith("Child node already exists")) {
        throw new FileAlreadyExistsException(pathStr + " already exists");
      }
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path, FsPermission fsPermission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(path, fsPermission, flags.contains(CreateFlag.OVERWRITE), bufferSize, replication,
        blockSize, progress); // TODO: This should have its own unique implementation
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) {
    throw new UnsupportedOperationException("Append is not supported by " + getClass().getName());
  }

  @Override
  public boolean rename(Path path, Path path1) {
    String pathStr = makeAbsolute(path).toString();
    String path1Str = makeAbsolute(path1).toString();
    try {
      client.rename(pathStr, path1Str);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    String pathStr = makeAbsolute(path).toString();
    try {
      if (recursive) {
        client.removeAll(pathStr);
      } else {
        client.remove(pathStr);
      }
    } catch (directory_service_exception e) {
      String msg = e.getMsg();
      if (msg.startsWith("Path does not exist")) {
        return false;
      }
      throw new IOException(msg);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private FileStatus toFileStatus(Path path, rpc_file_status fileStatus) throws TException {
    FsPermission perm = new FsPermission((short) fileStatus.getPermissions());
    long fileTS = 100; // TODO: Remove hardcoded file timestamp
    if (fileStatus.getType() == rpc_file_type.rpc_regular) {
      rpc_data_status dataStatus = client.fs().dstatus(path.toString());
      long fileLength = Long.parseLong(dataStatus.getTags().get("FileLength"));
      long blockSize = Long.parseLong(dataStatus.getTags().get("BlockSize"));
      int chainLength = dataStatus.getChainLength();
      return new FileStatus(fileLength, false, chainLength, blockSize, fileTS, fileTS,
          perm, user, group, addScheme(path));
    } else {
      return new FileStatus(0, true, 0, 0, fileTS, fileTS, perm, user,
          group, addScheme(path));
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    FileStatus status = getFileStatus(absolutePath);
    String pathStr = absolutePath.toString();
    if (status.isDirectory()) {
      try {
        List<rpc_dir_entry> entries = client.fs().directoryEntries(pathStr);
        FileStatus[] statuses = new FileStatus[entries.size()];
        for (int i = 0; i < entries.size(); ++i) {
          rpc_dir_entry entry = entries.get(i);
          statuses[i] = toFileStatus(new Path(absolutePath, entry.name), entry.status);
        }
        return statuses;
      } catch (Exception e) {
        throw new FileNotFoundException(pathStr);
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
  public boolean mkdirs(Path path, FsPermission fsPermission) throws ParentNotDirectoryException {
    String pathStr = makeAbsolute(path).toString();
    try {
      client.fs().createDirectories(pathStr);
    } catch (directory_service_exception e) {
      String msg = e.getMsg();
      if (msg.endsWith("is a file.")) {
        throw new ParentNotDirectoryException(pathStr);
      }
      return false;
    } catch (TException e) {
      return false;
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    String pathStr = absolutePath.toString();
    try {
      return toFileStatus(absolutePath, client.fs().status(pathStr));
    } catch (Exception e) {
      throw new FileNotFoundException();
    }
  }

  @Override
  public FsStatus getStatus(Path p) {
    return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
  }

  private String removeScheme(String s) {
    URI uri = URI.create(s);
    return uri.getPath();
  }

  private Path addScheme(Path path) {
    return new Path(getScheme() + "://" + dirHost + ":" + dirPort + makeAbsolute(path).toString());
  }

  private Path makeAbsolute(Path path) {
    String pathString = removeScheme(path.toString());
    if (path.isAbsolute()) {
      return new Path(pathString);
    } else if (pathString.equals("")) {
      return workingDir;
    } else {
      return new Path(workingDir, pathString);
    }
  }
}
