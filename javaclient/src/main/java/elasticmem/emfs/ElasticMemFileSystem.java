package elasticmem.emfs;

import elasticmem.directory.LeaseWorker;
import elasticmem.directory.directory_service;
import elasticmem.directory.rpc_data_status;
import elasticmem.directory.rpc_dir_entry;
import elasticmem.kv.KVClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessMode;
import java.nio.file.ClosedDirectoryStreamException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ElasticMemFileSystem extends FileSystem {

  private final ElasticMemFileSystemProvider fileSystemProvider;

  private TTransport transport;
  private directory_service.Client client;
  private String dirHost;
  private List<String> toRenew;
  private LeaseWorker leaseWorker;
  private int dirPort;

  ElasticMemFileSystem(ElasticMemFileSystemProvider fileSystemProvider, String dirHost,
      int dirPort) {
    this.dirHost = dirHost;
    this.dirPort = dirPort;
    this.toRenew = Collections.synchronizedList(new ArrayList<String>());
    this.leaseWorker = new LeaseWorker(this.dirHost, this.dirPort + 1, toRenew);
    this.fileSystemProvider = fileSystemProvider;
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
  public FileSystemProvider provider() {
    return fileSystemProvider;
  }

  @Override
  public void close() {
    transport.close();
    leaseWorker.stop();
  }

  @Override
  public boolean isOpen() {
    return transport.isOpen();
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public String getSeparator() {
    return "/";
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    return Collections.<Path>singleton(new ElasticMemPath(this, new byte[]{'/'}));
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    return Collections.<FileStore>singleton(new ElasticMemFileStore(this));
  }

  @Override
  public Set<String> supportedFileAttributeViews() {
    return null;
  }

  @Override
  public Path getPath(String first, String... more) {
    String path;
    if (more.length == 0) {
      path = first;
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(first);
      for (String segment : more) {
        if (segment.length() > 0) {
          if (sb.length() > 0) {
            sb.append('/');
          }
          sb.append(segment);
        }
      }
      path = sb.toString();
    }
    return new ElasticMemPath(this, path.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    int colonIndex = syntaxAndPattern.indexOf(':');
    if (colonIndex <= 0 || colonIndex == syntaxAndPattern.length() - 1) {
      throw new IllegalArgumentException(
          "syntaxAndPattern must have form \"syntax:pattern\" but was \"" + syntaxAndPattern
              + "\"");
    }

    String syntax = syntaxAndPattern.substring(0, colonIndex);
    String pattern = syntaxAndPattern.substring(colonIndex + 1);
    String expr;
    switch (syntax) {
      case "glob":
        expr = globToRegex(pattern);
        break;
      case "regex":
        expr = pattern;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported syntax \'" + syntax + "\'");
    }
    final Pattern regex = Pattern.compile(expr);
    return new PathMatcher() {
      @Override
      public boolean matches(Path path) {
        return regex.matcher(path.toString()).matches();
      }
    };
  }

  private String globToRegex(String pattern) {
    StringBuilder sb = new StringBuilder(pattern.length());
    int inGroup = 0;
    int inClass = 0;
    int firstIndexInClass = -1;
    char[] arr = pattern.toCharArray();
    for (int i = 0; i < arr.length; i++) {
      char ch = arr[i];
      switch (ch) {
        case '\\':
          if (++i >= arr.length) {
            sb.append('\\');
          } else {
            char next = arr[i];
            switch (next) {
              case ',':
                // escape not needed
                break;
              case 'Q':
              case 'E':
                // extra escape needed
                sb.append('\\');
              default:
                sb.append('\\');
            }
            sb.append(next);
          }
          break;
        case '*':
          if (inClass == 0) {
            sb.append(".*");
          } else {
            sb.append('*');
          }
          break;
        case '?':
          if (inClass == 0) {
            sb.append('.');
          } else {
            sb.append('?');
          }
          break;
        case '[':
          inClass++;
          firstIndexInClass = i + 1;
          sb.append('[');
          break;
        case ']':
          inClass--;
          sb.append(']');
          break;
        case '.':
        case '(':
        case ')':
        case '+':
        case '|':
        case '^':
        case '$':
        case '@':
        case '%':
          if (inClass == 0 || (firstIndexInClass == i && ch == '^')) {
            sb.append('\\');
          }
          sb.append(ch);
          break;
        case '!':
          if (firstIndexInClass == i) {
            sb.append('^');
          } else {
            sb.append('!');
          }
          break;
        case '{':
          inGroup++;
          sb.append('(');
          break;
        case '}':
          inGroup--;
          sb.append(')');
          break;
        case ',':
          if (inGroup > 0) {
            sb.append('|');
          } else {
            sb.append(',');
          }
          break;
        default:
          sb.append(ch);
      }
    }
    return sb.toString();
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    throw new UnsupportedOperationException(); // TODO
  }

  @Override
  public WatchService newWatchService() throws IOException {
    throw new UnsupportedOperationException(); // TODO
  }

  public DirectoryStream<Path> newDirectoryStream(final Path dir, final Filter<? super Path> filter)
      throws IOException {
    final List<rpc_dir_entry> entries;
    final List<Path> filteredPathEntries = new ArrayList<>();
    try {
      entries = client.directoryEntries(dir.toAbsolutePath().toString());
      for (rpc_dir_entry entry : entries) {
        Path path = dir
            .resolve(new ElasticMemPath(this, entry.name.getBytes(StandardCharsets.UTF_8)));
        if (filter.accept(path)) {
          filteredPathEntries.add(path);
        }
      }
    } catch (TException e) {
      throw new IOException(e);
    }
    return new DirectoryStream<Path>() {
      private volatile boolean isClosed = false;

      @Override
      public Iterator<Path> iterator() {
        if (isClosed) {
          throw new ClosedDirectoryStreamException();
        }
        return filteredPathEntries.iterator();
      }

      @Override
      public void close() {
        isClosed = true;
      }
    };
  }

  public KVClient create(Path path, String persistentStorePrefix, int numBlocks, int chainLength)
      throws TException {
    String pathStr = path.toAbsolutePath().toString();
    rpc_data_status status = client.create(pathStr, persistentStorePrefix, numBlocks, chainLength);
    toRenew.add(pathStr);
    return new KVClient(status);
  }

  public KVClient open(Path path) throws TException {
    String pathStr = path.toAbsolutePath().toString();
    rpc_data_status status = client.open(pathStr);
    if (!toRenew.contains(pathStr)) {
      toRenew.add(pathStr);
    }
    return new KVClient(status);
  }

  public void close(Path path) {
    toRenew.remove(path.toAbsolutePath().toString());
  }

  public void remove(Path path) throws TException {
    close(path);
    client.removeAll(path.toAbsolutePath().toString());
  }

  public void flush(Path path) throws TException {
    close(path);
    client.flush(path.toAbsolutePath().toString());
  }

  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    try {
      client.createDirectory(dir.toAbsolutePath().toString());
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public void delete(Path path) throws IOException {
    try {
      client.removeAll(path.toAbsolutePath().toString());
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    throw new UnsupportedOperationException(); // TODO: fix
  }

  public void move(Path source, Path target, CopyOption... options) throws IOException {
    try {
      client.rename(source.toAbsolutePath().toString(), target.toAbsolutePath().toString());
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public boolean isSameFile(Path path, Path path2) {
    return path.toAbsolutePath().equals(path2.toAbsolutePath());
  }

  public boolean isHidden(Path path) {
    return false;
  }

  public FileStore getFileStore(Path path) {
    return null;
  }

  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    // TODO: Implement
  }

  public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type,
      LinkOption... options) {
    return null; // TODO
  }

  public ElasticMemFileAttributes readAttributes(Path path) throws IOException {
    return new ElasticMemFileAttributes(client, path.toAbsolutePath().toString());
  }

  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
      throws IOException {
    return null; // TODO
  }

  public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
      throws IOException {
    // TODO
  }


}
