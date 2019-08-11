package jiffy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jiffy.directory.Permissions;
import jiffy.directory.directory_service;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import jiffy.partition.PartitionNameBuilder;
import jiffy.storage.FileReader;
import jiffy.storage.FileWriter;
import jiffy.storage.HashTableClient;
import jiffy.lease.LeaseWorker;
import jiffy.notification.HashTableListener;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class JiffyClient implements Closeable {

  private static final String DEFAULT_BACKING_PATH = "local://tmp";
  private static final int DEFAULT_NUM_BLOCKS = 1;
  private static final int DEFAULT_CHAIN_LENGTH = 1;
  private static final int DEFAULT_FLAGS = 0;
  private static final int DEFAULT_TIMEOUT_MS = 5000;
  private static final int DEFAULT_PERMISSIONS = Permissions.all;
  private static final Map<String, String> DEFAULT_TAGS = Collections.emptyMap();

  private TTransport transport;
  private directory_service.Client fs;
  private LeaseWorker worker;
  private Thread workerThread;
  private int timeoutMs;

  public JiffyClient(String host, int dirPort, int leasePort) throws TException {
    this(host, dirPort, leasePort, DEFAULT_TIMEOUT_MS);
  }

  public JiffyClient(String host, int dirPort, int leasePort, int timeoutMs) throws TException {
    this.timeoutMs = timeoutMs;
    TSocket sock = new TSocket(host, dirPort);
    sock.setTimeout(this.timeoutMs);
    this.transport = sock;
    this.fs = new Client(new TBinaryProtocol(transport));
    this.transport.open();
    this.worker = new LeaseWorker(host, leasePort);
    this.workerThread = new Thread(worker);
    this.workerThread.start();
  }

  public directory_service.Client fs() {
    return fs;
  }

  public LeaseWorker getWorker() {
    return worker;
  }

  public void beginScope(String path) {
    worker.addPath(path);
  }

  public void endScope(String path) {
    worker.removePath(path);
  }

  private static class InitType {

    static int CREATE = 0;
    static int OPEN_OR_CREATE = 1;
  }

  private rpc_data_status initDataStructure(int initType, String path, String type,
      String backingPath,
      int numBlocks, int chainLength, int flags, int permissions, Map<String, String> tags)
      throws TException {
    List<String> partitionNames = new ArrayList<>(numBlocks);
    List<String> partitionMetadata = new ArrayList<>(numBlocks);
    PartitionNameBuilder builder = PartitionNameBuilder.get(type, numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      partitionNames.add(i, builder.partitionName(i));
      partitionMetadata.add(i, "regular");
    }
    rpc_data_status status = null;
    if (initType == InitType.CREATE) {
      status = fs.create(path, type, backingPath, numBlocks, chainLength, flags, permissions,
          partitionNames, partitionMetadata, tags);
    } else if (initType == InitType.OPEN_OR_CREATE) {
      status = fs.openOrCreate(path, type, backingPath, numBlocks, chainLength, flags, permissions,
          partitionNames, partitionMetadata, tags);
    }
    beginScope(path);
    return status;
  }

  public HashTableClient createHashTable(String path) throws TException {
    return createHashTable(path, DEFAULT_BACKING_PATH);
  }

  public HashTableClient createHashTable(String path, String backingPath) throws TException {
    return createHashTable(path, backingPath, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH);
  }

  public HashTableClient createHashTable(String path, String backingPath, int numBlocks,
      int chainLength) throws TException {
    return createHashTable(path, backingPath, numBlocks, chainLength, DEFAULT_FLAGS,
        DEFAULT_PERMISSIONS, DEFAULT_TAGS);
  }

  public HashTableClient createHashTable(String path, String backingPath, int numBlocks,
      int chainLength, int flags, int permissions, Map<String, String> tags) throws TException {
    rpc_data_status status = initDataStructure(InitType.CREATE, path, "hashtable", backingPath,
        numBlocks, chainLength, flags, permissions, tags);
    return new HashTableClient(fs, path, status, timeoutMs);
  }

  public HashTableClient openHashTable(String path) throws TException {
    rpc_data_status status = fs.open(path);
    beginScope(path);
    return new HashTableClient(fs, path, status, timeoutMs);
  }

  public HashTableClient openOrCreateHashTable(String path) throws TException {
    return openOrCreateHashTable(path, DEFAULT_BACKING_PATH);
  }

  public HashTableClient openOrCreateHashTable(String path, String backingPath) throws TException {
    return openOrCreateHashTable(path, backingPath, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH);
  }

  public HashTableClient openOrCreateHashTable(String path, String backingPath, int numBlocks,
      int chainLength)
      throws TException {
    return openOrCreateHashTable(path, backingPath, numBlocks, chainLength, DEFAULT_FLAGS,
        DEFAULT_PERMISSIONS, DEFAULT_TAGS);
  }

  public HashTableClient openOrCreateHashTable(String path, String backingPath, int numBlocks,
      int chainLength, int flags, int permissions,
      Map<String, String> tags) throws TException {
    rpc_data_status status = initDataStructure(InitType.OPEN_OR_CREATE, path, "hashtable",
        backingPath, numBlocks, chainLength, flags, permissions, tags);
    return new HashTableClient(fs, path, status, timeoutMs);
  }

  public FileWriter createFile(String path) throws TException {
    return createFile(path, DEFAULT_BACKING_PATH);
  }

  public FileWriter createFile(String path, String backingPath) throws TException {
    return createFile(path, backingPath, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH);
  }

  public FileWriter createFile(String path, String backingPath, int numBlocks,
      int chainLength) throws TException {
    return createFile(path, backingPath, numBlocks, chainLength, DEFAULT_FLAGS,
        DEFAULT_PERMISSIONS, DEFAULT_TAGS);
  }

  public FileWriter createFile(String path, String backingPath, int numBlocks,
      int chainLength, int flags, int permissions, Map<String, String> tags) throws TException {
    rpc_data_status status = initDataStructure(InitType.CREATE, path, "file", backingPath,
        numBlocks, chainLength, flags, permissions, tags);
    return new FileWriter(fs, path, status, timeoutMs);
  }

  public FileReader openFile(String path) throws TException {
    rpc_data_status status = fs.open(path);
    beginScope(path);
    return new FileReader(fs, path, status, timeoutMs);
  }

  public FileWriter openOrCreateFile(String path) throws TException {
    return openOrCreateFile(path, DEFAULT_BACKING_PATH);
  }

  public FileWriter openOrCreateFile(String path, String backingPath) throws TException {
    return openOrCreateFile(path, backingPath, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH);
  }

  public FileWriter openOrCreateFile(String path, String backingPath, int numBlocks,
      int chainLength)
      throws TException {
    return openOrCreateFile(path, backingPath, numBlocks, chainLength, DEFAULT_FLAGS,
        DEFAULT_PERMISSIONS, DEFAULT_TAGS);
  }

  public FileWriter openOrCreateFile(String path, String backingPath, int numBlocks,
      int chainLength, int flags, int permissions,
      Map<String, String> tags) throws TException {
    rpc_data_status status = initDataStructure(InitType.OPEN_OR_CREATE, path, "file",
        backingPath, numBlocks, chainLength, flags, permissions, tags);
    return new FileWriter(fs, path, status, timeoutMs);
  }

  public HashTableListener listenOnHashTable(String path) throws TException {
    rpc_data_status status = fs.open(path);
    beginScope(path);
    return new HashTableListener(path, status);
  }

  public void remove(String path) throws TException {
    endScope(path);
    fs.remove(path);
  }

  public void removeAll(String path) throws TException {
    worker.removePaths(path);
    fs.removeAll(path);
  }

  public void rename(String oldPath, String newPath) throws TException {
    fs.rename(oldPath, newPath);
    worker.renamePath(oldPath, newPath);
  }

  public void sync(String path, String backingPath) throws TException {
    fs.sync(path, backingPath);
  }

  public void dump(String path, String backingPath) throws TException {
    fs.dump(path, backingPath);
  }

  public void load(String path, String backingPath) throws TException {
    fs.load(path, backingPath);
  }

  public void close(String path) {
    endScope(path);
  }

  @Override
  public void close() throws IOException {
    worker.stop();
    try {
      workerThread.join();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    transport.close();
  }
}
