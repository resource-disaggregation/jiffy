package mmux;

import com.github.phantomthief.thrift.client.ThriftClient;
import com.github.phantomthief.thrift.client.impl.ThriftClientImpl;
import com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider;
import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import com.github.phantomthief.thrift.client.pool.impl.DefaultThriftConnectionPoolImpl;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import mmux.directory.directory_service;
import mmux.directory.rpc_data_status;
import mmux.kv.KVClient;
import mmux.lease.LeaseWorker;
import mmux.notification.KVListener;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class MMuxClient implements Closeable {

  private static final String DEFAULT_BACKING_PATH = "local://tmp";
  private static final int DEFAULT_NUM_BLOCKS = 1;
  private static final int DEFAULT_CHAIN_LENGTH = 1;
  private static final int DEFAULT_FLAGS = 0;
  private static final int DEFAULT_TIMEOUT_MS = 5000;

  private ThriftClient fs;
  private LeaseWorker worker;
  private Thread workerThread;
  private int timeoutMs;

  public MMuxClient(String host, int dirPort, int leasePort) throws TException {
    this(host, dirPort, leasePort, DEFAULT_TIMEOUT_MS);
  }

  public MMuxClient(String host, int dirPort, int leasePort, int timeoutMs) throws TException {
    this.timeoutMs = timeoutMs;
    fs = makeFS(host, dirPort);
    worker = new LeaseWorker(host, leasePort);
    workerThread = new Thread(worker);
    workerThread.start();
  }

  private ThriftClient makeFS(String host, int port) {
    Function<ThriftServerInfo, TTransport> tProvider = info -> {
      TSocket socket = new TSocket(info.getHost(), info.getPort());
      return new TFramedTransport(socket);
    };
    GenericKeyedObjectPoolConfig pConf = new GenericKeyedObjectPoolConfig();
    ThriftServerInfo info = ThriftServerInfo.of(host, port);
    Supplier<List<ThriftServerInfo>> iProvider = () -> Collections.singletonList(info);
    ThriftConnectionPoolProvider pProvider = new DefaultThriftConnectionPoolImpl(pConf, tProvider);
    return new ThriftClientImpl(iProvider, pProvider);
  }

  public directory_service.Iface fs() {
    return fs.iface(directory_service.Client.class, TBinaryProtocol::new, 0);
  }

  LeaseWorker getWorker() {
    return worker;
  }

  public void beginScope(String path) {
    worker.addPath(path);
  }

  public void endScope(String path) {
    worker.removePath(path);
  }

  public KVClient create(String path) throws TException {
    return create(path, DEFAULT_BACKING_PATH, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH,
        DEFAULT_FLAGS);
  }

  public KVClient create(String path, String backingPath) throws TException {
    return create(path, backingPath, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH, DEFAULT_FLAGS);
  }

  public KVClient create(String path, String backingPath, int numBlocks, int chainLength)
      throws TException {
    return create(path, backingPath, numBlocks, chainLength, DEFAULT_FLAGS);
  }

  public KVClient create(String path, String backingPath, int numBlocks, int chainLength, int flags)
      throws TException {
    rpc_data_status status = fs().create(path, backingPath, numBlocks, chainLength, flags);
    beginScope(path);
    return new KVClient(fs, path, status, timeoutMs);
  }

  public KVClient open(String path) throws TException {
    rpc_data_status status = fs().open(path);
    beginScope(path);
    return new KVClient(fs, path, status, timeoutMs);
  }

  public KVClient openOrCreate(String path) throws TException {
    return openOrCreate(path, DEFAULT_BACKING_PATH, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH,
        DEFAULT_FLAGS);
  }

  public KVClient openOrCreate(String path, String backingPath) throws TException {
    return openOrCreate(path, backingPath, DEFAULT_NUM_BLOCKS, DEFAULT_CHAIN_LENGTH, DEFAULT_FLAGS);
  }

  public KVClient openOrCreate(String path, String backingPath, int numBlocks, int chainLength)
      throws TException {
    return openOrCreate(path, backingPath, numBlocks, chainLength, DEFAULT_FLAGS);
  }

  public KVClient openOrCreate(String path, String backingPath, int numBlocks,
      int chainLength, int flags) throws TException {
    rpc_data_status status = fs().openOrCreate(path, backingPath, numBlocks, chainLength, flags);
    beginScope(path);
    return new KVClient(fs, path, status, timeoutMs);
  }

  public KVListener listen(String path) throws TException {
    rpc_data_status status = fs().open(path);
    beginScope(path);
    return new KVListener(path, status);
  }

  public void remove(String path) throws TException {
    endScope(path);
    fs().remove(path);
  }

  public void removeAll(String path) throws TException {
    worker.removePaths(path);
    fs().removeAll(path);
  }

  public void rename(String oldPath, String newPath) throws TException {
    fs().rename(oldPath, newPath);
    worker.renamePath(oldPath, newPath);
  }

  public void sync(String path, String backingPath) throws TException {
    fs().sync(path, backingPath);
  }

  public void dump(String path, String backingPath) throws TException {
    fs().dump(path, backingPath);
  }

  public void load(String path, String backingPath) throws TException {
    fs().load(path, backingPath);
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
  }
}
