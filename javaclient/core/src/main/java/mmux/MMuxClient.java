package mmux;

import java.io.Closeable;
import java.io.IOException;
import mmux.directory.directory_service;
import mmux.directory.directory_service.Client;
import mmux.directory.rpc_data_status;
import mmux.kv.KVClient;
import mmux.lease.LeaseWorker;
import mmux.notification.KVListener;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MMuxClient implements Closeable {
  private TTransport transport;
  private directory_service.Client fs;
  private LeaseWorker worker;
  private Thread workerThread;

  public MMuxClient(String host, int dirPort, int leasePort) throws TException {
    transport = new TSocket(host, dirPort);
    fs = new Client(new TBinaryProtocol(transport));
    transport.open();
    worker = new LeaseWorker(host, leasePort);
    workerThread = new Thread(worker);
    workerThread.start();
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

  public KVClient create(String path, String persistentStorePrefix, int numBlocks, int chainLength)
      throws TException {
    rpc_data_status status = fs.create(path, persistentStorePrefix, numBlocks, chainLength);
    beginScope(path);
    return new KVClient(fs, path, status);
  }

  public KVClient open(String path) throws TException {
    rpc_data_status status = fs.open(path);
    beginScope(path);
    return new KVClient(fs, path, status);
  }

  public KVClient openOrCreate(String path, String persistentStorePrefix, int numBlocks, int chainLength) throws TException {
    rpc_data_status status = fs.openOrCreate(path, persistentStorePrefix, numBlocks, chainLength);
    beginScope(path);
    return new KVClient(fs, path, status);
  }

  public KVListener listen(String path) throws TException {
    rpc_data_status status = fs.open(path);
    beginScope(path);
    return new KVListener(path, status);
  }

  public void remove(String path) throws TException {
    endScope(path);
    fs.remove(path);
  }

  public void flush(String path, String dest) throws TException {
    endScope(path);
    fs.flush(path, dest);
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
