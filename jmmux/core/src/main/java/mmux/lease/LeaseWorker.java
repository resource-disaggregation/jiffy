package mmux.lease;

import com.github.phantomthief.thrift.client.ThriftClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import mmux.util.ThriftPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

public class LeaseWorker implements Runnable {

  private ThriftClient client;
  private List<String> toRenew;
  private AtomicBoolean exit;
  private AtomicLong renewalDurationMs;

  public LeaseWorker(String leaseHost, int leasePort) throws TException {
    this.exit = new AtomicBoolean();
    this.client = ThriftPool.clientPool(leaseHost, leasePort);
    this.toRenew = new ArrayList<>();
    rpc_lease_ack ack = broker().renewLeases(toRenew);
    this.renewalDurationMs = new AtomicLong(ack.getLeasePeriodMs());
  }

  private lease_service.Iface broker() {
    return client.iface(lease_service.Client.class, TBinaryProtocol::new, 0);
  }

  @Override
  public void run() {
    while (!exit.get()) {
      try {
        long start = System.currentTimeMillis();
        synchronized (this) {
          if (!toRenew.isEmpty()) {
            rpc_lease_ack ack = broker().renewLeases(toRenew);
            renewalDurationMs.set(ack.getLeasePeriodMs());
          }
        }
        long elapsed = System.currentTimeMillis() - start;
        long sleepTime = renewalDurationMs.get() - elapsed;
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public long getRenewalDurationMs() {
    return renewalDurationMs.get();
  }

  public void stop() {
    exit.set(true);
  }

  public void addPath(String path) {
    synchronized (this) {
      if (!toRenew.contains(path)) {
        toRenew.add(path);
      }
    }
  }

  public void removePath(String path) {
    synchronized (this) {
      toRenew.remove(path);
    }
  }

  public void renamePath(String oldPath, String newPath) {
    synchronized (this) {
      toRenew.remove(oldPath);
      toRenew.add(newPath);
    }
  }

  public boolean hasPath(String path) {
    synchronized (this) {
      return toRenew.contains(path);
    }
  }

  public void removePaths(String path) {
    synchronized (this) {
      toRenew.removeIf(p -> p.startsWith(path));
    }
  }
}