package mmux.lease;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import mmux.util.ThriftClientPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseWorker implements Runnable {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private ThriftClientPool client;
  private List<String> toRenew;
  private AtomicBoolean exit;
  private AtomicLong renewalDurationMs;

  public LeaseWorker(String leaseHost, int leasePort) throws TException {
    this.exit = new AtomicBoolean();
    this.client = ThriftClientPool.build(leaseHost, leasePort);
    this.toRenew = new ArrayList<>();
    rpc_lease_ack ack = broker().renewLeases(toRenew);
    this.renewalDurationMs = new AtomicLong(ack.getLeasePeriodMs());
  }

  private lease_service.Iface broker() {
    return client.iface(lease_service.Client.class);
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
      } catch (Exception e) {
        if (e.getMessage() != null) {
          logger.warn(e.getMessage());
        } else {
          logger.warn("Got exception of type " + e.getClass().getSimpleName());
        }
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
      if (!toRenew.remove(oldPath)) {
        toRenew.replaceAll(p -> p.replace(oldPath, newPath));
      } else {
        toRenew.add(newPath);
      }
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