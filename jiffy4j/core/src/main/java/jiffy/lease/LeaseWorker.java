package jiffy.lease;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LeaseWorker implements Runnable, Closeable {

  private Logger logger = LoggerFactory.getLogger(getClass());
  private TTransport transport;
  private lease_service.Client client;
  private List<String> toRenew;
  private AtomicBoolean exit;
  private AtomicLong renewalDurationMs;

  public LeaseWorker(String leaseHost, int leasePort) throws TException {
    this.exit = new AtomicBoolean(false);
    this.transport = new TSocket(leaseHost, leasePort);
    this.client = new lease_service.Client(new TBinaryProtocol(transport));
    this.toRenew = new ArrayList<>();
    transport.open();
    rpc_lease_ack ack = client.renewLeases(toRenew);
    this.renewalDurationMs = new AtomicLong(ack.getLeasePeriodMs());
  }

  @Override
  public void run() {
    while (!exit.get()) {
      try {
        long start = System.currentTimeMillis();
        synchronized (this) {
          if (!toRenew.isEmpty()) {
            rpc_lease_ack ack = client.renewLeases(toRenew);
            renewalDurationMs.set(ack.getLeasePeriodMs());
          }
        }
        long elapsed = System.currentTimeMillis() - start;
        long sleepTime = renewalDurationMs.get() - elapsed;
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      } catch (lease_service_exception e) {
        String msg = e.getMsg();
        if (msg != null) {
          if (msg.startsWith("Path does not exist: ")) {
            String path = msg.replace("Path does not exist: ", "");
            logger.info("No longer renewing leases for: " + path);
            synchronized (this) {
              toRenew.remove(path);
            }
          } else {
            logger.warn(msg);
          }
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

  @Override
  public void close() {
    if (transport != null && transport.isOpen()) {
      transport.close();
    }
  }

  public List<String> getPaths() {
    synchronized (this) {
      return new ArrayList<>(toRenew);
    }
  }
}