package mmux.lease;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class LeaseWorker implements Runnable {

  private TTransport transport;
  private lease_service.Client client;
  private List<String> toRenew;
  private AtomicBoolean exit;
  private AtomicLong renewalDurationMs;
  private Logger logger = Logger.getLogger(LeaseWorker.class.getName());

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
      if (toRenew.contains(path)) {
        toRenew.remove(path);
      }
    }
  }

  public boolean hasPath(String path) {
    synchronized (this) {
      return toRenew.contains(path);
    }
  }
}