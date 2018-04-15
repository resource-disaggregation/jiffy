package elasticmem.directory;

import elasticmem.lease.lease_service;
import elasticmem.lease.rpc_lease_ack;
import java.util.List;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

public class LeaseWorker implements Runnable {

  private lease_service.Client client;
  private List<String> toRenew;
  private volatile boolean exit = false;
  private long renewalDuration;
  private Logger logger = Logger.getLogger(LeaseWorker.class.getName());

  public LeaseWorker(String leaseHost, int leasePort, List<String> toRenew) {
    this.client = new lease_service.Client(new TBinaryProtocol(new TSocket(leaseHost, leasePort)));
    this.toRenew = toRenew;
    this.renewalDuration = 10000;
  }

  @Override
  public void run() {
    while (!exit) {
      try {
        long start = System.currentTimeMillis();
        if (!toRenew.isEmpty()) {
          rpc_lease_ack ack = client.renewLeases(toRenew);
          renewalDuration = ack.getLeasePeriodMs();
          if (ack.getRenewed() != toRenew.size()) {
            logger.warning(
                "Asked to renew " + toRenew.size() + ", server renewed " + ack.getRenewed());
          }
          long elapsed = System.currentTimeMillis() - start;
          long sleepTime = renewalDuration - elapsed;
          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        }
      } catch (TException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void stop() {
    exit = true;
  }
}
