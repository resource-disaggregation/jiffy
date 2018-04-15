package elasticmem.notification;

import elasticmem.directory.rpc_data_status;
import elasticmem.notification.event.Control;
import elasticmem.notification.event.Error;
import elasticmem.notification.event.Notification;
import elasticmem.notification.notification_service.Client;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Subscriber {

  private int[] blockIds;
  private TTransport[] transports;
  private notification_service.Client[] clients;
  private Mailbox<Control> controlMailbox;
  private NotificationWorker worker;

  public Subscriber(rpc_data_status status, Consumer<Notification> notificationConsumer)
      throws TTransportException {
    blockIds = new int[status.data_blocks.size()];
    transports = new TTransport[status.data_blocks.size()];
    TProtocol[] protocols = new TProtocol[status.data_blocks.size()];
    clients = new Client[status.data_blocks.size()];
    for (int i = 0; i < status.data_blocks.size(); i++) {
      String[] blockP = status.data_blocks.get(i).block_names.get(status.chain_length - 1)
          .split(":");
      blockIds[i] = Integer.parseInt(blockP[5]);
      transports[i] = new TSocket(blockP[0], Integer.parseInt(blockP[3]));
      protocols[i] = new TBinaryProtocol(transports[i]);
      clients[i] = new Client(protocols[i]);
      transports[i].open();
    }
    this.controlMailbox = new Mailbox<>();
    this.worker = new NotificationWorker(protocols, notificationConsumer, controlMailbox);
  }

  public void close() {
    worker.stop();
    for (TTransport transport : transports) {
      if (transport.isOpen()) {
        transport.close();
      }
    }
  }

  public void subcribe(List<String> ops) throws TException, InterruptedException {
    for (int i = 0; i < clients.length; i++) {
      clients[i].subscribe(blockIds[i], ops);
    }
    processResponse();
  }

  public void unsubscribe(List<String> ops) throws TException, InterruptedException {
    for (int i = 0; i < clients.length; i++) {
      clients[i].unsubscribe(blockIds[i], ops);
    }
    processResponse();
  }

  private void processResponse() throws InterruptedException, TException {
    for (Client client : clients) {
      Control c = controlMailbox.poll(10, TimeUnit.SECONDS);
      if (c == null) {
        throw new TException("One or more storage servers did not respond to subscription request");
      }
      if (c instanceof Error) {
        throw new TException("Subscription failure");
      }
    }
  }

}
