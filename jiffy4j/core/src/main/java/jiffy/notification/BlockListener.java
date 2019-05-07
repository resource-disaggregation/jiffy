package jiffy.notification;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jiffy.notification.event.Control;
import jiffy.notification.event.Error;
import jiffy.storage.block_request_service.Client;
import jiffy.storage.block_request_service;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BlockListener implements Closeable {

  private Mailbox<Control> controls;

  private TTransport transport;
  private TProtocol protocol;
  private block_request_service.Client client;

  BlockListener(String host, int port, Mailbox<Control> controls) throws TTransportException {
    this.transport = new TFramedTransport(new TSocket(host, port));
    this.protocol = new TBinaryProtocol(transport);
    this.client = new Client(protocol);
    this.controls = controls;
    transport.open();
  }

  public TProtocol getProtocol() {
    return protocol;
  }

  public void subscribe(int blockId, List<String> ops) throws TException, InterruptedException {
    client.subscribe(blockId, ops);
    Control control = controls.poll(3, TimeUnit.SECONDS);
    if (control instanceof Error) {
      throw new RuntimeException(((Error) control).getMessage());
    }
  }

  public void unsubscribe(int blockId, List<String> ops) throws TException, InterruptedException {
    client.unsubscribe(blockId, ops);
    Control control = controls.poll(3, TimeUnit.SECONDS);
    if (control instanceof Error) {
      throw new RuntimeException(((Error) control).getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    this.transport.close();
  }
}
