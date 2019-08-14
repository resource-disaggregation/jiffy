package jiffy.notification;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jiffy.directory.rpc_data_status;
import jiffy.directory.rpc_replica_chain;
import jiffy.storage.BlockNameParser;
import jiffy.storage.BlockNameParser.BlockMetadata;
import jiffy.notification.event.Control;
import jiffy.notification.event.Notification;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class HashTableListener implements Closeable {

  private Mailbox<Notification> notifications;
  private Mailbox<Control> controls;
  private NotificationWorker worker;
  private Thread workerThread;
  private BlockListener[] listeners;
  private int[] blockIds;

  public HashTableListener(String path, rpc_data_status status) throws TTransportException {
    this.notifications = new Mailbox<>();
    this.controls = new Mailbox<>();
    this.blockIds = new int[status.data_blocks.size()];
    this.listeners = new BlockListener[status.data_blocks.size()];
    this.worker = new NotificationWorker(notifications, controls);
    int i = 0;
    for (rpc_replica_chain block: status.data_blocks) {
      BlockMetadata t = BlockNameParser.parse(block.block_ids.get(block.block_ids.size() - 1));
      blockIds[i] = t.getBlockId();
      listeners[i] = new BlockListener(t.getHost(), t.getServicePort(), controls);
      worker.addProtocol(listeners[i].getProtocol());
    }
    workerThread = new Thread(worker);
    workerThread.start();
  }

  @Override
  public void close() throws IOException {
    for (BlockListener listener: listeners) {
      listener.close();
    }
    worker.stop();
    try {
      workerThread.join();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void subscribe(List<String> ops) throws TException, InterruptedException {
    for (int i = 0; i < listeners.length; i++) {
      listeners[i].subscribe(blockIds[i], ops);
    }
  }

  public void unsubscribe(List<String> ops) throws TException, InterruptedException {
    for (int i = 0; i < listeners.length; i++) {
      listeners[i].unsubscribe(blockIds[i], ops);
    }
  }

  public Notification getNotification(int timeout, TimeUnit unit) throws InterruptedException {
    return notifications.poll(timeout, unit);
  }

  public Notification getNotification() throws InterruptedException {
    return notifications.take();
  }
}
