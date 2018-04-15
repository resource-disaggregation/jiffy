package elasticmem.emfs;

import elasticmem.notification.KVOpWatchKey;
import elasticmem.notification.Mailbox;
import elasticmem.notification.Subscriber;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.TException;

public class ElasticMemWatchService implements WatchService {

  private final Subscriber subscriber;
  private final Mailbox<WatchEvent<?>> notificationMailbox;
  private final AtomicBoolean open = new AtomicBoolean(true);
  private final BlockingQueue<WatchKey> queue = new LinkedBlockingQueue<>();
  private final WatchKey poison = new WatchKey() {
    @Override
    public boolean isValid() {
      return false;
    }

    @Override
    public List<WatchEvent<?>> pollEvents() {
      return null;
    }

    @Override
    public boolean reset() {
      return false;
    }

    @Override
    public void cancel() {
    }

    @Override
    public Watchable watchable() {
      return null;
    }
  };

  ElasticMemWatchService(Subscriber subscriber, Mailbox<WatchEvent<?>> notificationMailbox) {
    this.subscriber = subscriber;
    this.notificationMailbox = notificationMailbox;
  }

  @Override
  public void close() {
    if (open.compareAndSet(true, false)) {
      queue.clear();
      queue.offer(poison);
    }
  }

  @Override
  public WatchKey poll() {
    if (!isOpen()) {
      throw new ClosedWatchServiceException();
    }
    return check(queue.poll());
  }

  @Override
  public WatchKey poll(long timeout, TimeUnit unit) throws InterruptedException {
    if (!isOpen()) {
      throw new ClosedWatchServiceException();
    }
    return check(queue.poll(timeout, unit));
  }

  @Override
  public WatchKey take() throws InterruptedException {
    if (!isOpen()) {
      throw new ClosedWatchServiceException();
    }
    return check(queue.take());
  }

  private ElasticMemPath toEmPath(Watchable watchable) {
    if (!(watchable instanceof ElasticMemPath)) {
      throw new IllegalArgumentException(
          "Can only accept watchable of type " + ElasticMemPath.class.getName());
    }
    return (ElasticMemPath) watchable;
  }

  public WatchKey register(Watchable watchable, WatchEvent.Kind<?>[] eventTypes)
      throws IOException {
    if (!isOpen()) {
      throw new ClosedWatchServiceException();
    }
    try {
      return new KVOpWatchKey(this, toEmPath(watchable), eventTypes);
    } catch (TException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  public Subscriber getSubscriber() {
    return subscriber;
  }

  public Mailbox<WatchEvent<?>> getNotificationMailbox() {
    return notificationMailbox;
  }

  public boolean isOpen() {
    return open.get();
  }

  public void enqueue(KVOpWatchKey kvOpWatchKey) {
    if (isOpen()) {
      queue.add(kvOpWatchKey);
    }
  }

  public void cancelled(KVOpWatchKey kvOpWatchKey) {
    // Do nothing
  }

  private WatchKey check(WatchKey key) {
    if (key == poison) {
      throw new ClosedWatchServiceException();
    }
    return key;
  }
}
