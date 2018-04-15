package elasticmem.notification;

import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import elasticmem.emfs.ElasticMemPath;
import elasticmem.emfs.ElasticMemWatchService;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.thrift.TException;

public final class KVOpWatchKey implements WatchKey {

  private final ElasticMemWatchService watcher;
  private final ElasticMemPath path;
  private final List<String> subscribedOps;

  private final AtomicReference<State> state = new AtomicReference<>(State.READY);
  private final AtomicBoolean valid = new AtomicBoolean(true);

  enum State {
    READY,
    SIGNALLED
  }

  public KVOpWatchKey(ElasticMemWatchService watcher,
      ElasticMemPath path, Kind<?>[] subscribedOps) throws TException, InterruptedException {
    this.watcher = watcher;
    this.path = path;
    this.subscribedOps = new ArrayList<>(subscribedOps.length);
    for (Kind<?> subscribedOp : subscribedOps) {
      this.subscribedOps.add(subscribedOp.name());
    }
    this.watcher.getSubscriber().subcribe(this.subscribedOps);
  }

  @Override
  public boolean isValid() {
    return watcher.isOpen() && valid.get();
  }

  @Override
  public List<WatchEvent<?>> pollEvents() {
    List<WatchEvent<?>> events = new ArrayList<>();
    this.watcher.getNotificationMailbox().drainTo(events);
    int overflowCount = this.watcher.getNotificationMailbox().resetOverflow();
    if (overflowCount != 0) {
      events.add(new WatchEvent<Object>() {
        @Override
        public Kind<Object> kind() {
          return OVERFLOW;
        }

        @Override
        public int count() {
          return overflowCount;
        }

        @Override
        public Object context() {
          return null;
        }
      });
    }
    return events;
  }

  @Override
  public boolean reset() {
    if (isValid() && state.compareAndSet(State.SIGNALLED, State.READY)) {
      // requeue if events are pending
      if (!this.watcher.getNotificationMailbox().isEmpty()) {
        signal();
      }
    }
    return isValid();
  }

  @Override
  public void cancel() {
    try {
      this.watcher.getSubscriber().unsubscribe(subscribedOps);
      valid.set(false);
      watcher.cancelled(this);
    } catch (TException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Watchable watchable() {
    return path;
  }

  State state() {
    return state.get();
  }

  private void signal() {
    if (state.getAndSet(State.SIGNALLED) == State.READY) {
      watcher.enqueue(this);
    }
  }
}
