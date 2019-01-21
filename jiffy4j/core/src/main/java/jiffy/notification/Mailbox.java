package jiffy.notification;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Mailbox<T> implements Consumer<T> {

  private BlockingQueue<T> queue;
  private AtomicInteger overflow;

  Mailbox() {
    queue = new LinkedBlockingQueue<>();
    overflow = new AtomicInteger(0);
  }

  Mailbox(int maxSize) {
    queue = new LinkedBlockingQueue<>(maxSize);
    overflow = new AtomicInteger(0);
  }

  @Override
  public void accept(T t) {
    if (!queue.offer(t)) {
      overflow.incrementAndGet();
    }
  }

  @Override
  public Consumer<T> andThen(Consumer<? super T> after) {
    return null;
  }

  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    return queue.poll(timeout, unit);
  }

  public T poll() {
    return queue.poll();
  }

  public T take() throws InterruptedException {
    return queue.take();
  }

  public void drainTo(Collection<T> c) {
    queue.drainTo(c);
  }

  public int resetOverflow() {
    return overflow.getAndSet(0);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
