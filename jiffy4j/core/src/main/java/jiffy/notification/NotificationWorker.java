package jiffy.notification;

import jiffy.notification.event.Control;
import jiffy.notification.event.Notification;
import jiffy.storage.block_response_service.Processor;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class NotificationWorker implements Runnable {

  private Logger logger = Logger.getLogger(NotificationWorker.class.getName());
  private List<TProtocol> protocols;
  private TProcessor processor;
  private AtomicBoolean exit;

  NotificationWorker(Consumer<Notification> notificationConsumer,
      Consumer<Control> controlConsumer) {
    this.protocols = new ArrayList<>();
    this.processor = new Processor<>(new NotificationHandler(notificationConsumer, controlConsumer));
    this.exit = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    try {
      while (!exit.get()) {
        for (TProtocol protocol : protocols) {
          processor.process(protocol, protocol);
        }
      }
    } catch (TException e) {
      logger.info("Notification worker terminating, reason: " + e.getMessage());
    } finally {
      exit.set(true);
    }
  }

  public void addProtocol(TProtocol protocol) {
    this.protocols.add(protocol);
  }

  public void stop() {
    exit.set(true);
  }

  public boolean isStopped() {
    return exit.get();
  }
}
