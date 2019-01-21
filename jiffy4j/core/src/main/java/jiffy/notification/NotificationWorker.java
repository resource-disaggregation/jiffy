package jiffy.notification;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import jiffy.notification.event.Control;
import jiffy.notification.event.Notification;
import jiffy.notification.subscription_service.Processor;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;

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
    } catch (TTransportException ignored) {

    } catch (TException e) {
      logger.severe("Error: " + e.getMessage());
      throw new RuntimeException(e);
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
