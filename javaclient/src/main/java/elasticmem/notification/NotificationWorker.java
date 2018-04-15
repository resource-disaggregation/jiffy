package elasticmem.notification;

import elasticmem.notification.event.Control;
import elasticmem.notification.event.Notification;
import elasticmem.notification.subscription_service.Processor;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;

public class NotificationWorker implements Runnable {

  private Logger logger = Logger.getLogger(NotificationWorker.class.getName());
  private TProtocol[] protocols;
  private TProcessor processor;
  private volatile boolean exit;

  NotificationWorker(TProtocol[] protocols, Consumer<Notification> notificationConsumer,
      Consumer<Control> controlConsumer) {
    this.protocols = protocols;
    NotificationHandler handler = new NotificationHandler(notificationConsumer, controlConsumer);
    this.processor = new Processor<>(handler);
    this.exit = false;
  }

  @Override
  public void run() {
    try {
      while (!exit) {
        for (TProtocol protocol : protocols) {
          processor.process(protocol, protocol);
        }
      }
    } catch (TTransportException e) {
      logger.info("Thrift transport closed");
    } catch (TException e) {
      logger.severe("Error: " + e.getMessage());
      throw new RuntimeException(e);
    } finally {
      exit = true;
    }
  }

  public void stop() {
    exit = true;
  }

  public boolean isStopped() {
    return exit;
  }
}
