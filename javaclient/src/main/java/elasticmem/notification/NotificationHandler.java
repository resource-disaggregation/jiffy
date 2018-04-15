package elasticmem.notification;

import elasticmem.notification.event.Control;
import elasticmem.notification.event.Error;
import elasticmem.notification.event.KvOpKind;
import elasticmem.notification.event.Notification;
import elasticmem.notification.event.Success;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

public class NotificationHandler implements subscription_service.Iface {

  private Consumer<Notification> notificationConsumer;
  private Consumer<Control> controlConsumer;

  NotificationHandler(Consumer<Notification> notificationConsumer,
      Consumer<Control> controlConsumer) {
    this.notificationConsumer = notificationConsumer;
    this.controlConsumer = controlConsumer;
  }

  @Override
  public void notification(String op, ByteBuffer data) {
    notificationConsumer.accept(new Notification(new KvOpKind(op), data));
  }

  @Override
  public void success(response_type type, List<String> ops) {
    controlConsumer.accept(new Success(ops, type));
  }

  @Override
  public void error(response_type type, String msg) {
    controlConsumer.accept(new Error(msg, type));
  }
}
