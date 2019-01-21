package jiffy.notification;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import jiffy.notification.event.Control;
import jiffy.notification.event.Error;
import jiffy.notification.event.KVOpKind;
import jiffy.notification.event.Notification;
import jiffy.notification.event.Success;

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
    notificationConsumer.accept(new Notification(new KVOpKind(op), data));
  }

  @Override
  public void control(response_type type, List<String> ops, String error) {
    if (error.isEmpty()) {
      controlConsumer.accept(new Success(ops, type));
    } else {
      controlConsumer.accept(new Error(error, type));
    }
  }
}
