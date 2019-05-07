package jiffy.notification;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import jiffy.notification.event.Control;
import jiffy.notification.event.Error;
import jiffy.notification.event.KVOpKind;
import jiffy.notification.event.Notification;
import jiffy.notification.event.Success;
import jiffy.storage.block_response_service;
import jiffy.storage.sequence_id;
import org.apache.thrift.TException;

public class NotificationHandler implements block_response_service.Iface {

  private Consumer<Notification> notificationConsumer;
  private Consumer<Control> controlConsumer;

  NotificationHandler(Consumer<Notification> notificationConsumer,
      Consumer<Control> controlConsumer) {
    this.notificationConsumer = notificationConsumer;
    this.controlConsumer = controlConsumer;
  }

  @Override
  public void response(sequence_id seq, List<ByteBuffer> result) throws TException {
    throw new TException("NotificationHandler does not support response");
  }

  @Override
  public void chainAck(sequence_id seq) throws TException {
    throw new TException("NotificationHandler does not support chainAck");
  }

  @Override
  public void notification(String op, ByteBuffer data) {
    notificationConsumer.accept(new Notification(new KVOpKind(op), data));
  }

  @Override
  public void control(jiffy.storage.response_type type, List<String> ops, String error) throws TException {
    if (error.isEmpty()) {
      controlConsumer.accept(new Success(ops, type));
    } else {
      controlConsumer.accept(new Error(error, type));
    }
  }
}
