package jiffy.notification.event;

import java.nio.ByteBuffer;
import java.nio.file.WatchEvent;

public class Notification implements WatchEvent<Notification> {
  private Kind<Notification> kind;
  private ByteBuffer data;

  public Notification(Kind<Notification> kind, ByteBuffer data) {
    this.kind = kind;
    this.data = data;
  }

  public ByteBuffer getData() {
    return data;
  }

  @Override
  public Kind<Notification> kind() {
    return kind;
  }

  @Override
  public int count() {
    return 1;
  }

  @Override
  public Notification context() {
    return this;
  }
}
