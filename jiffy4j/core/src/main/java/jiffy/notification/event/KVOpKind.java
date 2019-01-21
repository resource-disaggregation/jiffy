package jiffy.notification.event;

import java.nio.file.WatchEvent.Kind;

public final class KVOpKind implements Kind<Notification> {

  private final String name;

  public KVOpKind(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Class<Notification> type() {
    return Notification.class;
  }
}