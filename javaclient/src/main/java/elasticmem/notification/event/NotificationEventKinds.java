package elasticmem.notification.event;

import java.nio.file.WatchEvent;

public class NotificationEventKinds {

  public static final WatchEvent.Kind<Notification> ENTRY_GET = new KvOpKind("get");
  public static final WatchEvent.Kind<Notification> ENTRY_PUT = new KvOpKind("put");
  public static final WatchEvent.Kind<Notification> ENTRY_REMOVE = new KvOpKind("remove");
  public static final WatchEvent.Kind<Notification> ENTRY_UPDATE = new KvOpKind("update");

}
