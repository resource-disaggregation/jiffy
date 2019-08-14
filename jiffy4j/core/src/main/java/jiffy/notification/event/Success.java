package jiffy.notification.event;

import jiffy.storage.response_type;
import java.util.List;

public class Success extends Control {

  private List<String> ops;

  public Success(List<String> ops, response_type type) {
    super(type);
    this.ops = ops;
  }

  public List<String> getOps() {
    return ops;
  }
}