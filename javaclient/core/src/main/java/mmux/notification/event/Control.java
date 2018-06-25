package mmux.notification.event;

import mmux.notification.response_type;

public class Control {

  private response_type type;

  Control(response_type type) {
    this.type = type;
  }

  public response_type getType() {
    return type;
  }
}
