package elasticmem.notification.event;

import elasticmem.notification.response_type;

public class Control {

  private response_type type;

  Control(response_type type) {
    this.type = type;
  }

  public response_type getType() {
    return type;
  }
}
