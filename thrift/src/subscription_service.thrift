namespace cpp jiffy.storage
namespace py jiffy.subscription
namespace java jiffy.notification

enum response_type {
  subscribe,
  unsubscribe
}

service subscription_service {
  oneway void notification(1: string op, 2: binary data),
  oneway void control(1: response_type type, 2: list<string> ops, 3: string error),
}