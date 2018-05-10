namespace cpp mmux.storage
namespace py mmux.subscription
namespace java mmux.notification

enum response_type {
  subscribe,
  unsubscribe
}

service subscription_service {
  oneway void notification(1: string op, 2: binary data),
  oneway void success(1: response_type type, 2: list<string> ops),
  oneway void error(1: response_type type, 2: string msg),
}