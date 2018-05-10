namespace cpp mmux.storage
namespace py mmux.notification
namespace java mmux.notification

service notification_service {
  oneway void subscribe(1: i32 block_id, 2: list<string> ops),
  oneway void unsubscribe(1: i32 block_id, 2: list<string> ops),
}