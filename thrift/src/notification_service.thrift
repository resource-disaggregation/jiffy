namespace cpp jiffy.storage
namespace py jiffy.notification
namespace java jiffy.notification

service notification_service {
  oneway void subscribe(1: i32 block_id, 2: list<string> ops),
  oneway void unsubscribe(1: i32 block_id, 2: list<string> ops),
}