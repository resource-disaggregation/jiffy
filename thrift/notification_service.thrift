namespace cpp elasticmem.storage
namespace py elasticmem.notification
namespace java elasticmem.notification

service notification_service {
  oneway void subscribe(1: i32 block_id, 2: list<string> ops),
  oneway void unsubscribe(1: i32 block_id, 2: list<string> ops),
}