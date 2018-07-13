namespace cpp mmux.directory
namespace py mmux.directory
namespace java mmux.directory

struct heartbeat {
	1: i64 timestamp,
  2: string sender,
}

service heartbeat_service {
  void ping(1: heartbeat hb)
}