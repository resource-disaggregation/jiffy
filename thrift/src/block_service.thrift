namespace cpp jiffy.storage
namespace py jiffy.storage
namespace java jiffy.storage

exception block_exception {
  1: string msg
}

exception chain_failure_exception {
  1: string msg
}

struct sequence_id {
  1: required i64 client_id,
  2: required i64 client_seq_no,
  3: required i64 server_seq_no,
}

enum response_type {
  subscribe,
  unsubscribe
}

service block_request_service {
  // Get client ID (at tail node)
  i64 get_client_id(),

  // Register client ID (at tail node)
  void register_client_id(1: i32 block_id, 2: i64 client_id),

  // Command request (at head node)
  oneway void command_request(1: sequence_id seq, 2: i32 block_id, 3: list<binary> args),

  // Chain request (at non-head node)
  oneway void chain_request(1: sequence_id seq, 2: i32 block_id, 3: list<binary> args),

  // Run command (at non-head node, for fault recovery)
  list<binary> run_command(1: i32 block_id, 2: list<binary> arguments),

  // Subscription requests
  oneway void subscribe(1: i32 block_id, 2: list<string> ops),
  oneway void unsubscribe(1: i32 block_id, 2: list<string> ops),
}

service block_response_service {
  // Command response (at tail node)
  oneway void response(1: sequence_id seq, 2: list<binary> result),

  // Chain acknowledgement (at non-head node)
  oneway void chain_ack(1: sequence_id seq),

  // Subscription -- Notification response
  oneway void notification(1: string op, 2: binary data),

  // Subscription -- Control response
  oneway void control(1: response_type type, 2: list<string> ops, 3: string error),
}
