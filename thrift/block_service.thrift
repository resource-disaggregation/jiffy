namespace cpp mmux.storage
namespace py mmux.kv
namespace java mmux.kv

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

service block_request_service {
  i64 get_client_id(),
  void register_client_id(1: i32 block_id, 2: i64 client_id),
  oneway void command_request(1: sequence_id seq, 2: i32 block_id, 3: list<i32> cmd_ids, 4: list<list<binary>> arguments),
}

service block_response_service {
  oneway void response(1: sequence_id seq, 2: list<list<binary>> result),
}

service chain_request_service {
  list<list<binary>> run_command(1: i32 block_id, 2: list<i32> cmd_id, 3: list<list<binary>> arguments),
  oneway void chain_request(1: sequence_id seq, 2: i32 block_id, 3: list<i32> cmd_ids, 4: list<list<binary>> arguments),
}

service chain_response_service {
  oneway void chain_ack(1: sequence_id seq),
}
