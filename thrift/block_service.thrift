namespace cpp elasticmem.storage
namespace py elasticmem.kv

exception block_exception {
  1: string msg
}

exception chain_failure_exception {
  1: string msg
}

service block_service {
  list<string> run_command(1: i64 seq_no, 2: i32 block_id, 3: i32 cmd_id, 4: list<string> arguments)
    throws (1: block_exception be, 2: chain_failure_exception cfe),
}
