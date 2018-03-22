namespace cpp elasticmem.storage
namespace py elasticmem.kv

exception block_exception {
  1: string msg
}

service block_service {
  list<string> run_command(1: i32 block_id, 2: i32 cmd_id, 3: list<string> arguments)
    throws (1: block_exception ex),
}
