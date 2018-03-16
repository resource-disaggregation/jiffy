namespace cpp elasticmem.storage
namespace py elasticmem.kv

exception kv_rpc_exception {
  1: string msg
}

service kv_rpc_service {
  void put(1: i32 block_id, 2: binary key, 3: binary value)
    throws (1: kv_rpc_exception ex),

  binary get(1: i32 block_id, 2: binary key)
    throws (1: kv_rpc_exception ex),

  void update(1: i32 block_id, 2: binary key, 3: binary value)
    throws (1: kv_rpc_exception ex),

  void remove(1: i32 block_id, 2: binary key)
    throws (1: kv_rpc_exception ex),

  i64 size(1: i32 block_id)
    throws (1: kv_rpc_exception ex),
}
