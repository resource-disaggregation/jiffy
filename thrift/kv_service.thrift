namespace cpp elasticmem.kv
namespace py elasticmem.kv

exception kv_rpc_exception {
  1: string msg
}

exception kv_management_rpc_exception {
  1: string msg
}

service kv_rpc_service {
  void put(1: binary key, 2: binary value)
    throws (1: kv_rpc_exception ex),

  binary get(1: binary key)
    throws (1: kv_rpc_exception ex),

  binary update(1: binary key, 2: binary value)
    throws (1: kv_rpc_exception ex),

  void remove(1: binary key)
    throws (1: kv_rpc_exception ex),
}

service kv_management_rpc_service {
  void clear()
    throws (1: kv_management_rpc_exception ex),

  i64 size()
    throws (1: kv_management_rpc_exception ex),

  i64 capacity()
    throws (1: kv_management_rpc_exception ex),

  i64 num_entries()
    throws (1: kv_management_rpc_exception ex),
}
