namespace cpp elasticmem.storage
namespace py elasticmem.storage

exception storage_management_rpc_exception {
  1: string msg
}

service storage_management_rpc_service {
  void flush(1: i32 block_id, 2: string persistent_store_prefix, 3: string path)
    throws (1: storage_management_rpc_exception ex),

  void load(1: i32 block_id, 2: string persistent_store_prefix, 3: string path)
    throws (1: storage_management_rpc_exception ex),

  void clear(1: i32 block_id)
    throws (1: storage_management_rpc_exception ex),

  i64 storage_capacity(1: i32 block_id)
    throws (1: storage_management_rpc_exception ex),

  i64 storage_size(1: i32 block_id)
    throws (1: storage_management_rpc_exception ex),
}