namespace cpp elasticmem.storage

exception storage_management_exception {
  1: string msg
}

service storage_management_service {
  void setup_block(1: i32 block_id, 2: string path, 3: i32 chain_role, 4: string next_block_name)
    throws (1: storage_management_exception ex),

  string get_path(1: i32 block_id)
    throws (1: storage_management_exception ex),

  void flush(1: i32 block_id, 2: string persistent_store_prefix, 3: string path)
    throws (1: storage_management_exception ex),

  void load(1: i32 block_id, 2: string persistent_store_prefix, 3: string path)
    throws (1: storage_management_exception ex),

  void reset(1: i32 block_id)
    throws (1: storage_management_exception ex),

  i64 storage_capacity(1: i32 block_id)
    throws (1: storage_management_exception ex),

  i64 storage_size(1: i32 block_id)
    throws (1: storage_management_exception ex),

  void resend_pending(1: i32 block_id)
    throws (1: storage_management_exception ex),

  void forward_all(1: i32 block_id)
    throws (1: storage_management_exception ex),
}