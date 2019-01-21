namespace cpp jiffy.storage

exception storage_management_exception {
  1: string msg
}

service storage_management_service {
  void setup_block(1: i32 block_id, 2: string path, 3: string partition_type, 4: string partition_name,
                   5: string partition_metadata, 6: list<string> chain, 7: i32 chain_role, 8: string next_block_name)
    throws (1: storage_management_exception ex),

  string get_path(1: i32 block_id)
    throws (1: storage_management_exception ex),

  void sync(1: i32 block_id, 2: string backing_path)
    throws (1: storage_management_exception ex),

  void dump(1: i32 block_id, 2: string backing_path)
    throws (1: storage_management_exception ex),

  void load(1: i32 block_id, 2: string backing_path)
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