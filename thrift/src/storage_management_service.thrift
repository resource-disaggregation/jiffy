namespace cpp jiffy.storage

exception storage_management_exception {
  1: string msg
}

service storage_management_service {
  void create_partition(1: i32 block_id, 2: string partition_type, 3: string partition_name,
                        5: string partition_metadata, 6: map<string, string> conf, 7: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void setup_chain(1: i32 block_id, 2: string path, 6: list<string> chain, 7: i32 chain_role, 8: string next_block_id, 9: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void destroy_partition(1: i32 block_id, 2: i32 block_seq_no)
      throws (1: storage_management_exception ex),

  string get_path(1: i32 block_id, 2: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void sync(1: i32 block_id, 2: string backing_path, 3: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void dump(1: i32 block_id, 2: string backing_path, 3: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void load(1: i32 block_id, 2: string backing_path, 3: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  i64 storage_capacity(1: i32 block_id, 2: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  i64 storage_size(1: i32 block_id, 2: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void resend_pending(1: i32 block_id, 2: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void forward_all(1: i32 block_id, 2: i32 block_seq_no)
    throws (1: storage_management_exception ex),

  void update_partition_data(1: i32 block_id, 2: string partition_name, 3: string partition_metadata, 4: i32 block_seq_no)
    throws (1: storage_management_exception ex),
}