namespace cpp mmux.storage

exception storage_management_exception {
  1: string msg
}

enum rpc_block_state {
  rpc_regular = 0,
  rpc_importing = 1,
  rpc_exporting = 2,
}

struct rpc_slot_range {
  1: required i32 slot_begin,
  2: required i32 slot_end,
}

service storage_management_service {
  void setup_block(1: i32 block_id, 2: string path, 3: i32 slot_begin, 4: i32 slot_end, 5: list<string> chain,
                   6: bool auto_scale, 7: i32 chain_role, 8: string next_block_name)
    throws (1: storage_management_exception ex),

  rpc_slot_range slot_range(1: i32 block_id)
    throws (1: storage_management_exception ex),

  void set_exporting(1: i32 block_id, 2: list<string> target_block, 3: i32 slot_begin, 4: i32 slot_end)
    throws (1: storage_management_exception ex),

  void set_importing(1: i32 block_id, 3: i32 slot_begin, 4: i32 slot_end)
    throws (1: storage_management_exception ex),

  void setup_and_set_importing(1: i32 block_id, 2: string path, 3: i32 slot_begin, 4: i32 slot_end,
                               5: list<string> chain, 6: i32 chain_role, 7: string next_block_name)
    throws (1: storage_management_exception ex),

  void set_regular(1: i32 block_id, 2: i32 slot_begin, 3: i32 slot_end)
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

  void export_slots(1: i32 block_id)
    throws (1: storage_management_exception ex),
}