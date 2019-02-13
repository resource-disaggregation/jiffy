namespace cpp jiffy.directory
namespace py jiffy.directory
namespace java jiffy.directory

typedef i32 rpc_perms

enum rpc_perm_options {
  rpc_replace = 0,
  rpc_add = 1,
  rpc_remove = 2,
}

enum rpc_file_type {
  rpc_none = 0,
  rpc_regular = 1,
  rpc_directory = 2
}

enum rpc_storage_mode {
  rpc_in_memory = 0,
  rpc_in_memory_grace = 1,
  rpc_on_disk = 2
}

struct rpc_replica_chain {
  1: required list<string> block_ids,
  2: required string name,
  3: required string metadata,
  4: required rpc_storage_mode storage_mode
}

struct rpc_file_status {
  1: required rpc_file_type type,
  2: required rpc_perms permissions,
  3: required i64 last_write_time,
}

struct rpc_data_status {
  1: required string type,
  2: required string backing_path,
  3: required i32 chain_length,
  4: required list<rpc_replica_chain> data_blocks,
  5: required i32 flags,
  6: required map<string, string> tags,
}

struct rpc_dir_entry {
  1: required string name,
  2: required rpc_file_status status,
}

exception directory_service_exception {
  1: required string msg
}

service directory_service {
  void create_directory(1: string path)
    throws (1: directory_service_exception ex),
  void create_directories(1: string path)
    throws (1: directory_service_exception ex),

  rpc_data_status open(1: string path)
    throws (1: directory_service_exception ex),
  rpc_data_status create(1: string path, 2: string type, 3: string backing_path, 4: i32 num_blocks, 5: i32 chain_length,
                         6: i32 flags, 7: i32 permissions, 8: list<string> block_ids, 9: list<string> block_metadata,
                         10: map<string, string> tags)
    throws (1: directory_service_exception ex),
  rpc_data_status open_or_create(1: string path, 2: string type, 3: string backing_path, 4: i32 num_blocks,
                                 5: i32 chain_length, 6: i32 flags, 7: i32 permissions, 8: list<string> block_ids,
                                 9: list<string> block_metadata, 10: map<string, string> tags)
    throws (1: directory_service_exception ex),

  bool exists(1: string path)
    throws (1: directory_service_exception ex),

  i64 last_write_time(1: string path)
      throws (1: directory_service_exception ex),

  void set_permissions(1: string path, 2: rpc_perms perms, 3: rpc_perm_options opts)
    throws (1: directory_service_exception ex),
  rpc_perms get_permissions(1: string path)
    throws (1: directory_service_exception ex),

  void remove(1: string path)
    throws (1: directory_service_exception ex),
  void remove_all(1: string path)
    throws (1: directory_service_exception ex),

  void sync(1: string path, 2: string backing_path)
    throws (1: directory_service_exception ex),
  void dump(1: string path, 2: string backing_path)
      throws (1: directory_service_exception ex),
  void load(1: string path, 2: string backing_path)
      throws (1: directory_service_exception ex),

  void rename(1: string old_path, 2: string new_path)
    throws (1: directory_service_exception ex),

  rpc_file_status status(1: string path)
    throws (1: directory_service_exception ex),

  list<rpc_dir_entry> directory_entries(1: string path)
    throws (1: directory_service_exception ex),

  list<rpc_dir_entry> recursive_directory_entries(1: string path)
    throws (1: directory_service_exception ex),
  
  rpc_data_status dstatus(1: string path)
    throws (1: directory_service_exception ex),

  void add_tags(1: string path, 2: map<string, string> tags)
    throws (1: directory_service_exception ex),

  bool is_regular_file(1: string path)
    throws (1: directory_service_exception ex),

  bool is_directory(1: string path)
    throws (1: directory_service_exception ex),

  rpc_replica_chain reslove_failures(1: string path, 2: rpc_replica_chain chain) // TODO: We should pass in chain id...
    throws (1: directory_service_exception ex),

  rpc_replica_chain add_replica_to_chain(1: string path, 2: rpc_replica_chain chain) // TODO: We should pass in chain id...
    throws (1: directory_service_exception ex),

  rpc_replica_chain add_data_block(1: string path, 2: string partition_name, 3: string partition_metadata)
    throws (1: directory_service_exception ex),

  void remove_data_block(1: string path, 2: string partition_name)
    throws (1: directory_service_exception ex),

  void request_partition_data_update(1: string path, 2: string old_partition_name, 3: string new_partition_name, 4: string partition_metadata)
    throws (1: directory_service_exception ex),

  i64 get_storage_capacity(1: string path, 2: string partition_name)
    throws (1: directory_service_exception ex),
}