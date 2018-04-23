namespace cpp elasticmem.directory
namespace py elasticmem.directory
namespace java elasticmem.directory

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
  rpc_flushing = 2,
  rpc_on_disk = 3
}

struct rpc_replica_chain {
  1: required list<string> block_names,
  2: required i32 slot_begin,
  3: required i32 slot_end,
}

struct rpc_file_status {
  1: required rpc_file_type type,
  2: required rpc_perms permissions,
  3: required i64 last_write_time,
}

struct rpc_data_status {
  1: required rpc_storage_mode storage_mode,
  2: required string persistent_store_prefix,
  3: required i32 chain_length,
  4: required list<rpc_replica_chain> data_blocks,
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
  rpc_data_status create(1: string path, 2: string persistent_store_prefix, 3: i32 num_blocks, 4: i32 chain_length)
    throws (1: directory_service_exception ex),
  rpc_data_status open_or_create(1: string path, 2: string persistent_store_path, 3: i32 num_blocks, 4: i32 chain_length)
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

  void flush(1: string path)
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

  bool is_regular_file(1: string path)
    throws (1: directory_service_exception ex),

  bool is_directory(1: string path)
    throws (1: directory_service_exception ex),

  rpc_replica_chain reslove_failures(1: string path, 2: rpc_replica_chain chain) // TODO: We should pass in chain id...
    throws (1: directory_service_exception ex),

  rpc_replica_chain add_replica_to_chain(1: string path, 2: rpc_replica_chain chain) // TODO: We should pass in chain id...
    throws (1: directory_service_exception ex),

  void add_block_to_file(1: string path)
    throws (1: directory_service_exception ex),

  void split_slot_range(1: string path, 2: i32 slot_begin, 3: i32 slot_end)
      throws (1: directory_service_exception ex),

  void merge_slot_range(1: string path, 2: i32 slot_begin, 3: i32 slot_end)
        throws (1: directory_service_exception ex),
}