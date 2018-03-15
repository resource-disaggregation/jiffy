namespace cpp elasticmem.directory
namespace py elasticmem.directory

struct rpc_lease_update {
  1: required list<string> to_renew,
  2: required list<string> to_flush,
  3: required list<string> to_remove,
}

struct rpc_lease_ack {
  1: required i64 renewed,
  2: required i64 flushed,
  3: required i64 removed,
}

exception directory_lease_service_exception {
  1: string msg,
}

service directory_lease_service {
  rpc_lease_ack update_leases(1: rpc_lease_update updates)
    throws (1: directory_lease_service_exception ex),
}