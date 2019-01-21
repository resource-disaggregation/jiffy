namespace cpp jiffy.directory
namespace py jiffy.lease
namespace java jiffy.lease

struct rpc_lease_ack {
  1: required i64 renewed,
  2: required i64 lease_period_ms,
}

exception lease_service_exception {
  1: string msg,
}

service lease_service {
  rpc_lease_ack renew_leases(1: list<string> to_renew)
    throws (1: lease_service_exception ex),
}