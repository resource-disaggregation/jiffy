#include "directory_lease_service_handler.h"

namespace elasticmem {
namespace directory {

directory_lease_service_handler::directory_lease_service_handler(std::shared_ptr<directory_service_shard> shard)
    : shard_(std::move(shard)) {}

void directory_lease_service_handler::create(const std::string &path) {

}

void directory_lease_service_handler::load(const std::string &persistent_path, const std::string &memory_path) {

}

void directory_lease_service_handler::renew_lease(rpc_keep_alive_ack &_return, const rpc_keep_alive &msg) {

}

void directory_lease_service_handler::remove(const std::string &path, rpc_remove_mode mode) {

}

}
}
