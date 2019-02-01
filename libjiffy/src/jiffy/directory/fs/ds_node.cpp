#include "ds_node.h"

namespace jiffy {
namespace directory {

ds_node::ds_node(std::string name, file_status status)
    : name_(std::move(name)), status_(status) {}

const std::string &ds_node::name() const { return name_; }

bool ds_node::is_directory() const { return status_.type() == file_type::directory; }

bool ds_node::is_regular_file() const { return status_.type() == file_type::regular; }

file_status ds_node::status() const { return status_; }

directory_entry ds_node::entry() const { return directory_entry(name_, status_); }

std::uint64_t ds_node::last_write_time() const { return status_.last_write_time(); }

void ds_node::permissions(const perms &prms) { status_.permissions(prms); }

perms ds_node::permissions() const { return status_.permissions(); }

void ds_node::last_write_time(std::uint64_t time) { status_.last_write_time(time); }

}
}
