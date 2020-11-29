#include "partition.h"
#include <random>

namespace jiffy {
namespace storage {

namespace karma_rand {
  int intRand(const int & min, const int & max) {
    static thread_local std::random_device r;
    static thread_local std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
    static thread_local std::mt19937 generator(seed);
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
}
}

partition::partition(block_memory_manager *manager,
                     const std::string &name,
                     const std::string &metadata,
                     const command_map &supported_commands)
    : name_(name),
      metadata_(metadata),
      supported_commands_(supported_commands),
      manager_(manager),
      binary_allocator_(build_allocator<uint8_t>()),
      block_seq_no_(0) {
  default_ = supported_commands_.empty();
}

void partition::path(const std::string &path) {
  path_ = path;
}

const std::string &partition::path() const {
  return path_;
}

void partition::name(const std::string &name) {
  name_ = name;
}

const std::string &partition::name() const {
  return name_;
}

void partition::metadata(const std::string &metadata) {
  metadata_ = metadata;
}

const std::string &partition::metadata() const {
  return metadata_;
}

bool partition::is_accessor(const std::string &cmd) const {
  // Does not require lock since block_ops don't change
  return supported_commands_.at(cmd).is_accessor();
}

bool partition::is_mutator(const std::string &cmd) const {
  // Does not require lock since block_ops don't change
  return supported_commands_.at(cmd).is_mutator();
}

uint32_t partition::command_id(const std::string &cmd_name) {
  auto it = supported_commands_.find(cmd_name);
  if (it == supported_commands_.end())
    return UINT32_MAX;
  return it->second.id;
}

std::size_t partition::storage_capacity() {
  return manager_->mb_capacity();
}

std::size_t partition::storage_size() {
  return manager_->mb_used();
}

subscription_map &partition::subscriptions() {
  return sub_map_;
}

block_response_client_map &partition::clients() {
  return client_map_;
}

void partition::set_name_and_metadata(const std::string &name, const std::string &metadata) {
  name_ = name;
  metadata_ = metadata;
}

void partition::notify(const arg_list &args) {
  subscriptions().notify(args.front(), args[1]);
}

binary partition::make_binary(const std::string &str) {
  return binary(str, binary_allocator_);
}

int32_t partition::seq_no() {
  return block_seq_no_;
}

void partition::update_seq_no(int32_t x) {
  block_seq_no_ = x;
}

int32_t partition::extract_block_seq_no(const arg_list &args, arg_list &out_list) {
  if(args.size() >= 3 && args[1] == "$block_seq_no$") {
    auto block_seq_no = std::stoi(args[2]);
    out_list.push_back(args[0]);
    out_list.insert(out_list.end(), args.begin()+3, args.end());
    return block_seq_no;
  }

  out_list.insert(out_list.end(), args.begin(), args.end());
  return 0;
}


void partition::run_command(response &_return, const arg_list &_args) {
  arg_list args;
  int32_t block_seq_no = extract_block_seq_no(_args, args);
  if(block_seq_no < block_seq_no_){
    _return.emplace_back("!stale_seq_no");
    return;
  }
  if(block_seq_no > block_seq_no_) {
      auto rand_prefix = std::to_string(karma_rand::intRand(0, 99999999)) + std::to_string(karma_rand::intRand(0, 99999999)) + std::to_string(karma_rand::intRand(0, 99999999)) + std::to_string(karma_rand::intRand(0, 99999999));
      sync("s3://synergy-karma/" + rand_prefix + path_ + "_" + std::to_string(block_seq_no_));
      LOG(log_level::info) << "Updating block seq no, block: " << metadata_ << " " << block_seq_no;
      block_seq_no_ = block_seq_no;
  }
  run_command_impl(_return, args);
}


}
}
