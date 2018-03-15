#include <iostream>
#include <storage/kv/kv_rpc_server.h>
#include <storage/manager/storage_management_rpc_server.h>
#include <utils/signal_handling.h>
#include <utils/cmd_parse.h>

using namespace ::elasticmem::storage;
using namespace ::elasticmem::utils;

int main(int argc, char **argv) {
  signal_handling::install_signal_handler(argv[0], SIGSEGV, SIGKILL, SIGSTOP, SIGTRAP);

  cmd_options opts;
  opts.add(cmd_option("address", 'a', false).set_default("0.0.0.0").set_description("Address server binds to"));
  opts.add(cmd_option("service-port", 's', false).set_default("9093").set_description(
      "Port that storage service listens on"));
  opts.add(cmd_option("management-port", 'm', false).set_default("9094").set_description(
      "Port that storage management service listens on"));
  opts.add(cmd_option("block-port", 'b', false).set_default("9092").set_description(
      "Port that block advertisement interface connects to"));
  opts.add(cmd_option("num-blocks", 'n', false).set_default("64").set_description(
      "Number of blocks to advertise"));

  cmd_parser parser(argc, argv, opts);
  if (parser.get_flag("help")) {
    std::cout << parser.help_msg() << std::endl;
    return 0;
  }

  std::string address;
  int block_port;
  int service_port;
  int management_port;
  std::size_t num_blocks;

  try {
    address = parser.get("address");
    service_port = parser.get_int("service-port");
    management_port = parser.get_int("management-port");
    block_port = parser.get_int("block-port");
    num_blocks = static_cast<std::size_t>(parser.get_long("num-blocks"));
  } catch (cmd_parse_exception& ex) {
    std::cerr << "Could not parse command line args: " << ex.what() << std::endl;
    std::cerr << parser.help_msg() << std::endl;
    return -1;
  }

  std::vector<std::shared_ptr<block_management_ops>> block_management;
  block_management.resize(num_blocks);
  for (auto &block : block_management) {
    block = std::make_shared<kv_block>();
  }

  std::vector<std::shared_ptr<kv_block>> kv_blocks;
  for (auto &block : block_management) {
    kv_blocks.push_back(std::dynamic_pointer_cast<kv_block>(block));
  }

  auto management_server = storage_management_rpc_server::create(block_management, address, management_port);
  std::thread management_serve_thread([&management_server] {
    try {
      management_server->serve();
    } catch (std::exception &e) {
      std::cerr << "KV management server error: " << e.what() << std::endl;
    }
  });

  std::cout << "Management server listening on " << address << ":" << management_port << std::endl;

  auto kv_server = kv_rpc_server::create(kv_blocks, address, service_port);
  std::thread kv_serve_thread([&kv_server] {
    try {
      kv_server->serve();
    } catch (std::exception &e) {
      std::cerr << "KV server error: " << e.what() << std::endl;
    }
  });

  std::cout << "KV server listening on " << address << ":" << service_port << std::endl;

  if (management_serve_thread.joinable()) {
    management_serve_thread.join();
  }

  if (kv_serve_thread.joinable()) {
    kv_serve_thread.join();
  }

  return 0;
}