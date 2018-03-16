#include <iostream>
#include <vector>
#include <thread>
#include <directory/fs/directory_tree.h>
#include <directory/block/random_block_allocator.h>
#include <directory/fs/directory_rpc_server.h>
#include <directory/lease/lease_manager.h>
#include <directory/lease/directory_lease_server.h>
#include <storage/manager/storage_manager.h>
#include <utils/signal_handling.h>
#include <utils/cmd_parse.h>
#include <directory/block/block_allocation_server.h>

using namespace ::elasticmem::directory;
using namespace ::elasticmem::storage;
using namespace ::elasticmem::utils;

int main(int argc, char **argv) {
  signal_handling::install_signal_handler(argv[0], SIGSEGV, SIGKILL, SIGSTOP, SIGTRAP);

  cmd_options opts;
  opts.add(cmd_option("address", 'a', false).set_default("0.0.0.0").set_description("Address server binds to"));
  opts.add(cmd_option("service-port", 's', false).set_default("9090").set_description(
      "Port that directory service listens on"));
  opts.add(cmd_option("lease-port", 'l', false).set_default("9091").set_description(
      "Port that lease service listens on"));
  opts.add(cmd_option("block-port", 'b', false).set_default("9092").set_description(
      "Port that block advertisement service listens on"));
  opts.add(cmd_option("lease-period-ms", 'p', false).set_default("10000").set_description(
      "Lease duration (in ms) that lease service advertises"));
  opts.add(cmd_option("grace-period-ms", 'g', false).set_default("10000").set_description(
      "Grace period (in ms) that lease service waits for beyond lease duration"));

  cmd_parser parser(argc, argv, opts);
  if (parser.get_flag("help")) {
    std::cout << parser.help_msg() << std::endl;
    return 0;
  }

  std::string address;
  int service_port;
  int lease_port;
  int block_port;
  uint64_t lease_period_ms;
  uint64_t grace_period_ms;

  try {
    address = parser.get("address");
    service_port = parser.get_int("service-port");
    lease_port = parser.get_int("lease-port");
    block_port = parser.get_int("block-port");
    lease_period_ms = static_cast<uint64_t>(parser.get_long("lease-period-ms"));
    grace_period_ms = static_cast<uint64_t>(parser.get_long("grace-period-ms"));
  } catch (cmd_parse_exception &ex) {
    std::cerr << "Could not parse command line args: " << ex.what() << std::endl;
    std::cerr << parser.help_msg() << std::endl;
    return -1;
  }

  std::vector<std::string> blocks;

  auto alloc = std::make_shared<random_block_allocator>(blocks);
  auto block_allocation_server = block_allocation_server::create(alloc, address, block_port);
  std::thread block_allocation_serve_thread([&block_allocation_server] {
    try {
      block_allocation_server->serve();
    } catch (std::exception &e) {
      std::cerr << "Block allocation server error: " << e.what() << std::endl;
    }
  });
  std::cout << "Block allocation server listening on " << address << ":" << block_port << std::endl;

  auto tree = std::make_shared<directory_tree>(alloc);
  auto directory_server = directory_rpc_server::create(tree, address, service_port);
  std::thread directory_serve_thread([&directory_server] {
    try {
      directory_server->serve();
    } catch (std::exception &e) {
      std::cerr << "Directory server error: " << e.what() << std::endl;
    }
  });

  std::cout << "Directory server listening on " << address << ":" << service_port << std::endl;

  auto storage = std::make_shared<storage_manager>();
  auto lease_server = directory_lease_server::create(tree, storage, address, lease_port);
  std::thread lease_serve_thread([&lease_server] {
    try {
      lease_server->serve();
    } catch (std::exception &e) {
      std::cerr << "Lease server error: " << e.what() << std::endl;
    }
  });

  std::cout << "Lease server listening on " << address << ":" << lease_port << std::endl;

  lease_manager lmgr(tree, lease_period_ms, grace_period_ms);
  lmgr.start();

  if (directory_serve_thread.joinable()) {
    directory_serve_thread.join();
  }

  if (lease_serve_thread.joinable()) {
    lease_serve_thread.join();
  }

  return 0;
}