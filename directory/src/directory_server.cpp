#include <iostream>
#include <vector>
#include <thread>
#include <directory/fs/directory_tree.h>
#include <directory/block/random_block_allocator.h>
#include <directory/fs/directory_rpc_server.h>
#include <directory/lease/lease_expiry_worker.h>
#include <directory/lease/directory_lease_server.h>
#include <storage/manager/storage_manager.h>
#include <utils/signal_handling.h>
#include <utils/cmd_parse.h>
#include <directory/block/block_allocation_server.h>
#include <utils/logger.h>

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

  std::exception_ptr alloc_exception = nullptr;
  auto alloc = std::make_shared<random_block_allocator>();
  auto block_allocation_server = block_allocation_server::create(alloc, address, block_port);
  std::thread block_allocation_serve_thread([&alloc_exception, &block_allocation_server] {
    try {
      block_allocation_server->serve();
    } catch (...) {
      alloc_exception = std::current_exception();
    }
  });
  LOG(log_level::info) << "Block allocation server listening on " << address << ":" << block_port;

  std::exception_ptr directory_exception = nullptr;
  auto storage = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, storage);
  auto directory_server = directory_rpc_server::create(tree, address, service_port);
  std::thread directory_serve_thread([&directory_exception, &directory_server] {
    try {
      directory_server->serve();
    } catch (...) {
      directory_exception = std::current_exception();
    }
  });

  LOG(log_level::info) << "Directory server listening on " << address << ":" << service_port;

  std::exception_ptr lease_exception = nullptr;
  auto lease_server = directory_lease_server::create(tree, address, lease_port);
  std::thread lease_serve_thread([&lease_exception, &lease_server] {
    try {
      lease_server->serve();
    } catch (...) {
      lease_exception = std::current_exception();
    }
  });

  LOG(log_level::info) << "Lease server listening on " << address << ":" << lease_port;

  lease_expiry_worker lmgr(tree, lease_period_ms, grace_period_ms);
  lmgr.start();

  if (block_allocation_serve_thread.joinable()) {
    block_allocation_serve_thread.join();
    if (alloc_exception) {
      try {
        std::rethrow_exception(alloc_exception);
      } catch (std::exception& e) {
        LOG(log_level::error) << "Block allocation server failed: " << e.what();
        std::exit(-1);
      }
    }
  }

  if (directory_serve_thread.joinable()) {
    directory_serve_thread.join();
    if (directory_exception) {
      try {
        std::rethrow_exception(directory_exception);
      } catch (std::exception& e) {
        LOG(log_level::error) << "Directory server failed: " << e.what();
        std::exit(-1);
      }
    }
  }

  if (lease_serve_thread.joinable()) {
    lease_serve_thread.join();
    if (lease_exception) {
      try {
        std::rethrow_exception(lease_exception);
      } catch (std::exception& e) {
        LOG(log_level::error) << "Lease server failed: " << e.what();
        std::exit(-1);
      }
    }
  }

  return 0;
}