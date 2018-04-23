#include <atomic>
#include <iostream>
#include <directory/block/block_advertisement_client.h>
#include <storage/kv/kv_block.h>
#include <storage/manager/storage_management_server.h>
#include <storage/notification/notification_server.h>
#include <storage/service/block_server.h>
#include <utils/signal_handling.h>
#include <utils/cmd_parse.h>
#include <utils/logger.h>
#include <storage/service/chain_server.h>

using namespace ::elasticmem::directory;
using namespace ::elasticmem::storage;
using namespace ::elasticmem::utils;

using namespace ::apache::thrift;

std::string dir_host;
int32_t block_port;
std::vector<std::string> block_names;

void retract_block_names_and_print_stacktrace(int sig_num) {
  std::string trace = signal_handling::stacktrace();
  LOG(log_level::info) << "Caught signal " << sig_num << ", cleaning up...";
  try {
    block_advertisement_client client(dir_host, block_port);
    client.retract_blocks(block_names);
    client.disconnect();
  } catch (std::exception &e) {
    LOG(log_level::error) << "Failed to retract blocks: " << e.what()
                          << "; make sure block allocation server is running\n";
  }

  fprintf(stderr, "%s\n", trace.c_str());
  fprintf(stderr, "Exiting...\n");
  std::exit(0);
}

int main(int argc, char **argv) {
  signal_handling::install_signal_handler(retract_block_names_and_print_stacktrace,
                                          SIGSTOP,
                                          SIGINT,
                                          SIGTERM,
                                          SIGABRT,
                                          SIGFPE,
                                          SIGSEGV,
                                          SIGILL,
                                          SIGTRAP);
  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);

  cmd_options opts;
  opts.add(cmd_option("address", 'a', false).set_default("0.0.0.0").set_description("Address server binds to"));
  opts.add(cmd_option("service-port", 's', false).set_default("9093").set_description(
      "Port that storage service listens on"));
  opts.add(cmd_option("management-port", 'm', false).set_default("9094").set_description(
      "Port that storage management service listens on"));
  opts.add(cmd_option("notification-port", 'n', false).set_default("9095").set_description(
      "Port that notification service listens on"));
  opts.add(cmd_option("dir-address", 'd', false).set_default("127.0.0.1").set_description(
      "Host for directory service"));
  opts.add(cmd_option("dir-port", 'p', false).set_default("9090").set_description(
      "Port that directory service interface connects to"));
  opts.add(cmd_option("block-port", 't', false).set_default("9092").set_description(
      "Port that block advertisement interface connects to"));
  opts.add(cmd_option("chain-port", 'c', false).set_default("9096").set_description(
      "Port that block server listens on for chain requests"));
  opts.add(cmd_option("num-blocks", 'n', false).set_default("64").set_description(
      "Number of blocks to advertise"));
  opts.add(cmd_option("block-capacity", 'b', false).set_default("134217728").set_description(
      "Storage capacity of each block"));
  opts.add(cmd_option("capacity-threshold-lo", 'l', false).set_default("0.25").set_description(
      "Low threshold fraction for block capacity"));
  opts.add(cmd_option("capacity-threshold-hi", 'h', false).set_default("0.75").set_description(
      "High threshold fraction for block capacity"));

  cmd_parser parser(argc, argv, opts);
  if (parser.get_flag("help")) {
    std::cout << parser.help_msg() << std::endl;
    return 0;
  }

  std::string address;
  int32_t service_port;
  int32_t management_port;
  int32_t notification_port;
  int32_t chain_port;
  int32_t dir_port;
  std::size_t num_blocks;
  std::size_t block_capacity;
  double capacity_threshold_lo;
  double capacity_threshold_hi;
  try {
    address = parser.get("address");
    dir_host = parser.get("dir-address");
    dir_port = parser.get_int("dir-port");
    service_port = parser.get_int("service-port");
    management_port = parser.get_int("management-port");
    block_port = parser.get_int("block-port");
    notification_port = parser.get_int("notification-port");
    chain_port = parser.get_int("chain-port");
    num_blocks = static_cast<std::size_t>(parser.get_long("num-blocks"));
    block_capacity = static_cast<std::size_t>(parser.get_long("block-capacity"));
    capacity_threshold_lo = parser.get_double("capacity-threshold-lo");
    capacity_threshold_hi = parser.get_double("capacity-threshold-hi");
  } catch (cmd_parse_exception &ex) {
    std::cerr << "Could not parse command line args: " << ex.what() << std::endl;
    std::cerr << parser.help_msg() << std::endl;
    return -1;
  }

  std::mutex failure_mtx;
  std::condition_variable failure_condition;
  std::atomic<int> failing_thread(-1); // management -> 0, service -> 1, notification -> 2, chain -> 3

  std::string hostname;
  if (address == "0.0.0.0") {
    char hbuf[1024];
    gethostname(hbuf, sizeof(hbuf));
    hostname = hbuf;
  } else {
    hostname = address;
  }

  LOG(log_level::info) << "Hostname: " << hostname;

  for (int i = 0; i < static_cast<int>(num_blocks); i++) {
    block_names.push_back(block_name_parser::make(hostname, service_port, management_port, notification_port, 0, i));
  }

  std::vector<std::shared_ptr<chain_module>> blocks;
  blocks.resize(num_blocks);
  for (size_t i = 0; i < blocks.size(); ++i) {
    blocks[i] = std::make_shared<kv_block>(block_names[i],
                                           block_capacity,
                                           capacity_threshold_lo,
                                           capacity_threshold_hi,
                                           dir_host,
                                           dir_port);
  }
  LOG(log_level::info) << "Created " << blocks.size() << " blocks";

  std::exception_ptr management_exception = nullptr;
  auto management_server = storage_management_server::create(blocks, address, management_port);
  std::thread management_serve_thread([&management_exception, &management_server, &failing_thread, &failure_condition] {
    try {
      management_server->serve();
    } catch (...) {
      management_exception = std::current_exception();
      failing_thread = 0;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "Management server listening on " << address << ":" << management_port;

  try {
    block_advertisement_client client(dir_host, block_port);
    client.advertise_blocks(block_names);
    client.disconnect();
  } catch (std::exception &e) {
    LOG(log_level::error) << "Failed to advertise blocks: " << e.what()
                          << "; make sure block allocation server is running";
    std::exit(-1);
  }

  LOG(log_level::info) << "Advertised " << num_blocks << " to block allocation server";

  std::exception_ptr kv_exception = nullptr;
  auto kv_server = block_server::create(blocks, address, service_port);
  std::thread kv_serve_thread([&kv_exception, &kv_server, &failing_thread, &failure_condition] {
    try {
      kv_server->serve();
    } catch (...) {
      kv_exception = std::current_exception();
      failing_thread = 1;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "KV server listening on " << address << ":" << service_port;

  std::exception_ptr notification_exception = nullptr;
  auto notification_server = notification_server::create(blocks, address, notification_port);
  std::thread
      notification_serve_thread([&notification_exception, &notification_server, &failing_thread, &failure_condition] {
    try {
      notification_server->serve();
    } catch (...) {
      notification_exception = std::current_exception();
      failing_thread = 2;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "Notification server listening on " << address << ":" << notification_port;

  std::exception_ptr chain_exception = nullptr;
  auto chain_server = chain_server::create(blocks, address, chain_port);
  std::thread chain_serve_thread([&chain_exception, &chain_server, &failing_thread, &failure_condition] {
    try {
      chain_server->serve();
    } catch (...) {
      chain_exception = std::current_exception();
      failing_thread = 2;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "Chain server listening on " << address << ":" << chain_port;

  std::unique_lock<std::mutex> failure_condition_lock{failure_mtx};
  failure_condition.wait(failure_condition_lock, [&failing_thread] {
    return failing_thread != -1;
  });

  switch (failing_thread.load()) {
    case 0: {
      LOG(log_level::error) << "Storage management server failed";
      if (management_exception) {
        try {
          std::rethrow_exception(management_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "ERROR: " << e.what();
        }
      }
      break;
    }
    case 1: {
      LOG(log_level::error) << "KV server failed";
      if (kv_exception) {
        try {
          std::rethrow_exception(kv_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "ERROR: " << e.what();
          std::exit(-1);
        }
      }
      break;
    }
    case 2: {
      LOG(log_level::error) << "Notification server failed";
      if (notification_exception) {
        try {
          std::rethrow_exception(notification_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "ERROR: " << e.what();
          std::exit(-1);
        }
      }
      break;
    }
    case 3: {
      LOG(log_level::error) << "Chain server failed";
      if (chain_exception) {
        try {
          std::rethrow_exception(chain_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "ERROR: " << e.what();
          std::exit(-1);
        }
      }
      break;
    }
    default:break;
  }

  try {
    block_advertisement_client client(dir_host, block_port);
    client.retract_blocks(block_names);
    client.disconnect();
  } catch (std::exception &e) {
    LOG(log_level::error) << "Failed to retract blocks: " << e.what()
                          << "; make sure block allocation server is running\n";
    std::exit(-1);
  }

  return -1;
}