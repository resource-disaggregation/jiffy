#include <atomic>
#include <iostream>
#include <jiffy/directory/block/block_registration_client.h>
#include <jiffy/storage/hashtable/hash_table_partition.h>
#include <jiffy/storage/manager/storage_management_server.h>
#include <jiffy/auto_scaling/auto_scaling_server.h>
#include <jiffy/storage/service/block_server.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/logger.h>
#include <boost/program_options.hpp>
#include <ifaddrs.h>
#include "server_storage_tracker.h"

using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::auto_scaling;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;
using namespace ::apache::thrift::server;
std::string mapper(const std::string &env_var) {
  if (env_var == "JIFFY_DIRECTORY_HOST") return "directory.host";
  else if (env_var == "JIFFY_DIRECTORY_SERVICE_PORT") return "directory.service_port";
  else if (env_var == "JIFFY_BLOCK_PORT") return "directory.block_port";
  else if (env_var == "JIFFY_STORAGE_HOST") return "storage.host";
  else if (env_var == "JIFFY_STORAGE_SERVICE_PORT") return "storage.service_port";
  else if (env_var == "JIFFY_STORAGE_MGMT_PORT") return "storage.management_port";
  else if (env_var == "JIFFY_STORAGE_SCALING_PORT") return "storage.auto_scaling_port";
  else if (env_var == "JIFFY_STORAGE_NUM_BLOCKS") return "storage.block.num_blocks";
  else if (env_var == "JIFFY_BLOCK_CAPACITY") return "storage.block.capacity";
  else if (env_var == "JIFFY_STORAGE_NUM_BLOCK_GROUPS") return "storage.block.num_block_groups";
  return "";
}

std::string local_address() {
  std::string ip_address = "";
  struct ifaddrs *interfaces = NULL;
  struct ifaddrs *temp_addr = NULL;
  int success = 0;
  // retrieve the current interfaces - returns 0 on success
  success = getifaddrs(&interfaces);
  if (success == 0) {
    // Loop through linked list of interfaces
    temp_addr = interfaces;
    while (temp_addr != NULL) {
      if (temp_addr->ifa_addr->sa_family == AF_INET) {
        ip_address = inet_ntoa(((struct sockaddr_in *) temp_addr->ifa_addr)->sin_addr);
      }
      temp_addr = temp_addr->ifa_next;
    }
  }
  // Free memory
  freeifaddrs(interfaces);
  if (ip_address.empty()) {
    throw std::runtime_error("Unable to get IP Address");
  }
  return ip_address;
}

std::string dir_host = "127.0.0.1";
int32_t block_port = 9092;
std::vector<std::string> block_ids;

int main(int argc, char **argv) {
  signal_handling::install_error_handler(SIGABRT, SIGFPE, SIGSEGV, SIGILL, SIGTRAP);
  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);

  // Parse configuration parameters
  // Configuration priority order: default < env < configuration file < commandline args
  // First set defaults
  std::string address = "127.0.0.1";
  int32_t mgmt_port = 9093;
  int32_t auto_scaling_port = 9094;
  int32_t service_port = 9095;
  int32_t dir_port = 9090;
  std::size_t num_blocks = 64;
  std::size_t num_block_groups = std::thread::hardware_concurrency() / 2;
  std::size_t block_capacity = 134217728;
  double blk_thresh_lo = 0.25;
  double blk_thresh_hi = 0.75;
  std::string storage_trace = "";
  try {
    namespace po = boost::program_options;
    std::string config_file = "";
    po::options_description generic(" options");
    generic.add_options()
        ("version,v", "Print version string")
        ("help,h", "Print help message")
        ("config,c", po::value<std::string>(&config_file), "Configuration file");

    po::options_description hidden("Hidden options");
    hidden.add_options()
        ("storage-trace,T", po::value<std::string>(&storage_trace), "Storage trace file");

    // Configuration file variables are named differently
    po::options_description config_file_options;
    config_file_options.add_options()
        ("storage.host", po::value<std::string>(&address)->default_value("127.0.0.1"))
        ("storage.management_port", po::value<int>(&mgmt_port)->default_value(9093))
        ("storage.auto_scaling_port", po::value<int>(&auto_scaling_port)->default_value(9094))
        ("storage.service_port", po::value<int>(&service_port)->default_value(9095))
        ("directory.host", po::value<std::string>(&dir_host)->default_value("127.0.0.1"))
        ("directory.service_port", po::value<int>(&dir_port)->default_value(9090))
        ("directory.block_port", po::value<int>(&block_port)->default_value(9092))
        ("storage.block.num_blocks", po::value<size_t>(&num_blocks)->default_value(64))
        ("storage.block.num_block_groups", po::value<size_t>(&num_block_groups)->default_value(std::thread::hardware_concurrency() / 2))
        ("storage.block.capacity", po::value<size_t>(&block_capacity)->default_value(134217728))
        ("storage.block.capacity_threshold_lo", po::value<double>(&blk_thresh_lo)->default_value(0.25))
        ("storage.block.capacity_threshold_hi", po::value<double>(&blk_thresh_hi)->default_value(0.75));

    po::options_description cmdline_options, env_options;
    cmdline_options.add(generic).add(hidden);
    env_options.add(config_file_options);

    po::options_description visible;
    visible.add(generic);

    po::variables_map vm;

    // Commandline args have highest priority
    store(po::command_line_parser(argc, argv).options(cmdline_options).run(), vm);
    notify(vm);

    if (vm.count("help")) {
      std::cout << "Storage service daemon" << std::endl;
      std::cout << visible << std::endl;
      return 0;
    }

    if (vm.count("version")) {
      std::cout << "Jiffy storage service daemon, Version 0.1.0" << std::endl; // TODO: Configure version string
      return 0;
    }

    // Configuration files have higher priority than env vars
    // std::vector<std::string> config_files;
    // if (config_file == "") {
    //   config_files = {"conf/jiffy.conf", "/etc/jiffy/jiffy.conf", "/usr/conf/jiffy.conf", "/usr/local/conf/jiffy.conf"};
    // } else {
    //   config_files = {config_file};
    // }

    // for (const auto &cfile: config_files) {
    //   std::ifstream ifs(cfile.c_str());
    //   if (ifs) {
    //     LOG(log_level::info) << "config: " << cfile;
    //     store(parse_config_file(ifs, config_file_options, true), vm);
    //     notify(vm);
    //     break;
    //   }
    // }

    // Env vars have lowest priority
    store(po::parse_environment(env_options, boost::function1<std::string, std::string>(mapper)), vm);
    notify(vm);

    // Output all the configuration parameters:
    LOG(log_level::info) << "storage.host: " << address;
    LOG(log_level::info) << "storage.service_port: " << service_port;
    LOG(log_level::info) << "storage.management_port: " << mgmt_port;
    LOG(log_level::info) << "storage.auto_scaling_port: " << auto_scaling_port;
    LOG(log_level::info) << "storage.block.num_blocks: " << num_blocks;
    LOG(log_level::info) << "storage.block.num_block_groups: " << num_block_groups;
    LOG(log_level::info) << "storage.block.capacity: " << block_capacity;
    LOG(log_level::info) << "storage.block.capacity_threshold_lo: " << blk_thresh_lo;
    LOG(log_level::info) << "storage.block.capacity_threshold_hi: " << blk_thresh_hi;
    LOG(log_level::info) << "directory.host: " << dir_host;
    LOG(log_level::info) << "directory.service_port: " << dir_port;
    LOG(log_level::info) << "directory.block_port: " << block_port;
  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  std::mutex failure_mtx;
  std::condition_variable failure_condition;
  std::atomic<int>
      failing_thread(-1); // management -> 0, service -> 1, notification -> 2, chain -> 3, auto_scaling -> 4

  std::string hostname;
  if (address == "0.0.0.0") {
    hostname = local_address();
  } else {
    hostname = address;
  }

  LOG(log_level::info) << "Hostname: " << hostname;

  for (int i = 0; i < static_cast<int>(num_blocks); i++) {
    block_ids.push_back(block_id_parser::make(hostname, service_port + i % num_block_groups, mgmt_port, i));
  }

  std::vector<std::shared_ptr<block>> blocks;
  blocks.resize(num_blocks);
  for (size_t i = 0; i < blocks.size(); ++i) {
    blocks[i] = std::make_shared<block>(block_ids[i], block_capacity, address, auto_scaling_port);
  }
  LOG(log_level::info) << "Created " << blocks.size() << " blocks";

  std::exception_ptr auto_scaling_exception = nullptr;
  auto scaling_server = auto_scaling_server::create(dir_host, dir_port, address, auto_scaling_port);
  std::thread scaling_serve_thread([&auto_scaling_exception, &scaling_server, &failing_thread, & failure_condition] {
    try {
      scaling_server->serve();
    } catch (...) {
      auto_scaling_exception = std::current_exception();
      failing_thread = 4;
      failure_condition.notify_all();
    }
  });

  std::exception_ptr management_exception = nullptr;
  auto management_server = storage_management_server::create(blocks, address, mgmt_port);
  std::thread management_serve_thread([&management_exception, &management_server, &failing_thread, &failure_condition] {
    try {
      management_server->serve();
    } catch (...) {
      management_exception = std::current_exception();
      failing_thread = 0;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "Management server listening on " << address << ":" << mgmt_port;

  try {
    block_registration_client client(dir_host, block_port);
    client.register_blocks(block_ids);
    client.disconnect();
  } catch (std::exception &e) {
    LOG(log_level::error) << "Failed to advertise blocks: " << e.what()
                          << "; make sure block allocation server is running";
    std::exit(-1);
  }

  LOG(log_level::info) << "Advertised " << num_blocks << " to block allocation server";

  std::exception_ptr storage_exception;
  std::vector<std::thread> storage_serve_thread(num_block_groups);
  std::vector<std::shared_ptr<TServer>> storage_server(num_block_groups);
  std::vector<std::vector<std::shared_ptr<block>>> block_vec(num_block_groups);
  for (size_t i = 0; i < num_block_groups; i++) {
    auto block_group = std::vector<std::shared_ptr<block>>();
    for (size_t j = i; j < num_blocks; j += num_block_groups)
      block_group.push_back(blocks[j]);
    storage_server[i] = block_server::create(block_group, service_port + i);
    storage_serve_thread[i] =
        std::thread([&storage_exception, &storage_server, &failing_thread, &failure_condition, i] {
          try {
            storage_server[i]->serve();
          } catch (...) {
            storage_exception = std::current_exception();
            failing_thread = 1;
            failure_condition.notify_all();
          }
        });
  }

  LOG(log_level::info) << "Storage server listening on " << address << ":" << service_port;

  server_storage_tracker tracker(blocks, 1000, storage_trace);
  if (!storage_trace.empty()) {
    tracker.start();
  }

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
      if (storage_exception) {
        try {
          std::rethrow_exception(storage_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "ERROR: " << e.what();
          std::exit(-1);
        }
      }
      break;
    }
    case 4: {
      LOG(log_level::error) << "Auto_scaling server failed";
      if (auto_scaling_exception) {
        try {
          std::rethrow_exception(auto_scaling_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "ERROR: " << e.what();
          std::exit(-1);
        }
      }
    }
    default:break;
  }

  try {
    block_registration_client client(dir_host, block_port);
    client.deregister_blocks(block_ids);
    client.disconnect();
  } catch (std::exception &e) {
    LOG(log_level::error) << "Failed to retract blocks: " << e.what()
                          << "; make sure block allocation server is running\n";
    std::exit(-1);
  }

  return -1;
}
