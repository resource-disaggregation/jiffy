#include <iostream>
#include <vector>
#include <thread>
#include <jiffy/directory/fs/directory_tree.h>
#include <jiffy/directory/block/random_block_allocator.h>
#include <jiffy/directory/block/maxmin_block_allocator.h>
#include <jiffy/directory/block/static_block_allocator.h>
#include <jiffy/directory/block/karma_block_allocator.h>
#include <jiffy/directory/fs/directory_server.h>
#include <jiffy/directory/lease/lease_expiry_worker.h>
#include <jiffy/directory/lease/lease_server.h>
#include <jiffy/storage/manager/storage_manager.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/directory/block/block_registration_server.h>
#include <jiffy/utils/logger.h>
#include <jiffy/directory/block/file_size_tracker.h>
#include <boost/program_options.hpp>
#include <jiffy/directory/fs/sync_worker.h>

using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;

std::string mapper(const std::string &env_var) {
  if (env_var == "JIFFY_DIRECTORY_HOST") return "directory.host";
  else if (env_var == "JIFFY_DIRECTORY_SERVICE_PORT") return "directory.service_port";
  else if (env_var == "JIFFY_LEASE_PORT") return "directory.lease_port";
  else if (env_var == "JIFFY_BLOCK_PORT") return "directory.block_port";
  else if (env_var == "JIFFY_LEASE_PERIOD_MS") return "directory.lease.lease_period_ms";
  else if (env_var == "JIFFY_GRACE_PERIOD_MS") return "directory.lease.grace_period_ms";
  return "";
}

int main(int argc, char **argv) {
  signal_handling::install_error_handler(SIGABRT, SIGFPE, SIGSEGV, SIGILL, SIGTRAP);

  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);

  // Parse configuration parameters
  // Configuration priority order: default < env < configuration file < commandline args
  // First set defaults
  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int block_port = 9092;
  uint64_t lease_period_ms = 10000;
  uint64_t grace_period_ms = 10000;
  std::string storage_trace = "";
  std::string allocator_type = "maxmin";
  uint32_t num_tenants = 1;
  uint64_t init_credits = 0;
  uint32_t algo_interval_ms = 1000;
  uint32_t public_blocks = 0;

  try {
    namespace po = boost::program_options;
    std::string config_file = "";
    po::options_description generic("options");
    generic.add_options()
        ("version,v", "Print version string")
        ("help,h", "Print help message")
        ("config,c", po::value<std::string>(&config_file), "Configuration file")
        ("alloc,a", po::value<std::string>(&allocator_type), "Allocator type")
        ("num_tenants,n", po::value<uint32_t>(&num_tenants), "No of tenants")
        ("init_credits,i", po::value<uint64_t>(&init_credits), "Initial credits")
        ("algo_interval,z", po::value<uint32_t>(&algo_interval_ms), "Algorithm interval in ms")
        ("public_blocks,p", po::value<uint32_t>(&public_blocks), "Public blocks");

    po::options_description hidden("Hidden options");
    hidden.add_options()
        ("storage-trace,T", po::value<std::string>(&storage_trace), "Storage trace file");

    // Configuration file variables are named differently
    po::options_description config_file_options;
    config_file_options.add_options()
        ("directory.host", po::value<std::string>(&address)->default_value("127.0.0.1"))
        ("directory.service_port", po::value<int>(&service_port)->default_value(9090))
        ("directory.lease_port", po::value<int>(&lease_port)->default_value(9091))
        ("directory.block_port", po::value<int>(&lease_port)->default_value(9092))
        ("directory.lease.lease_period_ms", po::value<uint64_t>(&lease_period_ms)->default_value(10000))
        ("directory.lease.grace_period_ms", po::value<uint64_t>(&grace_period_ms)->default_value(10000));

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
      std::cout << "Directory service daemon" << std::endl;
      std::cout << visible << std::endl;
      return 0;
    }

    if (vm.count("version")) {
      std::cout << "Directory service daemon, Version 0.1.0" << std::endl; // TODO: Configure version string
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
    LOG(log_level::info) << "directory.host: " << address;
    LOG(log_level::info) << "directory.service_port: " << service_port;
    LOG(log_level::info) << "directory.lease_port: " << lease_port;
    LOG(log_level::info) << "directory.block_port: " << block_port;
    LOG(log_level::info) << "directory.lease.lease_period_ms: " << lease_period_ms;
    LOG(log_level::info) << "directory.lease.grace_period_ms: " << grace_period_ms;

    LOG(log_level::info) << "public_blocks: " << public_blocks;
    LOG(log_level::info) << "init credits: " << init_credits;
    LOG(log_level::info) << "num tenants: " << num_tenants;
    LOG(log_level::info) << "algo interval: " << algo_interval_ms;
    LOG(log_level::info) << "allocator: " << allocator_type;

  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  std::mutex failure_mtx;
  std::condition_variable failure_condition;
  std::atomic<int> failing_thread(-1); // alloc -> 0, directory -> 1, lease -> 2

  std::exception_ptr alloc_exception = nullptr;
  std::shared_ptr<block_allocator> alloc;
  if(allocator_type == "maxmin"){
    alloc = std::make_shared<maxmin_block_allocator>(num_tenants, algo_interval_ms);
  } 
  else if(allocator_type == "static") {
    alloc = std::make_shared<static_block_allocator>(num_tenants, algo_interval_ms);
  }
  else if(allocator_type == "random") {
    alloc = std::make_shared<random_block_allocator>();
  }
  else if(allocator_type == "karma") {
    alloc = std::make_shared<karma_block_allocator>(num_tenants, init_credits, algo_interval_ms, public_blocks);
  }
  else 
  {
    std::cerr << "unkown allocator type" << std::endl;
    return 1;
  }
  auto alloc_server = block_registration_server::create(alloc, address, block_port);
  std::thread alloc_serve_thread([&alloc_exception, &alloc_server, &failing_thread, &failure_condition] {
    try {
      alloc_server->serve();
    } catch (...) {
      alloc_exception = std::current_exception();
      failing_thread = 0;
      failure_condition.notify_all();
    }
  });
  LOG(log_level::info) << "Block allocation server listening on " << address << ":" << block_port;

  std::exception_ptr directory_exception = nullptr;
  auto storage = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(alloc, storage);
  auto directory_server = directory_server::create(tree, address, service_port);
  std::thread directory_serve_thread([&directory_exception, &directory_server, &failing_thread, &failure_condition] {
    try {
      directory_server->serve();
    } catch (...) {
      directory_exception = std::current_exception();
      failing_thread = 1;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "Directory server listening on " << address << ":" << service_port;

  std::exception_ptr lease_exception = nullptr;
  auto lease_server = lease_server::create(tree, lease_period_ms, address, lease_port);
  std::thread lease_serve_thread([&lease_exception, &lease_server, &failing_thread, &failure_condition] {
    try {
      lease_server->serve();
    } catch (...) {
      lease_exception = std::current_exception();
      failing_thread = 2;
      failure_condition.notify_all();
    }
  });

  LOG(log_level::info) << "Lease server listening on " << address << ":" << lease_port;

  lease_expiry_worker lmgr(tree, lease_period_ms, grace_period_ms);
  lmgr.start();

  sync_worker syncer(tree, 1000);
  syncer.start();

  file_size_tracker tracker(tree, 1000, storage_trace);
  if (!storage_trace.empty()) {
    LOG(log_level::info) << "Logging storage trace to " << storage_trace;
    tracker.start();
  }

  std::unique_lock<std::mutex> failure_condition_lock{failure_mtx};
  failure_condition.wait(failure_condition_lock, [&failing_thread] {
    return failing_thread != -1;
  });

  switch (failing_thread.load()) {
    case 0:
      if (alloc_exception) {
        try {
          std::rethrow_exception(alloc_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "Block allocation server failed: " << e.what();
        }
      }
      break;
    case 1:
      if (directory_exception) {
        try {
          std::rethrow_exception(directory_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "Directory server failed: " << e.what();
        }
      }
      break;
    case 2:
      if (lease_exception) {
        try {
          std::rethrow_exception(lease_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "Lease server failed: " << e.what();
          std::exit(-1);
        }
      }
      break;
    default:break;
  }
  return -1;
}