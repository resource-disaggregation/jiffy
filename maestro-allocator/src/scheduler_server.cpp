#include <jiffy/storage/manager/storage_manager.h>
#include <jiffy/directory/fs/directory_server.h>
#include <jiffy/maestro/controller/maestro_allocator_client.h>
#include <jiffy/maestro/scheduler/scheduler_block_allocator.h>
#include <jiffy/directory/lease/lease_server.h>
#include <exception>

using namespace ::jiffy::directory;
using namespace ::jiffy::maestro;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;

int main(int argc, char **argv) {

  std::string address = "127.0.0.1";
  int service_port = 9001;
  int lease_port = 9091;

  std::string tenantID = "TENANTA";
  std::string allocator_service_address = "127.0.0.1";
  int allocator_service_port = 9090;
  uint64_t lease_period_ms = 10000;
  uint64_t grace_period_ms = 10000;

  std::mutex failure_mtx;
  std::condition_variable failure_condition;
  std::atomic<int> failing_thread(-1); // alloc -> 0, directory -> 1, lease -> 2


  std::exception_ptr directory_exception = nullptr;

  auto client = std::make_shared<maestro_allocator_client>(allocator_service_address, allocator_service_port);
  auto allocator_client = std::make_shared<scheduler_block_allocator>(tenantID, client);
  auto storage = std::make_shared<storage_manager>();
  auto tree = std::make_shared<directory_tree>(allocator_client, storage);
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

  std::unique_lock<std::mutex> failure_condition_lock{failure_mtx};
  failure_condition.wait(failure_condition_lock, [&failing_thread] {
    return failing_thread != -1;
  });

  switch (failing_thread.load()) {
    case 0:
      if (directory_exception) {
        try {
          std::rethrow_exception(directory_exception);
        } catch (std::exception &e) {
          LOG(log_level::error) << "directory server failed: " << e.what();
        }
      }
      break;
    default:break;
  }
  return -1;

}