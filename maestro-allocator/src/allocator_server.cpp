#include <iostream>
#include <vector>
#include <thread>
#include <jiffy/maestro/free_block_allocator.h>
#include <jiffy/directory/block/block_registration_server.h>
#include <jiffy/directory/fs/directory_tree.h>
#include <jiffy/utils/logger.h>
#include <jiffy/maestro/allocator_service.h>
#include <jiffy/maestro/controller/maestro_alloactor_server.h>

using namespace ::jiffy::maestro;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;

int main(int argc, char **argv) {

    std::string address = "127.0.0.1";
    int block_port = 9092;
    int service_port = 9090;

    std::mutex failure_mtx;
    std::condition_variable failure_condition;
    std::atomic<int> failing_thread(-1); // alloc -> 0

    std::exception_ptr free_pool_exception = nullptr;
    auto alloc = std::make_shared<free_block_allocator>();
    auto alloc_server = block_registration_server::create(alloc, address, block_port);
    std::thread alloc_serve_thread([&free_pool_exception, &alloc_server, &failing_thread, &failure_condition] {
        try {
            alloc_server->serve();
        } catch (...) {
            free_pool_exception = std::current_exception();
            failing_thread = 0;
            failure_condition.notify_all();
        }
    });
    LOG(log_level::info) << "Free pool server listening on " << address << ":" << block_port;


    std::exception_ptr allocator_exception = nullptr;
    auto allocator = std::make_shared<allocator_service>();
    auto maestro_alloactor_server = maestro_alloactor_server::create(allocator, address, service_port);
    std::thread directory_serve_thread([&allocator_exception, &maestro_alloactor_server, &failing_thread, &failure_condition] {
      try {
        maestro_alloactor_server->serve();
      } catch (...) {
        allocator_exception = std::current_exception();
        failing_thread = 1;
        failure_condition.notify_all();
      }
    });

    LOG(log_level::info) << "Allocator server listening on " << address << ":" << service_port;

    std::unique_lock<std::mutex> failure_condition_lock{failure_mtx};
    failure_condition.wait(failure_condition_lock, [&failing_thread] {
        return failing_thread != -1;
    });

    switch (failing_thread.load()) {
        case 0:
            if (free_pool_exception) {
                try {
                    std::rethrow_exception(free_pool_exception);
                } catch (std::exception &e) {
                    LOG(log_level::error) << "Free pool server failed: " << e.what();
                }
            }
            break;
      case 1:
        if (allocator_exception) {
          try {
            std::rethrow_exception(allocator_exception);
          } catch (std::exception &e) {
            LOG(log_level::error) << "Allocator server failed: " << e.what();
          }
        }
        break;
      default:break;
    }
    return -1;

}