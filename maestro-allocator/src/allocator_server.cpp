#include <iostream>
#include <vector>
#include <thread>
#include <jiffy/directory/fs/directory_tree.h>
#include <jiffy/directory/block/random_block_allocator.h>
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

int main(int argc, char **argv) {

    std::string address = "127.0.0.1";
    int block_port = 9092;

    std::mutex failure_mtx;
    std::condition_variable failure_condition;
    std::atomic<int> failing_thread(-1); // alloc -> 0

    std::exception_ptr alloc_exception = nullptr;
    auto alloc = std::make_shared<random_block_allocator>();
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
    LOG(log_level::info) << "Allocation server listening on " << address << ":" << block_port;

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
                    LOG(log_level::error) << "allocation server failed: " << e.what();
                }
            }
            break;
        default:break;
    }
    return -1;

}