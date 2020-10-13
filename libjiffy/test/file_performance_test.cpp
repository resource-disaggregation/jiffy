#include "catch.hpp"
#include <vector>
#include <thread>
#include <string>
#include <boost/program_options.hpp>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/file/file_partition.h"

using namespace ::jiffy::client;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;


TEST_CASE("file_performance_test", "[write][read][performance]") {
    std::string address = "127.0.0.1";
    int service_port = 9090;
    int lease_port = 9091;
    int num_blocks = 1;
    int chain_length = 1;
    int num_ops = 100000;
    int data_size = 1024;
    
    std::string path = "/tmp";
    std::string backing_path = "local://tmp";
    // Output all the configuration parameters:
    LOG(log_level::info) << "host: " << address;
    LOG(log_level::info) << "service-port: " << service_port;
    LOG(log_level::info) << "lease-port: " << lease_port;
    LOG(log_level::info) << "num-blocks: " << num_blocks;
    LOG(log_level::info) << "chain-length: " << chain_length;
    LOG(log_level::info) << "num-ops: " << num_ops;
    LOG(log_level::info) << "data-size: " << data_size;
    LOG(log_level::info) << "path: " << path;
    LOG(log_level::info) << "backing-path: " << backing_path;
  

    block_memory_manager manager;
    file_partition block(&manager);
    std::size_t offset = 0;
    std::string data_ (data_size, 'x');

    auto bench_begin = time_utils::now_us();
    uint64_t tot_time = 0;

    for (int i = 0; i < num_ops; ++i) {
        response resp;
        block.write(resp, {"write", data_, std::to_string(offset)});
        offset += data_.size();
    }
	auto bench_end = time_utils::now_us();
	tot_time = bench_end - bench_begin;
	LOG(log_level::info) << "===== " << "file_write" << " ======";
	LOG(log_level::info) << "total_time: " << tot_time;
	LOG(log_level::info) << "\t" << 3 * num_ops << " requests completed in " << tot_time
											<< " us";
	LOG(log_level::info) << "\t" << data_size << " payload";
	LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";

    bench_begin = time_utils::now_us();
    tot_time = 0;
    int read_pos = 0;
    for (int i = 0; i < num_ops; ++i) {
        response resp;
        block.read(resp, {"read", std::to_string(read_pos), std::to_string(data_.size())});
        read_pos += data_.size();
    }
	bench_end = time_utils::now_us();
	tot_time = bench_end - bench_begin;
	LOG(log_level::info) << "===== " << "file_read" << " ======";
	LOG(log_level::info) << "total_time: " << tot_time;
	LOG(log_level::info) << "\t" << 3 * num_ops << " requests completed in " << tot_time
											<< " us";
	LOG(log_level::info) << "\t" << data_size << " payload";
	LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";
}