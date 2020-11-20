#include "catch.hpp"
#include <vector>
#include <thread>
#include <string>
#include <boost/program_options.hpp>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
#include "jiffy/storage/shared_log/shared_log_defs.h"
#include "jiffy/storage/shared_log/shared_log_partition.h"
#include <memkind.h>

using namespace ::jiffy::client;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;


TEST_CASE("shared_log_performance_test", "[write][read][performance]") {
    std::string address = "127.0.0.1";
    int service_port = 9090;
    int lease_port = 9091;
    int num_blocks = 1;
    int chain_length = 1;
    int num_ops = 1024;
    int data_size = 64;
    
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
  

    struct memkind* pmem_kind = nullptr;
    std::string pmem_path = "/media/pmem0/shijie"; 
    std::string memory_mode = "DRAM";
    size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
    if(err) {
        char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
        memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
        fprintf(stderr, "%s\n", error_message);
    }
    size_t capacity = 134217728;
    block_memory_manager manager(capacity, memory_mode, pmem_kind);
    shared_log_partition block(&manager);
    std::size_t offset = 0;
    std::string data_ (data_size, 'x');

    auto bench_begin = time_utils::now_us();
    uint64_t tot_time = 0;

    for (int i = 0; i < num_ops; ++i) {
        response resp;
        block.write(resp, {"write", std::to_string(i), data_, std::to_string(i)+"_stream"});
        offset += (71 + std::to_string(i).size());
    }
	auto bench_end = time_utils::now_us();
	tot_time = bench_end - bench_begin;
	LOG(log_level::info) << "===== " << "shared_log_write" << " ======";
	LOG(log_level::info) << "total_time: " << tot_time;
	LOG(log_level::info) << "\t" << num_ops << " requests completed in " << tot_time
											<< " us";
	LOG(log_level::info) << "\t" << data_size << " payload";
	LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";

    
    int scan_size = 1;
    while (scan_size <= num_ops) {
        bench_begin = time_utils::now_us();
        tot_time = 0;
        for (int j = 0; j < num_ops/scan_size; j+= scan_size){
            response resp;
            std::vector<std::string> scan_args = {"scan", std::to_string(j), std::to_string(j + scan_size - 1), std::to_string(j)+"_stream", std::to_string(j+1)+"_stream"};
            block.scan(resp, scan_args);
        }
        
        bench_end = time_utils::now_us();
        tot_time = bench_end - bench_begin;
        LOG(log_level::info) << "===== " << "shared_log_scan" << " ======";
        LOG(log_level::info) << "total_time: " << tot_time;
        LOG(log_level::info) << "scan_entry_size: " << scan_size;
        LOG(log_level::info) << "\t" << num_ops / scan_size << " requests completed in " << tot_time
                                                << " us";
        LOG(log_level::info) << "\t" << data_size << " payload";
        LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";

        scan_size *= 2;
    }
	
    
    bench_begin = time_utils::now_us();
    tot_time = 0;
    for (int i = 0; i < num_ops/16; ++i) {
        response resp;
        block.trim(resp, {"trim", std::to_string(i), std::to_string(i+16)});
    }
	bench_end = time_utils::now_us();
	tot_time = bench_end - bench_begin;
	LOG(log_level::info) << "===== " << "shared_log_trim" << " ======";
	LOG(log_level::info) << "total_time: " << tot_time;
	LOG(log_level::info) << "\t" << num_ops/16 << " requests completed in " << tot_time
											<< " us";
	LOG(log_level::info) << "\t" << data_size << " payload";
	LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";
    
    
    // memkind_destroy_kind(pmem_kind);
}