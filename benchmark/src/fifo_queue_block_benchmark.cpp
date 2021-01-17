#include <vector>
#include <thread>
#include <iostream>
#include <string>
#include <boost/program_options.hpp>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
#include "jiffy/storage/fifoqueue/fifo_queue_defs.h"
#include "jiffy/storage/fifoqueue/fifo_queue_partition.h"
#include <memkind.h>

namespace  bpo = boost::program_options;

int main(int argc, char const *argv[])
{

    bpo::options_description opts("all options"); 
    bpo::variables_map vm;

    opts.add_options()  
    ("pmem", bpo::value<std::string>(), "Run the benchmark under PMEM mode. Usage: '-pmem=PMEM_ADDRESS'.")
    ("dram", "Run the benchmark under DRAM mode. Usage: '-dram'.")
    ("help", "This benchmark only runs by block.");

    try {
        bpo::store(bpo::parse_command_line(argc, argv, opts), vm);
    }
    catch(...) {
        std::cout << "Wrong command line arguments! Please use '-help' to see how to correctly use arguments.\n";
        return 0;
    }
    
    if (vm.size() > 1 || vm.empty()) {
        std::cout << "Please use one option. See '-help' for more information. \n";
    }
    if (vm.count("help")) {
        std::cout << opts << std::endl;   
    }
    
    else if (vm.count("dram")) {
        struct memkind* pmem_kind = nullptr;
        std::string memory_mode = "DRAM";
    }
    if (vm.count("pmem")) {
        struct memkind* pmem_kind = nullptr;
        std::string memory_mode = "PMEM";
        std::string pmem_path = vm["pmem"].as<std::string>();
        size_t err = memkind_create_pmem(pmem_path.c_str(),0,&pmem_kind);
        if (err) {
            char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
            memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
            fprintf(stderr, "%s\n", error_message);
        }
    }

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
  
    size_t capacity = 134217728;
    block_memory_manager manager(capacity, memory_mode, pmem_kind);
    fifo_queue_partition block(&manager);

    auto bench_begin = time_utils::now_us();
    uint64_t tot_time = 0;
    std::string data_ (data_size, 'x');

    for (int i = 0; i < num_ops; ++i) {
        response resp;
        block.enqueue(resp, {"enqueue", data_});
    }
	auto bench_end = time_utils::now_us();
	tot_time = bench_end - bench_begin;
	LOG(log_level::info) << "===== " << "fifo_queue_enqueue" << " ======";
	LOG(log_level::info) << "total_time: " << tot_time;
	LOG(log_level::info) << "\t" << num_ops << " requests completed in " << tot_time
											<< " us";
	LOG(log_level::info) << "\t" << data_size << " payload";
	LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";

    bench_begin = time_utils::now_us();
    tot_time = 0;
    int read_pos = 0;
    for (int i = 0; i < num_ops; ++i) {
        response resp;
        block.dequeue(resp, {"dequeue"});
    }
	bench_end = time_utils::now_us();
	tot_time = bench_end - bench_begin;
	LOG(log_level::info) << "===== " << "fifo_queue_dequeue" << " ======";
	LOG(log_level::info) << "total_time: " << tot_time;
	LOG(log_level::info) << "\t" << num_ops << " requests completed in " << tot_time
											<< " us";
	LOG(log_level::info) << "\t" << data_size << " payload";
	LOG(log_level::info) << "\tThroughput: " << num_ops * 1E3 / tot_time << " requests per microsecond";
    
    memkind_destroy_kind(pmem_kind);
}