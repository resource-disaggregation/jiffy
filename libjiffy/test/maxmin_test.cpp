#include "catch.hpp"
#include "jiffy/directory/block/maxmin_block_allocator.h"
#include <random>

using namespace ::jiffy::directory;


TEST_CASE("maxmin_one", "[maxmin_algorithm_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 10}, {"C", 10}});
    auto alloc = std::make_shared<maxmin_block_allocator>(3, 0);
    alloc->total_blocks_ = 15;

    alloc->maxmin_algorithm_fast(demands);

    REQUIRE(alloc->allocations_["A"] == 5);
    REQUIRE(alloc->allocations_["B"] == 5);
    REQUIRE(alloc->allocations_["C"] == 5);
    REQUIRE(alloc->not_allocated_ == 0);
}

TEST_CASE("maxmin_two", "[maxmin_algorithm_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 3 }, {"B", 10}, {"C", 10}});
    auto alloc = std::make_shared<maxmin_block_allocator>(3, 0);
    alloc->total_blocks_ = 15;

    alloc->maxmin_algorithm_fast(demands);

    REQUIRE(alloc->allocations_["A"] == 3);
    REQUIRE(alloc->allocations_["B"] == 6);
    REQUIRE(alloc->allocations_["C"] == 6);
    REQUIRE(alloc->not_allocated_ == 0);
}


TEST_CASE("maxmin_three", "[maxmin_algorithm_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 1 }, {"B", 9}, {"C", 6}});
    auto alloc = std::make_shared<maxmin_block_allocator>(3, 0);
    alloc->total_blocks_ = 15;

    alloc->maxmin_algorithm_fast(demands);

    REQUIRE(alloc->allocations_["A"] == 1);
    REQUIRE(alloc->allocations_["B"] == 8);
    REQUIRE(alloc->allocations_["C"] == 6);
    REQUIRE(alloc->not_allocated_ == 0);
}

TEST_CASE("maxmin_four", "[maxmin_algorithm_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 1 }, {"B", 1}, {"C", 3}});
    auto alloc = std::make_shared<maxmin_block_allocator>(3, 0);
    alloc->total_blocks_ = 15;

    alloc->maxmin_algorithm_fast(demands);

    REQUIRE(alloc->allocations_["A"] == 1);
    REQUIRE(alloc->allocations_["B"] == 1);
    REQUIRE(alloc->allocations_["C"] == 3);
    REQUIRE(alloc->not_allocated_ == 10);
}


TEST_CASE("maxmin_random", "[maxmin_algorithm_fast]") {
    std::size_t num_tenants = 5;
    std::unordered_map<std::string, uint32_t> demands;

    std::mt19937 gen(1995);
    std::uniform_int_distribution<> distr(0, 20);
    for(std::size_t i = 0; i < num_tenants; i++)
    {
        demands["tenant" + std::to_string(i)] = distr(gen);
    }

    auto alloc = std::make_shared<maxmin_block_allocator>(num_tenants, 0);
    alloc->total_blocks_ = 50;

    alloc->maxmin_algorithm_fast(demands);

    
    uint32_t total_demand = 0;
    uint32_t total_allocation = 0;
    for(std::size_t i = 0; i < num_tenants; i++)
    {
        auto t = "tenant" + std::to_string(i);
        // Allocation should not exceed demand
        REQUIRE(alloc->allocations_[t] <= demands[t]);
        // Isolation
        REQUIRE(alloc->allocations_[t] >= std::min(demands[t], (uint32_t)10));

        total_demand += demands[t];
        total_allocation += alloc->allocations_[t];
    }

    // Work conservation
    REQUIRE(total_allocation == std::min(total_demand, (uint32_t) 50));
    REQUIRE(alloc->not_allocated_ == 50 - total_allocation);
}
