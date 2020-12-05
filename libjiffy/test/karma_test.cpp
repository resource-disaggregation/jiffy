#include "catch.hpp"
#include "jiffy/directory/block/karma_block_allocator.h"

using namespace ::jiffy::directory;


TEST_CASE("matching_supply_one", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 0}, {"C", 1}, {"D", 1}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 1;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 0;
    alloc->allocations_["C"] = 1;
    alloc->allocations_["D"] = 1;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["B"] == 5);
    REQUIRE(alloc->rate_["C"] == 0);
    REQUIRE(alloc->rate_["D"] == 0);
    REQUIRE(alloc->allocations_["A"] == 10);
}

TEST_CASE("matching_supply_two", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 0}, {"C", 0}, {"D", 1}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 1000;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 0;
    alloc->allocations_["C"] = 0;
    alloc->allocations_["D"] = 1;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["B"] >= 2);
    REQUIRE(alloc->rate_["C"] >= 2);
    REQUIRE(((alloc->rate_["B"] == 3) || (alloc->rate_["C"] == 3)));
    REQUIRE(alloc->rate_["D"] == 0);
    REQUIRE(alloc->allocations_["A"] == 10);
}

// demands = {'A': [10], 'B': [0], 'C': [0], 'D': [0]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 100, 'C': 100, 'D': 100, '$public$': 0}
// alloc.allocations = {'A': [5], 'B': [0], 'C': [0], 'D': [0]}
// alloc.borrow_from_poorest_fast(0, ['B', 'C', 'D'], ['A'])
// assert sorted([alloc.rate_map[x] for x in ['B', 'C', 'D']]) == [1,2,2]
// assert alloc.allocations['A'][0] == 10

TEST_CASE("matching_supply_three", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 0}, {"C", 0}, {"D", 0}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 0;
    alloc->allocations_["C"] = 0;
    alloc->allocations_["D"] = 0;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates = {1,2,2};

    REQUIRE(rates == expected_rates);
    REQUIRE(alloc->allocations_["A"] == 10);
}

// def test_four(self):
// demands = {'A': [10], 'B': [0], 'C': [0], 'D': [0]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 100, 'C': 101, 'D': 102, '$public$': 0}
// alloc.allocations = {'A': [5], 'B': [0], 'C': [0], 'D': [0]}
// alloc.borrow_from_poorest_fast(0, ['B', 'C', 'D'], ['A'])
// res = sorted([alloc.rate_map[x] for x in ['B', 'C', 'D']])
// assert res == [1,2,2] or res == [0,2,3]
// assert alloc.allocations['A'][0] == 10

TEST_CASE("matching_supply_four", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 0}, {"C", 0}, {"D", 0}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 101;
    alloc->credits_["D"] = 102;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 0;
    alloc->allocations_["C"] = 0;
    alloc->allocations_["D"] = 0;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates1 = {1,2,2};
    std::vector<int32_t> expected_rates2 = {0,2,3};

    REQUIRE(((rates == expected_rates1)||(rates == expected_rates2)));
    REQUIRE(alloc->allocations_["A"] == 10);
}

// def test_eight(self):
// demands = {'A': [10], 'B': [4], 'C': [0], 'D': [0]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 1, 'C': 100, 'D': 100, '$public$': 0}
// alloc.allocations = {'A': [5], 'B': [4], 'C': [0], 'D': [0]}
// alloc.borrow_from_poorest_fast(0, ['B', 'C', 'D'], ['A'])
// assert alloc.rate_map['B'] == 1
// assert alloc.rate_map['C'] == 2
// assert alloc.rate_map['D'] == 2
// assert alloc.allocations['A'][0] == 10

TEST_CASE("matching_supply_eight", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 4}, {"C", 0}, {"D", 0}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 1;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 4;
    alloc->allocations_["C"] = 0;
    alloc->allocations_["D"] = 0;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["B"] == 1);
    REQUIRE(alloc->rate_["C"] == 2);
    REQUIRE(alloc->rate_["D"] == 2);
    REQUIRE(alloc->allocations_["A"] == 10);
}

TEST_CASE("matching_supply_largec", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 0}, {"C", 0}, {"D", 0}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 9999999999LL + 10;
    alloc->credits_["B"] = 9999999999LL + 100;
    alloc->credits_["C"] = 9999999999LL + 100;
    alloc->credits_["D"] = 9999999999LL + 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 0;
    alloc->allocations_["C"] = 0;
    alloc->allocations_["D"] = 0;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates = {1,2,2};

    REQUIRE(rates == expected_rates);
    REQUIRE(alloc->allocations_["A"] == 10);
}


TEST_CASE("matching_supply_five", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10 }, {"B", 4}, {"C", 4}, {"D", 4}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 5);
    alloc->total_blocks_ = 25;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 4;
    alloc->allocations_["C"] = 4;
    alloc->allocations_["D"] = 4;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["B"] == 1);
    REQUIRE(alloc->rate_["C"] == 1);
    REQUIRE(alloc->rate_["D"] == 1);
    REQUIRE(alloc->rate_["$public$"] == 2);
    REQUIRE(alloc->allocations_["A"] == 10);
}

TEST_CASE("matching_supply_six", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10}, {"B", 3}, {"C", 4}, {"D", 4}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 5);
    alloc->total_blocks_ = 25;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 1;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 3;
    alloc->allocations_["C"] = 4;
    alloc->allocations_["D"] = 4;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["B"] == 2);
    REQUIRE(alloc->rate_["C"] == 1);
    REQUIRE(alloc->rate_["D"] == 1);
    REQUIRE(alloc->rate_["$public$"] == 1);
    REQUIRE(alloc->allocations_["A"] == 10);
}


TEST_CASE("matching_supply_seven", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 10}, {"B", 3}, {"C", 3}, {"D", 4}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 5);
    alloc->total_blocks_ = 25;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 1;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 5;
    alloc->allocations_["B"] = 3;
    alloc->allocations_["C"] = 3;
    alloc->allocations_["D"] = 4;

    std::vector<std::string> donors = {"B", "C", "D"};
    std::vector<std::string> borrowers = {"A"};
    alloc->borrow_from_poorest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["B"] == 2);
    REQUIRE(alloc->rate_["C"] == 2);
    REQUIRE(alloc->rate_["D"] == 1);
    REQUIRE(alloc->rate_["$public$"] == 0);
    REQUIRE(alloc->allocations_["A"] == 10);
}


TEST_CASE("matching_demand_one", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 10}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 1;
    alloc->credits_["D"] = 1;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["A"] == 5);
    REQUIRE(alloc->allocations_["B"] == 10);
    REQUIRE(alloc->rate_["B"] == -5);
    REQUIRE(alloc->allocations_["C"] == 5);
    REQUIRE(alloc->rate_["C"] == 0);
    REQUIRE(alloc->allocations_["D"] == 5);
    REQUIRE(alloc->rate_["D"] == 0);
    
}


// demands = {'A': [0], 'B': [10], 'C': [10], 'D': [10]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 100, 'C': 100, 'D': 1, '$public$': 0}
// alloc.allocations = {'A': [0], 'B': [5], 'C': [5], 'D': [5]}
// alloc.give_to_richest_fast(0, ['A'], ['B', 'C', 'D'])
// assert sorted([alloc.rate_map[x] for x in ['B', 'C', 'D']]) == [-3,-2,0]
// assert sorted([alloc.allocations[x][0] for x in ['B', 'C', 'D']]) == [5,7,8]
TEST_CASE("matching_demand_two", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 10}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 1;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates = {-3,-2,0};
    REQUIRE(rates == expected_rates);
    std::vector<uint32_t> allocations = {alloc->allocations_["B"], alloc->allocations_["C"], alloc->allocations_["D"]};
    std::sort(allocations.begin(), allocations.end());
    std::vector<uint32_t> expected_allocations = {5,7,8};
    REQUIRE(allocations == expected_allocations);
}

// demands = {'A': [0], 'B': [10], 'C': [10], 'D': [10]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 100, 'C': 100, 'D': 100, '$public$': 0}
// alloc.allocations = {'A': [0], 'B': [5], 'C': [5], 'D': [5]}
// alloc.give_to_richest_fast(0, ['A'], ['B', 'C', 'D'])
// assert sorted([alloc.rate_map[x] for x in ['B', 'C', 'D']]) == [-2,-2,-1]
// assert sorted([alloc.allocations[x][0] for x in ['B', 'C', 'D']]) == [6,7,7]
TEST_CASE("matching_demand_three", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 10}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 100;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates = {-2,-2,-1};
    REQUIRE(rates == expected_rates);
    std::vector<uint32_t> allocations = {alloc->allocations_["B"], alloc->allocations_["C"], alloc->allocations_["D"]};
    std::sort(allocations.begin(), allocations.end());
    std::vector<uint32_t> expected_allocations = {6,7,7};
    REQUIRE(allocations == expected_allocations);
}

// demands = {'A': [0], 'B': [10], 'C': [10], 'D': [10]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 102, 'C': 101, 'D': 100, '$public$': 0}
// alloc.allocations = {'A': [0], 'B': [5], 'C': [5], 'D': [5]}
// alloc.give_to_richest_fast(0, ['A'], ['B', 'C', 'D'])
// res = sorted([alloc.rate_map[x] for x in ['B', 'C', 'D']])
// assert res == [-3,-2,0] or res == [-2,-2,-1]
// res = sorted([alloc.allocations[x][0] for x in ['B', 'C', 'D']])
// assert res == [5,7,8] or res == [6,7,7]
TEST_CASE("matching_demand_four", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 10}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 102;
    alloc->credits_["C"] = 101;
    alloc->credits_["D"] = 100;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates1 = {-2,-2,-1};
    std::vector<int32_t> expected_rates2 = {-3,-2,0};
    REQUIRE(((rates == expected_rates1)||(rates == expected_rates2)));
    std::vector<uint32_t> allocations = {alloc->allocations_["B"], alloc->allocations_["C"], alloc->allocations_["D"]};
    std::sort(allocations.begin(), allocations.end());
    std::vector<uint32_t> expected_allocations1 = {6,7,7};
    std::vector<uint32_t> expected_allocations2 = {5,7,8};
    REQUIRE((allocations == expected_allocations1 || allocations == expected_allocations2));
}


// demands = {'A': [0], 'B': [6], 'C': [10], 'D': [10]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 100, 'C': 10, 'D': 10, '$public$': 0}
// alloc.allocations = {'A': [0], 'B': [5], 'C': [5], 'D': [5]}
// alloc.give_to_richest_fast(0, ['A'], ['B', 'C', 'D'])
// assert alloc.rate_map['A'] == 5
// assert alloc.allocations['B'][0] == 6
// assert alloc.rate_map['B'] == -1
// assert alloc.allocations['C'][0] == 7
// assert alloc.rate_map['C'] == -2
// assert alloc.allocations['D'][0] == 7
// assert alloc.rate_map['D'] == -2
TEST_CASE("matching_demand_five", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 6}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 100;
    alloc->credits_["C"] = 10;
    alloc->credits_["D"] = 10;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["A"] == 5);
    REQUIRE(alloc->allocations_["B"] == 6);
    REQUIRE(alloc->rate_["B"] == -1);
    REQUIRE(alloc->allocations_["C"] == 7);
    REQUIRE(alloc->rate_["C"] == -2);
    REQUIRE(alloc->allocations_["D"] == 7);
    REQUIRE(alloc->rate_["D"] == -2);
    
}

// demands = {'A': [0], 'B': [10], 'C': [10], 'D': [10]}
// num_epochs = len(demands[list(demands.keys())[0]])
// alloc = Allocator(demands, total_blocks=20, init_credits=0)
// alloc.credit_map = {'A': 10, 'B': 4, 'C': 1, 'D': 0, '$public$': 0}
// alloc.allocations = {'A': [0], 'B': [5], 'C': [5], 'D': [5]}
// alloc.give_to_richest_fast(0, ['A'], ['B', 'C', 'D'])
// assert alloc.rate_map['A'] == 5
// assert alloc.allocations['B'][0] == 9
// assert alloc.rate_map['B'] == -4
// assert alloc.allocations['C'][0] == 6
// assert alloc.rate_map['C'] == -1
// assert alloc.allocations['D'][0] == 5
// assert alloc.rate_map['D'] == 0
TEST_CASE("matching_demand_six", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 10}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 10;
    alloc->credits_["B"] = 4;
    alloc->credits_["C"] = 1;
    alloc->credits_["D"] = 0;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    REQUIRE(alloc->rate_["A"] == 5);
    REQUIRE(alloc->allocations_["B"] == 9);
    REQUIRE(alloc->rate_["B"] == -4);
    REQUIRE(alloc->allocations_["C"] == 6);
    REQUIRE(alloc->rate_["C"] == -1);
    REQUIRE(alloc->allocations_["D"] == 5);
    REQUIRE(alloc->rate_["D"] == 0);
    
}

TEST_CASE("matching_demand_largec", "[borrow_from_poorest_fast]") {
    std::unordered_map<std::string, uint32_t> demands({{"A", 0}, {"B", 10}, {"C", 10}, {"D", 10}});
    auto alloc = std::make_shared<karma_block_allocator>(4, 0, 0, 0);
    alloc->total_blocks_ = 20;
    alloc->credits_["A"] = 9999999999LL + 10;
    alloc->credits_["B"] = 9999999999LL + 100;
    alloc->credits_["C"] = 9999999999LL + 100;
    alloc->credits_["D"] = 9999999999LL + 100;

    alloc->allocations_["A"] = 0;
    alloc->allocations_["B"] = 5;
    alloc->allocations_["C"] = 5;
    alloc->allocations_["D"] = 5;

    std::vector<std::string> donors = {"A"};
    std::vector<std::string> borrowers = {"B", "C", "D"};
    alloc->give_to_richest_fast(demands, donors, borrowers);

    std::vector<int32_t> rates = {alloc->rate_["B"], alloc->rate_["C"], alloc->rate_["D"]};
    std::sort(rates.begin(), rates.end());
    std::vector<int32_t> expected_rates = {-2,-2,-1};
    REQUIRE(rates == expected_rates);
    std::vector<uint32_t> allocations = {alloc->allocations_["B"], alloc->allocations_["C"], alloc->allocations_["D"]};
    std::sort(allocations.begin(), allocations.end());
    std::vector<uint32_t> expected_allocations = {6,7,7};
    REQUIRE(allocations == expected_allocations);
}


TEST_CASE("karma_e2e_one", "[karma_algo_fast]") {
    auto alloc = std::make_shared<karma_block_allocator>(3, 10, 0, 3);
    alloc->total_blocks_ = 33;
    alloc->register_tenant("A");
    alloc->register_tenant("B");
    alloc->register_tenant("C");

    alloc->demands_["A"] = 10;
    alloc->demands_["B"] = 10;
    alloc->demands_["C"] = 10;
    alloc->compute_allocations();
    REQUIRE(alloc->allocations_["A"] == 10);
    REQUIRE(alloc->allocations_["B"] == 10);
    REQUIRE(alloc->allocations_["C"] == 10);
    REQUIRE(alloc->credits_["A"] == 10);
    REQUIRE(alloc->credits_["B"] == 10);
    REQUIRE(alloc->credits_["C"] == 10);

    alloc->demands_["A"] = 2;
    alloc->demands_["B"] = 12;
    alloc->demands_["C"] = 16;
    alloc->compute_allocations();
    REQUIRE(alloc->allocations_["A"] == 2);
    REQUIRE(alloc->allocations_["B"] == 12);
    REQUIRE(alloc->allocations_["C"] == 16);
    REQUIRE(alloc->credits_["A"] == 18);
    REQUIRE(alloc->credits_["B"] == 8);
    REQUIRE(alloc->credits_["C"] == 4);

    alloc->demands_["A"] = 6;
    alloc->demands_["B"] = 12;
    alloc->demands_["C"] = 6;
    alloc->compute_allocations();
    REQUIRE(alloc->allocations_["A"] == 6);
    REQUIRE(alloc->allocations_["B"] == 12);
    REQUIRE(alloc->allocations_["C"] == 6);
    REQUIRE(alloc->credits_["A"] == 18);
    REQUIRE(alloc->credits_["B"] == 6);
    REQUIRE(alloc->credits_["C"] == 6);

    alloc->demands_["A"] = 18;
    alloc->demands_["B"] = 5;
    alloc->demands_["C"] = 12;
    alloc->compute_allocations();
    REQUIRE(alloc->allocations_["A"] == 18);
    REQUIRE(alloc->allocations_["B"] == 5);
    REQUIRE(alloc->allocations_["C"] == 10);
    REQUIRE(alloc->credits_["A"] == 10);
    REQUIRE(alloc->credits_["B"] == 11);
    REQUIRE(alloc->credits_["C"] == 6);
    REQUIRE(alloc->credits_["$public$"] == 3);

    alloc->demands_["A"] = 10;
    alloc->demands_["B"] = 10;
    alloc->demands_["C"] = 10;
    alloc->compute_allocations();
    REQUIRE(alloc->allocations_["A"] == 10);
    REQUIRE(alloc->allocations_["B"] == 10);
    REQUIRE(alloc->allocations_["C"] == 10);
    REQUIRE(alloc->credits_["A"] == 11);
    REQUIRE(alloc->credits_["B"] == 12);
    REQUIRE(alloc->credits_["C"] == 7);
    REQUIRE(alloc->credits_["$public$"] == 0);
}

TEST_CASE("karma_reclaim_one", "[karma_algo_fast]") {
    auto alloc = std::make_shared<karma_block_allocator>(3, 10, 0, 3);
    std::vector<std::string> blocks;
    for(int i = 0; i < 33; i++) 
    {
        blocks.push_back("block" + std::to_string(i));
    }
    alloc->add_blocks(blocks);


    alloc->total_blocks_ = 33;
    alloc->register_tenant("A");
    alloc->register_tenant("B");
    alloc->register_tenant("C");

    alloc->demands_["A"] = 10;
    alloc->demands_["B"] = 5;
    alloc->demands_["C"] = 15;
    alloc->compute_allocations();
    REQUIRE(alloc->allocations_["A"] == 10);
    REQUIRE(alloc->allocations_["B"] == 5);
    REQUIRE(alloc->allocations_["C"] == 15);

    for(int i = 0; i < 10; i++) 
    {
        REQUIRE_NOTHROW(alloc->allocate(1, {}, "A"));
    }
    REQUIRE_THROWS(alloc->allocate(1, {}, "A"));

    for(int i = 0; i < 15; i++) 
    {
        REQUIRE_NOTHROW(alloc->allocate(1, {}, "C"));
    }
    REQUIRE_THROWS(alloc->allocate(1, {}, "C"));

    for(int i = 0; i < 5; i++) 
    {
        REQUIRE_NOTHROW(alloc->allocate(1, {}, "B"));
    }
    REQUIRE_NOTHROW(alloc->allocate(1, {}, "B"));
    REQUIRE(alloc->credits_["C"] == 5);
    REQUIRE(alloc->rate_["C"] == -5);
    REQUIRE(alloc->credits_["A"] == 10);
    REQUIRE(alloc->rate_["A"] == 0);
    REQUIRE(alloc->credits_["$public$"] == 1);
    REQUIRE(alloc->rate_["$public$"] == 1);


    REQUIRE_NOTHROW(alloc->allocate(1, {}, "B"));
    REQUIRE(alloc->credits_["C"] == 5);
    REQUIRE(alloc->rate_["C"] == -5);
    REQUIRE(alloc->credits_["$public$"] == 2);
    REQUIRE(alloc->rate_["$public$"] == 2);

    REQUIRE_NOTHROW(alloc->allocate(1, {}, "B"));
    REQUIRE(alloc->credits_["C"] == 5);
    REQUIRE(alloc->rate_["C"] == -5);
    REQUIRE(alloc->credits_["$public$"] == 3);
    REQUIRE(alloc->rate_["$public$"] == 3);

    REQUIRE_NOTHROW(alloc->allocate(1, {}, "B"));
    REQUIRE(alloc->credits_["C"] == 6);
    REQUIRE(alloc->rate_["C"] == -4);
    REQUIRE(alloc->credits_["$public$"] == 3);
    REQUIRE(alloc->rate_["$public$"] == 3);

    REQUIRE_NOTHROW(alloc->allocate(1, {}, "B"));
    REQUIRE(alloc->credits_["C"] == 7);
    REQUIRE(alloc->rate_["C"] == -3);
    REQUIRE(alloc->credits_["$public$"] == 3);
    REQUIRE(alloc->rate_["$public$"] == 3);

    REQUIRE_THROWS(alloc->allocate(1, {}, "B"));

}