#include "catch.hpp"
#include "jiffy/directory/block/bheap.h"

using namespace ::jiffy::directory;


// h.push('a', 7)
//         h.push('b', 1)
//         assert h.size() == 2
//         h.add_to_all(10)
//         k, v = h.pop()
//         assert k == 'b'
//         assert v == 11
//         h.push('c', 1)
//         assert h.size() == 2
//         k, v = h.pop()
//         assert k == 'c'
//         assert v == 1
TEST_CASE("bheap_test", "[bheap]") {

    auto h = BroadcastHeap();
    REQUIRE(h.size() == 0);
    h.push("a", 7);
    h.push("b", 1);
    REQUIRE(h.size() == 2);
    h.add_to_all(10);
    auto elem = h.pop();
    REQUIRE(elem.first == "b");
    REQUIRE(elem.second == 11);
    h.push("c", 1);
    REQUIRE(h.size() == 2);
    elem = h.pop();
    REQUIRE(elem.first == "c");
    REQUIRE(elem.second == 1);

}