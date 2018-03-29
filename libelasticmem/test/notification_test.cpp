#include <catch.hpp>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>
#include "../src/storage/notification/subscriber.h"
#include "../src/storage/notification/notification_server.h"
#include "test_utils.h"

using namespace ::elasticmem::storage;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::protocol;

#define HOST "127.0.0.1"
#define PORT 9090

TEST_CASE("notification_test", "[subscribe][get_message]") {
  std::vector<std::shared_ptr<chain_module>> blocks = {std::make_shared<kv_block>("")};
  auto &sub_map = blocks[0]->subscriptions();

  auto server = notification_server::create(blocks, HOST, PORT);
  std::thread serve_thread([&server] { server->serve(); });
  test_utils::wait_till_server_ready(HOST, PORT);

  subscriber sub1(HOST, PORT);
  subscriber sub2(HOST, PORT);
  subscriber sub3(HOST, PORT);

  std::string op1 = "op1", op2 = "op2";
  std::string msg1 = "msg1", msg2 = "msg2";

  REQUIRE_NOTHROW(sub1.subscribe(0, {op1}));
  REQUIRE_NOTHROW(sub2.subscribe(0, {op1, op2}));
  REQUIRE_NOTHROW(sub3.subscribe(0, {op2}));

  REQUIRE_NOTHROW(sub_map.notify(op1, msg1));
  REQUIRE_NOTHROW(sub_map.notify(op2, msg2));

  REQUIRE(sub1.get_message() == std::make_pair(op1, msg1));
  REQUIRE(sub2.get_message() == std::make_pair(op1, msg1));
  REQUIRE(sub2.get_message() == std::make_pair(op2, msg2));
  REQUIRE(sub3.get_message() == std::make_pair(op2, msg2));

  REQUIRE_THROWS_AS(sub1.get_message(100), std::out_of_range);
  REQUIRE_THROWS_AS(sub2.get_message(100), std::out_of_range);
  REQUIRE_THROWS_AS(sub3.get_message(100), std::out_of_range);

  REQUIRE_NOTHROW(sub1.unsubscribe(0, {op1}));
  REQUIRE_NOTHROW(sub2.unsubscribe(0, {op2}));

  REQUIRE_NOTHROW(sub_map.notify(op1, msg1));
  REQUIRE_NOTHROW(sub_map.notify(op2, msg2));

  REQUIRE(sub2.get_message() == std::make_pair(op1, msg1));
  REQUIRE(sub3.get_message() == std::make_pair(op2, msg2));

  REQUIRE_THROWS_AS(sub1.get_message(100), std::out_of_range);
  REQUIRE_THROWS_AS(sub2.get_message(100), std::out_of_range);
  REQUIRE_THROWS_AS(sub3.get_message(100), std::out_of_range);

  sub1.disconnect();
  sub2.disconnect();
  sub3.disconnect();

  server->stop();
  if (serve_thread.joinable()) {
    serve_thread.join();
  }
}