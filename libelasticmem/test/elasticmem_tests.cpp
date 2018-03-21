#define CATCH_CONFIG_RUNNER
#include <catch.hpp>

#include <thrift/TOutput.h>
#include "../src/utils/logger.h"

using namespace ::apache::thrift;
using namespace ::elasticmem::utils;

int main( int argc, char* argv[] ) {
  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);
  return Catch::Session().run( argc, argv );
}