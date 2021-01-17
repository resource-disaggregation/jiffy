#define CATCH_CONFIG_RUNNER
#include <catch.hpp>

#include <thrift/TOutput.h>
#include "jiffy/utils/logger.h"

using namespace ::apache::thrift;
using namespace ::jiffy::utils;
using namespace Catch::clara;

int main(int argc, char *argv[]) {
  Catch::Session session;
  log_utils::configure_log_level(log_level::info);
  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);
  bool pmem_mode = false;
  std::string pmem_path = "/Shijie!!!!!!!!!!!";
  auto cli = session.cli()
            |Opt(pmem_mode, "pmem_mode")
              ["-p"]["--pmem"]
              ("Run test in PMEM mode. DRAM is default if '-p' is not specified.")
            |Opt(pmem_path, "pmem_path")
              ["--path"]
              ("Input the PMEM path you are using.");
  session.cli(cli);
  auto ret = session.applyCommandLine(argc, argv);
  if (ret) {
      return ret;
  }
  return session.run();
}
