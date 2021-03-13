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
  std::string mode = "DRAM";
  std::string pmem_path;
  auto cli = session.cli()
            |Opt(mode, "pmem_mode")
              ["-m"]["--mode"]
              ("Run test in PMEM mode. DRAM is default if '-p' is not specified.")
            |Opt(pmem_path, "pmem_path")
              ["--path"]
              ("Input the PMEM path you are using.");
  session.cli(cli);
  auto ret = session.applyCommandLine(argc, argv);
  if (ret) {
      return ret;
  }
  if (mode == "DRAM" || mode == "PMEM") {
    setenv("JIFFY_TEST_MODE", mode.c_str(), 1);
  }
  setenv("PMEM_PATH", pmem_path.c_str(), 1);
  return session.run();
}
