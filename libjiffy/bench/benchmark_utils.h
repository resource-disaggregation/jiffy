#ifndef JIFFY_BENCHMARK_UTILS_H
#define JIFFY_BENCHMARK_UTILS_H

#include <string>
#include <vector>
#include <iostream>
#include <cstdint>

#include "jiffy/storage/hashtable/hash_table_ops.h"

namespace jiffy {
namespace benchmark {

using namespace storage;

class benchmark_utils {
 public:
  static void load_workload(const std::string &workload_path,
                            std::size_t workload_offset,
                            std::size_t num_ops,
                            std::vector<std::pair<int32_t, std::vector<std::string>>> &workload) {
    workload.clear();
    std::ifstream in(workload_path);
    std::string line;
    std::cerr << "Loading workload from " << workload_path << std::endl;
    std::size_t line_num = 0;
    while (std::getline(in, line) && workload.size() < num_ops) {
      if (line_num++ < workload_offset)
        continue;
      std::istringstream iss(line);
      std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                      std::istream_iterator<std::string>{}};
      int32_t cmd_id = -1;
      if (tokens[0] == "get") {
        cmd_id = hash_table_cmd_id::get;
      } else if (tokens[0] == "put") {
        cmd_id = hash_table_cmd_id::put;
      } else if (tokens[0] == "remove") {
        cmd_id = hash_table_cmd_id::remove;
      } else if (tokens[0] == "update") {
        cmd_id = hash_table_cmd_id::update;
      } else if (tokens[0] == "wait") {
        continue;
      } else {
        throw std::logic_error("Unknown operation " + tokens[0]);
      }
      tokens.erase(tokens.begin());
      workload.emplace_back(cmd_id, tokens);
    }
    std::cerr << "Loaded " << workload.size() << " entries from " << workload_path << std::endl;
  }

  template<typename Container>
  static void vector_diff(const Container &A, const Container &B, const std::string &output_file) {
    if (A.size() != B.size()) {
      throw std::logic_error("Cannot compute diff of unequally sized containers");
    }
    std::ofstream out(output_file);
    for (size_t i = 0; i < A.size(); ++i) {
      out << (A[i] - B[i]) << std::endl;
    }
    out.close();
  }
};

}
}

#endif //JIFFY_BENCHMARK_UTILS_H
