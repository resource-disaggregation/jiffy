#ifndef ELASTICMEM_BLOCK_NAME_PARSER_H
#define ELASTICMEM_BLOCK_NAME_PARSER_H

#include <string>

namespace elasticmem {
namespace storage {

class block_name_parser {
 public:
  static std::tuple<std::string, int, int> parse(const std::string &name);
  static std::string make_block_name(const std::tuple<std::string, int, int> &t);
};

}
}

#endif //ELASTICMEM_BLOCK_NAME_PARSER_H
