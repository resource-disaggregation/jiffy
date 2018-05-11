#ifndef MMUX_BLOCK_NAME_PARSER_H
#define MMUX_BLOCK_NAME_PARSER_H

#include <string>

namespace mmux {
namespace storage {

struct block_id {
  std::string host;
  int32_t service_port;
  int32_t management_port;
  int32_t notification_port;
  int32_t chain_port;
  int32_t id;
};

class block_name_parser {
 public:
  static block_id parse(const std::string &name);
  static std::string make(const std::string &host,
                           int32_t service_port,
                           int32_t management_port,
                           int32_t notification_port,
                           int32_t chain_port,
                           int32_t id);
};

}
}

#endif //MMUX_BLOCK_NAME_PARSER_H
