#ifndef MMUX_BLOCK_NAME_PARSER_H
#define MMUX_BLOCK_NAME_PARSER_H

#include <string>

namespace mmux {
namespace storage {
/* Structure of block id */
struct block_id {
  std::string host;
  int32_t service_port;
  int32_t management_port;
  int32_t notification_port;
  int32_t chain_port;
  int32_t id;
};
/* Block name parser class */
class block_name_parser {
 public:

  /**
   * @brief Block name parser
   * @param name Block name
   * @return Block id structure
   */

  static block_id parse(const std::string &name);

  /**
   * @brief Make a block name by connecting all parts in a single string
   * @param host Host
   * @param service_port Service port number
   * @param management_port Management port number
   * @param notification_port Notification port number
   * @param chain_port Chain port number
   * @param id Block id number
   * @return Block name string
   */

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
