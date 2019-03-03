#include "local_store.h"
#include "../../utils/directory_utils.h"

namespace jiffy {
namespace persistent {

using namespace utils;

local_store_impl::local_store_impl(std::shared_ptr<storage::serde> ser) : persistent_service(std::move(ser)) {}
template <typename Datatype>
void local_store_impl::write_impl(const Datatype &table, const std::string &out_path) {
  size_t found = out_path.find_last_of("/\\");
  auto dir = out_path.substr(0, found);
  directory_utils::create_directory(dir);
  std::shared_ptr<std::ofstream> out(new std::ofstream(out_path));
  serde()->serialize<Datatype>(table, out);
  out->close();
}
template <typename Datatype>
void local_store_impl::read_impl(const std::string &in_path, Datatype &table) {
  auto in = std::make_shared<std::ifstream>(in_path.c_str(), std::fstream::in);
  serde()->deserialize<Datatype>(in, table);
  in->close();
}

std::string local_store_impl::URI() {
  return "local";
}

}
}
