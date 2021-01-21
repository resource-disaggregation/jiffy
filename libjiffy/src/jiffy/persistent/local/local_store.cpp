#include "local_store.h"
#include "../../utils/directory_utils.h"

namespace jiffy {
namespace persistent {

using namespace utils;

local_store::local_store(std::shared_ptr<storage::serde> ser) : persistent_service(std::move(ser)) {}

void local_store::write(const storage::locked_hash_table_type &table, const std::string &out_path) {
  size_t found = out_path.find_last_of("/\\");
  auto dir = out_path.substr(0, found);
  directory_utils::create_directory(dir);
  try {
    serde()->serialize(table, out_path);
  }
  catch(std::exception& err) {
    std::cout<<err.what()<<std::endl;
  }
  
  out->close();
}

void local_store::read(const std::string &in_path, storage::locked_hash_table_type &table) {
  try {
    serde()->deserialize(table, in_path);
  }
  catch (...) {
    std::cout << "catch (...)" << std::endl;
  }
  in->close();
}

std::string local_store::URI() {
  return "local";
}

}
}
