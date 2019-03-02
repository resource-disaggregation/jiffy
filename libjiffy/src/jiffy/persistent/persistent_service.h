#ifndef JIFFY_PERSISTENT_SERVICE_H
#define JIFFY_PERSISTENT_SERVICE_H

#include <string>
#include "jiffy/storage/hashtable/hash_table_defs.h"
#include "jiffy/storage/btree/btree_defs.h"
#include "jiffy/storage/serde/serde_all.h"


namespace jiffy {
namespace persistent {

/* Persistent service virtual class */
class persistent_service {
 public:
  persistent_service(std::shared_ptr<storage::serde> ser) : ser_(std::move(ser)) {}

  virtual ~persistent_service() = default;


  template<typename Datatype>
  void write(const Datatype &table, const std::string &out_path) {
    return virtual_write(table, out_path);
  }

  template<typename Datatype>
  void read(const std::string &in_path, Datatype &table) {
    return virtual_read(in_path, table);
  }

  virtual std::string URI() = 0;

  /**
   * @brief  Fetch custom serializer/deserializer
   * @return Custom serializer/deserializer
   */
  std::shared_ptr<storage::serde> serde() {
    return ser_;
  }

 private:
  /* Custom serializer/deserializer */
  std::shared_ptr<storage::serde> ser_;
  virtual void virtual_write(const storage::locked_hash_table_type &table, const std::string &out_path) = 0;
  virtual void virtual_write(const storage::btree_type &table, const std::string &out_path) = 0;
  virtual void virtual_read(const std::string &in_path, storage::locked_hash_table_type &table) = 0;
  virtual void virtual_read(const std::string &in_path, storage::btree_type &table) = 0;
};

template <class persistent_service_impl>
class derived_persistent : public persistent_service_impl {
 public:
  template<class... TArgs>
  derived_persistent(TArgs &&... args): persistent_service_impl(std::forward<TArgs>(args)...) {

  }
 private:
  void virtual_write(const storage::locked_hash_table_type &table, const std::string &out_path) final {
    return persistent_service_impl::write_impl(table, out_path);
  }
  void virtual_write(const storage::btree_type &table, const std::string &out_path) final {
    return persistent_service_impl::write_impl(table, out_path);
  }
  void virtual_read(const std::string &in_path, storage::locked_hash_table_type &table) final {
    return persistent_service_impl::read_impl(in_path, table);
  }
  void virtual_read(const std::string &in_path, storage::btree_type &table) final {
    return persistent_service_impl::read_impl(in_path, table);
  }
};



}
}

#endif //JIFFY_PERSISTENT_SERVICE_H
