#ifndef JIFFY_PERSISTENT_SERVICE_H
#define JIFFY_PERSISTENT_SERVICE_H

#include <string>
#include "jiffy/storage/hashtable/hash_table_defs.h"
#include "jiffy/storage/file/file_defs.h"
#include "jiffy/storage/serde/serde_all.h"
#include <aws/core/Aws.h>
#include "../utils/logger.h"
#include "../utils/directory_utils.h"
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/core/utils/stream/SimpleStreamBuf.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <fstream>

namespace jiffy {
namespace persistent {
using namespace utils;
/* Persistent service virtual class */
class persistent_service {
 public:
  /**
   * @brief Constructor
   * @param Custom serializer/deserializer
   */
  persistent_service(std::shared_ptr<storage::serde> ser) : ser_(std::move(ser)) {}

  /**
   * @brief Destructor
   */
  virtual ~persistent_service() = default;

  /**
   * @brief Write to persistent store
   * @param table Data structure
   * @param out_path Persistent store path
   */

  template<typename Datatype>
  void write(const Datatype &table, const std::string &out_path) {
    return virtual_write(table, out_path);
  }

  /**
   * @brief Read from persistent store
   * @param in_path Persistent store path
   * @param table Data structure
   */

  template<typename Datatype>
  void read(const std::string &in_path, Datatype &table) {
    return virtual_read(in_path, table);
  }

  /**
   * @brief Fetch URI
   * @return URI
   */
  virtual std::string URI() = 0;

  /**
   * @brief Fetch custom serializer/deserializer
   * @return Custom serializer/deserializer
   */
  std::shared_ptr<storage::serde> serde() {
    return ser_;
  }

 private:
  /* Custom serializer/deserializer */
  std::shared_ptr<storage::serde> ser_;

  /**
   * @brief Virtual write for fifo queue
   * @param table Fifo queue
   * @param out_path Persistent store path
   */

  virtual void virtual_write(const storage::fifo_queue_type &table, const std::string &out_path) = 0;

  /**
   * @brief Virtual write for file
   * @param table File
   * @param out_path Persistent store path
   */

  virtual void virtual_write(const storage::file_type &table, const std::string &out_path) = 0;

  /**
   * @brief Virtual write for new hash table type
   * @param table Hash table
   * @param out_path Persistent store path
   */

  virtual void virtual_write(const storage::hash_table_type &table, const std::string &out_path) = 0;

  /**
   * @brief Virtual read for fifo queue
   * @param in_path Persistent store path
   * @param table Fifo queue
   */

  virtual void virtual_read(const std::string &in_path, storage::fifo_queue_type &table) = 0;

  /**
   * @brief Virtual read for file
   * @param in_path Persistent store path
   * @param table File
   */

  virtual void virtual_read(const std::string &in_path, storage::file_type &table) = 0;

  /**
   * @brief Virtual read for new hash table type
   * @param in_path Persistent store path
   * @param table Hash table
   */

  virtual void virtual_read(const std::string &in_path, storage::hash_table_type &table) = 0;
};

/**
 * Derviced persistent service class
 */
template<class persistent_service_impl>
class derived_persistent : public persistent_service_impl {
 public:
  /**
   * @brief Constructor
   */

  template<class... TArgs>
  derived_persistent(TArgs &&... args): persistent_service_impl(std::forward<TArgs>(args)...) {
  }
 private:

  /**
   * @brief Virtual write for fifo queue
   * @param table Fifo queue
   * @param out_path Persistent store path
   */


  void virtual_write(const storage::fifo_queue_type &table, const std::string &out_path) final {
    return persistent_service_impl::write_impl(table, out_path);
  }

  /**
   * @brief Virtual write for file
   * @param table File
   * @param out_path Persistent store path
   */

  void virtual_write(const storage::file_type &table, const std::string &out_path) final {
    return persistent_service_impl::write_impl(table, out_path);
  }

  /**
   * @brief Virtual write for new hash table type
   * @param table Hash table
   * @param out_path Persistent store path
   */

  void virtual_write(const storage::hash_table_type &table, const std::string &out_path) final {
    return persistent_service_impl::write_impl(table, out_path);
  }

  /**
   * @brief Virtual read for fifo queue
   * @param in_path Persistent store path
   * @param table Fifo queue
   */

  void virtual_read(const std::string &in_path, storage::fifo_queue_type &table) final {
    return persistent_service_impl::read_impl(in_path, table);
  }

  /**
   * @brief Virtual read for file
   * @param in_path Persistent store path
   * @param table File
   */

  void virtual_read(const std::string &in_path, storage::file_type &table) final {
    return persistent_service_impl::read_impl(in_path, table);
  }

  /**
   * @brief Virtual read for hash table
   * @param in_path Persistent store path
   * @param table Hash table
   */

  void virtual_read(const std::string &in_path, storage::hash_table_type &table) final {
    return persistent_service_impl::read_impl(in_path, table);
  }

};

/* Local store, inherited persistent_service */
class local_store_impl : public persistent_service {
 protected:
  /**
   * @brief Constructor
   * @param ser Custom serializer/deserializer
   */

  local_store_impl(std::shared_ptr<storage::serde> ser);

  /**
   * @brief Write data from hash table to persistent storage
   * @param table Hash table
   * @param out_path Output persistent storage path
   */
  template<typename Datatype>
  void write_impl(const Datatype &table, const std::string &out_path) {
    size_t found = out_path.find_last_of("/\\");
    auto dir = out_path.substr(0, found);
    directory_utils::create_directory(dir);
    std::shared_ptr<std::ofstream> out(new std::ofstream(out_path));
    serde()->serialize<Datatype>(table, out);
    out->close();
  }

  /**
   * @brief Read data from persistent storage to hash table
   * @param in_path Input persistent storage path
   * @param table Hash table
   */
  template<typename Datatype>
  void read_impl(const std::string &in_path, Datatype &table) {
    auto in = std::make_shared<std::ifstream>(in_path.c_str(), std::fstream::in);
    serde()->deserialize<Datatype>(in, table);
    in->close();
  }

 public:
  /**
   * @brief Fetch URI
   * @return URI string
   */

  std::string URI() override;
};

using local_store = derived_persistent<local_store_impl>;

/* s3_store class, inherited from persistent_service class */
class s3_store_impl : public persistent_service {
 public:

  /**
   * @brief Destructor
   */

  ~s3_store_impl();
 protected:

  /**
   * @brief Constructor
   * @param ser Custom serializer/deserializer
   */

  s3_store_impl(std::shared_ptr<storage::serde> ser);

  /**
   * @brief Write data from hash table to persistent storage
   * @param table Hash table
   * @param out_path Output persistent storage path
   */
  
  template<typename Datatype>
  void write_impl(const Datatype &table, const std::string &out_path) {
    auto path_elements = extract_path_elements(out_path);
    auto bucket_name = path_elements.first.c_str();
    auto key = path_elements.second.c_str();

    // Aws::Client::ClientConfiguration client_config;
    // client_config.region = "us-east-2";
    // Aws::S3::S3Client s3_client(client_config);

    Aws::S3::Model::PutObjectRequest object_request;
    LOG(log_level::info) << "Bucket: " << bucket_name;
    object_request.WithBucket(bucket_name).WithKey(key);

    Aws::Utils::Stream::SimpleStreamBuf sbuf;
    auto out = Aws::MakeShared<Aws::IOStream>("StreamBuf", &sbuf);
    serde()->serialize<Datatype>(table, out);
    out->seekg(0, std::ios_base::beg);

    object_request.SetBody(out);
    auto put_object_outcome = s3_client_->PutObject(object_request);
    if (put_object_outcome.IsSuccess()) {
      LOG(log_level::info) << "Successfully wrote table to " << out_path;
    } else {
      LOG(log_level::error) << "S3 PutObject error: " << put_object_outcome.GetError().GetExceptionName() << " " <<
                            put_object_outcome.GetError().GetMessage();
      throw std::runtime_error("Error in writing data to S3");
    }
  }

  /**
   * @brief Read data from persistent storage to hash table
   * @param in_path Input persistent storage path
   * @param table Hash table
   */

  template<typename Datatype>
  void read_impl(const std::string &in_path, Datatype &table) {
    auto path_elements = extract_path_elements(in_path);
    auto bucket_name = path_elements.first.c_str();
    auto key = path_elements.second.c_str();

    // Aws::S3::S3Client s3_client;

    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(key);

    auto get_object_outcome = s3_client_->GetObject(object_request);

    if (get_object_outcome.IsSuccess()) {
      Aws::OFStream local_file;
      auto in = std::make_shared<Aws::IOStream>(get_object_outcome.GetResult().GetBody().rdbuf());
      serde()->deserialize<Datatype>(in, table);
      LOG(log_level::info) << "Successfully read table from " << in_path;
    } else {
      LOG(log_level::error) << "S3 GetObject error: " << get_object_outcome.GetError().GetExceptionName() << " " <<
                            get_object_outcome.GetError().GetMessage();
      throw std::runtime_error("Error in reading data from S3");
    }
  }
 public:
  /**
   * @brief Fetch URI
   * @return URI string
   */

  std::string URI() override;
 private:
  /**
   * @brief Extract path element
   * @param s3_path s3 path
   * @return Pair of bucket name and key
   */

  std::pair<std::string, std::string> extract_path_elements(const std::string &s3_path);
  /* AWS SDK options */
  Aws::SDKOptions options_;
  std::shared_ptr<Aws::S3::S3Client> s3_client_;

};

using s3_store = derived_persistent<s3_store_impl>;

}
}

#endif //JIFFY_PERSISTENT_SERVICE_H
