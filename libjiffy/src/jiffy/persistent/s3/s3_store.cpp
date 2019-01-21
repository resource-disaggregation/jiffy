#include "s3_store.h"
#include "../../utils/logger.h"
#include "../../utils/directory_utils.h"

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

s3_store::s3_store(std::shared_ptr<storage::serde> ser) : persistent_service(std::move(ser)), options_{} {
  options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;
  Aws::InitAPI(options_);
}

s3_store::~s3_store() {
  Aws::ShutdownAPI(options_);
}

void s3_store::write(const storage::locked_hash_table_type &table, const std::string &out_path) {
  auto path_elements = extract_path_elements(out_path);
  auto bucket_name = path_elements.first.c_str();
  auto key = path_elements.second.c_str();

  Aws::Client::ClientConfiguration client_config;
  Aws::S3::S3Client s3_client(client_config);

  Aws::S3::Model::PutObjectRequest object_request;
  object_request.WithBucket(bucket_name).WithKey(key);

  Aws::Utils::Stream::SimpleStreamBuf sbuf;
  auto out = Aws::MakeShared<Aws::IOStream>("StreamBuf", &sbuf);
  serde()->serialize(table, out);
  out->seekg(0, std::ios_base::beg);

  object_request.SetBody(out);
  auto put_object_outcome = s3_client.PutObject(object_request);
  if (put_object_outcome.IsSuccess()) {
    LOG(log_level::info) << "Successfully wrote table to " << out_path;
  } else {
    LOG(log_level::error) << "S3 PutObject error: " << put_object_outcome.GetError().GetExceptionName() << " " <<
                          put_object_outcome.GetError().GetMessage();
    throw std::runtime_error("Error in writing data to S3");
  }
}

void s3_store::read(const std::string &in_path, storage::locked_hash_table_type &table) {
  auto path_elements = extract_path_elements(in_path);
  auto bucket_name = path_elements.first.c_str();
  auto key = path_elements.second.c_str();

  Aws::S3::S3Client s3_client;

  Aws::S3::Model::GetObjectRequest object_request;
  object_request.WithBucket(bucket_name).WithKey(key);

  auto get_object_outcome = s3_client.GetObject(object_request);

  if (get_object_outcome.IsSuccess()) {
    Aws::OFStream local_file;
    auto in = std::make_shared<Aws::IOStream>(get_object_outcome.GetResult().GetBody().rdbuf());
    serde()->deserialize(in, table);
    LOG(log_level::info) << "Successfully read table from " << in_path;
  } else {
    LOG(log_level::error) << "S3 GetObject error: " << get_object_outcome.GetError().GetExceptionName() << " " <<
                          get_object_outcome.GetError().GetMessage();
    throw std::runtime_error("Error in reading data from S3");
  }
}

std::pair<std::string, std::string> s3_store::extract_path_elements(const std::string &s3_path) {
  utils::directory_utils::check_path(s3_path);

  auto bucket_end = std::find(s3_path.begin() + 1, s3_path.end(), '/');
  std::string bucket_name = std::string(s3_path.begin() + 1, bucket_end);
  std::string key = (bucket_end == s3_path.end()) ? std::string() : std::string(bucket_end + 1, s3_path.end());
  return std::make_pair(bucket_name, key);
}

std::string s3_store::URI() {
  return "s3";
}

}
}
