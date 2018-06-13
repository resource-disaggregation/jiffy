#include "s3_store.h"
#include "../../utils/logger.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/core/utils/stream/SimpleStreamBuf.h>
#include <fstream>

namespace mmux {
namespace persistent {

using namespace utils;

s3_store::s3_store(std::shared_ptr<storage::serde> ser) : persistent_service(std::move(ser)), options_{} {
  Aws::InitAPI(options_);
}

s3_store::~s3_store() {
  Aws::ShutdownAPI(options_);
}

void s3_store::write(const storage::locked_hash_table_type &table, const std::string &out_path) {
  auto path_components = extract_s3_path_elements(out_path);
  auto bucket_name = path_components.first.c_str();
  auto key = path_components.second.c_str();

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
  auto path_components = extract_s3_path_elements(in_path);
  auto bucket_name = path_components.first.c_str();
  auto key = path_components.second.c_str();

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

std::pair<std::string, std::string> s3_store::extract_s3_path_elements(const std::string &s3_path) {
  std::string pfix = "s3://";
  auto it1 = std::search(s3_path.begin(), s3_path.end(), std::begin(pfix), std::end(pfix), [](char c1, char c2) {
    return std::tolower(c1) == std::tolower(c2);
  });
  if (it1 == s3_path.end()) {
    throw std::invalid_argument("Path is not a valid S3 URI: " + s3_path);
  }
  it1 += pfix.length();
  auto it2 = std::find(it1, s3_path.end(), '/');
  std::string bucket_name = std::string(it1, it2);
  std::string key_prefix = (it2 == s3_path.end()) ? std::string() : std::string(it2 + 1, s3_path.end());
  return std::make_pair(bucket_name, key_prefix);
}

std::string s3_store::URI() {
  return "s3";
}

}
}