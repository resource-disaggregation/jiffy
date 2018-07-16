#include <mmux/storage/client/kv_client.h>
#include "kv_client_wrapper.h"

data_status __from_cxx(const mmux::directory::data_status &s) {
  data_status ret;
  ret.chain_length = s.chain_length();
  ret.num_blocks = s.data_blocks().size();
  ret.backing_path = strdup(s.backing_path().c_str());
  ret.flags = s.flags();
  ret.data_blocks = (replica_chain *) malloc(sizeof(replica_chain) * ret.num_blocks);
  for (size_t i = 0; i < ret.num_blocks; i++) {
    ret.data_blocks[i].storage_mode = (int32_t) s.data_blocks().at(i).mode;
    ret.data_blocks[i].slot_begin = s.data_blocks().at(i).slot_begin();
    ret.data_blocks[i].slot_end = s.data_blocks().at(i).slot_end();
    ret.data_blocks[i].chain_length = s.data_blocks().at(i).block_names.size();
    ret.data_blocks[i].block_names = (char **) malloc(sizeof(char *) * ret.data_blocks[i].chain_length);
    for (size_t j = 0; j < ret.data_blocks[i].chain_length; j++) {
      ret.data_blocks[i].block_names[j] = strdup(s.data_blocks().at(i).block_names.at(j).c_str());
    }
  }
  return ret;
}

void destroy_kv(kv_client *client) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  delete c;
}

void kv_refresh(kv_client *client) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  c->refresh();
}

data_status kv_get_status(kv_client *client) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  return __from_cxx(c->status());
}

char *kv_put(kv_client *client, const char *key, const char *value) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  return strdup(c->put(std::string(key), std::string(value)).c_str());
}

char *kv_get(kv_client *client, const char *key) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  return strdup(c->get(std::string(key)).c_str());
}

char *kv_update(kv_client *client, const char *key, const char *value) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  return strdup(c->update(std::string(key), std::string(value)).c_str());
}

char *kv_remove(kv_client *client, const char *key) {
  mmux::storage::kv_client *c = static_cast<mmux::storage::kv_client *>(client);
  return strdup(c->remove(std::string(key)).c_str());
}
