#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "../src/mmux/utils/connection_utils.h"
#include "test.h"
#include "../src/mmux/client/cmmux_client.h"
#include "../src/mmux/utils/logging.h"

#define NUM_BLOCKS 3
#define HOST "127.0.0.1"
#define DIRECTORY_SERVICE_PORT 9090
#define DIRECTORY_LEASE_PORT 9091
#define DIRECTORY_BLOCK_PORT 9092

#define STORAGE1_SERVICE_PORT 9093
#define STORAGE1_MANAGEMENT_PORT 9094
#define STORAGE1_NOTIFICATION_PORT 9095
#define STORAGE1_CHAIN_PORT 9096

#define STORAGE2_SERVICE_PORT 9097
#define STORAGE2_MANAGEMENT_PORT 9098
#define STORAGE2_NOTIFICATION_PORT 9099
#define STORAGE2_CHAIN_PORT 9100

#define STORAGE3_SERVICE_PORT 9101
#define STORAGE3_MANAGEMENT_PORT 9102
#define STORAGE3_NOTIFICATION_PORT 9103
#define STORAGE3_CHAIN_PORT 9104

#define LEASE_PERIOD_MS 1000
#define LEASE_PERIOD_US (LEASE_PERIOD_MS * 1000)

struct test_ctx {
  const char *directoryd;
  const char *storaged;
  const char *resources_prefix;
  char storage1_conf[100];
  char storage2_conf[100];
  char storage3_conf[100];
  char storage_auto_conf[100];
  char directory_conf[100];

  int directory_pid;
  int storage1_pid;
  int storage2_pid;
  int storage3_pid;
};

const char *read_env(const char *var, const char *def) {
  const char *val = getenv(var);
  if (val == NULL)
    return def;
  return val;
}

struct test_ctx init_test_ctx() {
  struct test_ctx ctx;
  ctx.directoryd = read_env("DIRECTORY_SERVER_EXEC", "directoryd");
  ctx.storaged = read_env("STORAGE_SERVER_EXEC", "storaged");
  ctx.resources_prefix = read_env("TEST_RESOURCES_PREFIX", ".");
  strcpy(ctx.storage1_conf, ctx.resources_prefix);
  strcat(ctx.storage1_conf, "/storage1.conf");
  strcpy(ctx.storage2_conf, ctx.resources_prefix);
  strcat(ctx.storage2_conf, "/storage2.conf");
  strcpy(ctx.storage3_conf, ctx.resources_prefix);
  strcat(ctx.storage3_conf, "/storage3.conf");
  strcpy(ctx.storage_auto_conf, ctx.resources_prefix);
  strcat(ctx.storage_auto_conf, "/storage_auto_scale.conf");
  strcpy(ctx.directory_conf, ctx.resources_prefix);
  strcat(ctx.directory_conf, "/directory.conf");
  ctx.directory_pid = -1;
  ctx.storage1_pid = -1;
  ctx.storage2_pid = -1;
  ctx.storage3_pid = -1;
  return ctx;
}

void wait_till_server_ready(const char *host, int port) {
  while (!is_server_alive(host, port))
    usleep(100000);
}

int start_process(const char *exec, const char *conf) {
  int pid = fork();
  const char *args[4];
  args[0] = exec;
  args[1] = "--config";
  args[2] = conf;
  args[3] = NULL;
  if (pid < 0) {
    fprintf(stderr, "Error forking\n");
    exit(1);
  } else if (!pid && execvp(exec, (char *const *) args)) {
    fprintf(stderr, "Error running executable %s: %s\n", exec, strerror(errno));
    kill(getppid(), 9);
    exit(1);
  }
  return pid;
}

struct test_ctx start_servers(int chain, int auto_scale) {
  struct test_ctx ctx = init_test_ctx();

  ctx.directory_pid = start_process(ctx.directoryd, ctx.directory_conf);
  wait_till_server_ready(HOST, DIRECTORY_SERVICE_PORT);
  wait_till_server_ready(HOST, DIRECTORY_LEASE_PORT);
  wait_till_server_ready(HOST, DIRECTORY_BLOCK_PORT);

  ctx.storage1_pid = start_process(ctx.storaged, auto_scale ? ctx.storage_auto_conf : ctx.storage1_conf);
  wait_till_server_ready(HOST, STORAGE1_SERVICE_PORT);
  wait_till_server_ready(HOST, STORAGE1_MANAGEMENT_PORT);
  wait_till_server_ready(HOST, STORAGE1_NOTIFICATION_PORT);
  wait_till_server_ready(HOST, STORAGE1_CHAIN_PORT);

  if (chain) {
    ctx.storage2_pid = start_process(ctx.storaged, ctx.storage2_conf);
    wait_till_server_ready(HOST, STORAGE2_SERVICE_PORT);
    wait_till_server_ready(HOST, STORAGE2_MANAGEMENT_PORT);
    wait_till_server_ready(HOST, STORAGE2_NOTIFICATION_PORT);
    wait_till_server_ready(HOST, STORAGE2_CHAIN_PORT);

    ctx.storage3_pid = start_process(ctx.storaged, ctx.storage3_conf);
    wait_till_server_ready(HOST, STORAGE3_SERVICE_PORT);
    wait_till_server_ready(HOST, STORAGE3_MANAGEMENT_PORT);
    wait_till_server_ready(HOST, STORAGE3_NOTIFICATION_PORT);
    wait_till_server_ready(HOST, STORAGE3_CHAIN_PORT);
  }
  return ctx;
}

void stop_servers(struct test_ctx ctx) {
  if (ctx.storage1_pid > 0) {
    kill(ctx.storage1_pid, SIGKILL);
    waitpid(ctx.storage1_pid, NULL, 0);
  }
  if (ctx.storage2_pid > 0) {
    kill(ctx.storage2_pid, SIGKILL);
    waitpid(ctx.storage2_pid, NULL, 0);
  }
  if (ctx.storage3_pid > 0) {
    kill(ctx.storage3_pid, SIGKILL);
    waitpid(ctx.storage3_pid, NULL, 0);
  }
  if (ctx.directory_pid > 0) {
    kill(ctx.directory_pid, SIGKILL);
    waitpid(ctx.directory_pid, NULL, 0);
  }
}

void kv_ops(kv_client *kv) {
  char buf1[100];
  char buf2[100];
  for (size_t i = 0; i < 1000; i++) {
    sprintf(buf1, "%zu", i);
    ASSERT_STREQ("!ok", kv_put(kv, buf1, buf1));
  }

  for (size_t i = 0; i < 1000; i++) {
    sprintf(buf1, "%zu", i);
    ASSERT_STREQ(buf1, kv_get(kv, buf1));
  }

  for (size_t i = 1000; i < 2000; i++) {
    sprintf(buf1, "%zu", i);
    ASSERT_STREQ("!key_not_found", kv_get(kv, buf1));
  }

  for (size_t i = 0; i < 1000; i++) {
    sprintf(buf1, "%zu", i);
    sprintf(buf2, "%zu", i + 1000);
    ASSERT_STREQ(buf1, kv_update(kv, buf1, buf2));
  }

  for (size_t i = 1000; i < 2000; i++) {
    sprintf(buf1, "%zu", i);
    sprintf(buf2, "%zu", i + 1000);
    ASSERT_STREQ("!key_not_found", kv_update(kv, buf1, buf2));
  }

  for (size_t i = 0; i < 1000; i++) {
    sprintf(buf1, "%zu", i);
    sprintf(buf2, "%zu", i + 1000);
    ASSERT_STREQ(buf2, kv_get(kv, buf1));
  }

  for (size_t i = 0; i < 1000; i++) {
    sprintf(buf1, "%zu", i);
    sprintf(buf2, "%zu", i + 1000);
    ASSERT_STREQ(buf2, kv_remove(kv, buf1));
  }

  for (size_t i = 1000; i < 2000; i++) {
    sprintf(buf1, "%zu", i);
    ASSERT_STREQ("!key_not_found", kv_remove(kv, buf1));
  }

  for (size_t i = 0; i < 1000; i++) {
    sprintf(buf1, "%zu", i);
    ASSERT_STREQ("!key_not_found", kv_get(kv, buf1));
  }
}

void test_create() {
  struct test_ctx ctx = start_servers(0, 0);
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  kv_client *kv = mmux_create(client, "/a/file.txt", "local://tmp", 1, 1, 0);
  kv_ops(kv);
  directory_client *fs = mmux_get_fs(client);
  ASSERT_TRUE(fs_exists(fs, "/a/file.txt"));
  destroy_kv(kv);
  destroy_mmux_client(client);
  stop_servers(ctx);
}

void test_open() {
  struct test_ctx ctx = start_servers(0, 0);
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  mmux_create(client, "/a/file.txt", "local://tmp", 1, 1, 0);
  directory_client *fs = mmux_get_fs(client);
  ASSERT_TRUE(fs_exists(fs, "/a/file.txt"));
  kv_client *kv = mmux_open(client, "/a/file.txt");
  kv_ops(kv);
  destroy_kv(kv);
  destroy_mmux_client(client);
  stop_servers(ctx);
}

void test_sync_remove() {
  struct test_ctx ctx = start_servers(0, 0);
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  mmux_create(client, "/a/file.txt", "local://tmp", 1, 1, 0);
  directory_client *fs = mmux_get_fs(client);
  ASSERT_TRUE(fs_exists(fs, "/a/file.txt"));
  mmux_sync(client, "/a/file.txt", "local://tmp");
  ASSERT_TRUE(fs_exists(fs, "/a/file.txt"));
  mmux_remove(client, "/a/file.txt");
  ASSERT_FALSE(fs_exists(fs, "/a/file.txt"));
  destroy_mmux_client(client);
  stop_servers(ctx);
}

void test_close() {
  struct test_ctx ctx = start_servers(0, 0);
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  mmux_create(client, "/a/file1.txt", "local://tmp", 1, 1, 0);
  mmux_create(client, "/a/file2.txt", "local://tmp", 1, 1, PINNED);
  mmux_create(client, "/a/file3.txt", "local://tmp", 1, 1, MAPPED);
  directory_client *fs = mmux_get_fs(client);
  ASSERT_TRUE(fs_exists(fs, "/a/file1.txt"));
  ASSERT_TRUE(fs_exists(fs, "/a/file2.txt"));
  ASSERT_TRUE(fs_exists(fs, "/a/file3.txt"));
  mmux_close(client, "/a/file1.txt");
  mmux_close(client, "/a/file2.txt");
  mmux_close(client, "/a/file3.txt");
  usleep(LEASE_PERIOD_US);
  ASSERT_TRUE(fs_exists(fs, "/a/file1.txt"));
  ASSERT_TRUE(fs_exists(fs, "/a/file2.txt"));
  ASSERT_TRUE(fs_exists(fs, "/a/file3.txt"));
  usleep(LEASE_PERIOD_US * 2);
  ASSERT_FALSE(fs_exists(fs, "/a/file1.txt"));
  ASSERT_TRUE(fs_exists(fs, "/a/file2.txt"));
  ASSERT_TRUE(fs_exists(fs, "/a/file3.txt"));
  destroy_mmux_client(client);
  stop_servers(ctx);
}

void test_auto_scaling() {
  struct test_ctx ctx = start_servers(0, 1);
  char buf[100];
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  kv_client *kv = mmux_create(client, "/a/file.txt", "local://tmp", 1, 1, 0);
  for (size_t i = 0; i < 2000; i++) {
    sprintf(buf, "%zu", i);
    ASSERT_STREQ("!ok", kv_put(kv, buf, buf));
  }
  struct data_status s;
  kv_get_status(kv, &s);
  ASSERT_EQ(4, s.num_blocks);
  for (size_t i = 0; i < 2000; i++) {
    sprintf(buf, "%zu", i);
    ASSERT_STREQ(buf, kv_remove(kv, buf));
  }
  kv_get_status(kv, &s);
  ASSERT_EQ(1, s.num_blocks);
  destroy_kv(kv);
  destroy_mmux_client(client);
  stop_servers(ctx);
}

void test_chain_replication() {
  struct test_ctx ctx = start_servers(1, 0);
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  kv_client *kv = mmux_create(client, "/a/file.txt", "local://tmp", 1, 3, 0);
  struct data_status s;
  kv_get_status(kv, &s);
  ASSERT_EQ(3, s.chain_length);
  kv_ops(kv);
  destroy_kv(kv);
  destroy_mmux_client(client);
  stop_servers(ctx);
}

void test_notifications() {
  struct test_ctx ctx = start_servers(0, 0);
  mmux_client *client = create_mmux_client(HOST, DIRECTORY_SERVICE_PORT, DIRECTORY_LEASE_PORT);
  kv_client *kv = mmux_create(client, "/a/file.txt", "local://tmp", 1, 1, 0);
  kv_listener *n1 = mmux_listen(client, "/a/file.txt");
  kv_listener *n2 = mmux_listen(client, "/a/file.txt");
  kv_listener *n3 = mmux_listen(client, "/a/file.txt");
  const char *ops1[1] = {"put"};
  const char *ops2[2] = {"put", "remove"};
  const char *ops3[1] = {"remove"};
  kv_subscribe(n1, ops1, 1);
  kv_subscribe(n2, ops2, 2);
  kv_subscribe(n3, ops3, 1);

  kv_put(kv, "key1", "value1");
  kv_remove(kv, "key1");

  struct notification_t N1, N2, N3, N4;
  ASSERT_TRUE(kv_get_notification(n1, -1, &N1) == 0);
  ASSERT_TRUE(kv_get_notification(n2, -1, &N2) == 0);
  ASSERT_TRUE(kv_get_notification(n2, -1, &N3) == 0);
  ASSERT_TRUE(kv_get_notification(n3, -1, &N4) == 0);

  ASSERT_STREQ("put", N1.op);
  ASSERT_STREQ("key1", N1.arg);
  ASSERT_STREQ("put", N2.op);
  ASSERT_STREQ("key1", N2.arg);
  ASSERT_STREQ("remove", N3.op);
  ASSERT_STREQ("key1", N3.arg);
  ASSERT_STREQ("remove", N4.op);
  ASSERT_STREQ("key1", N4.arg);

  kv_unsubscribe(n1, ops1, 1);
  kv_unsubscribe(n2, ops3, 1);

  kv_put(kv, "key1", "value1");
  kv_remove(kv, "key1");

  ASSERT_TRUE(kv_get_notification(n2, -1, &N1) == 0);
  ASSERT_TRUE(kv_get_notification(n3, -1, &N2) == 0);

  ASSERT_STREQ("put", N1.op);
  ASSERT_STREQ("key1", N1.arg);
  ASSERT_STREQ("remove", N2.op);
  ASSERT_STREQ("key1", N2.arg);
  destroy_kv(kv);
  destroy_mmux_client(client);
  stop_servers(ctx);
}

int main(int argc, const char **argv) {
  configure_logging(INFO);
  ADD_TEST(test_create);
  ADD_TEST(test_open);
  ADD_TEST(test_sync_remove);
  ADD_TEST(test_close);
  ADD_TEST(test_auto_scaling);
  ADD_TEST(test_chain_replication);
  ADD_TEST(test_notifications);
  RUN_TESTS;
  PRINT_SUMMARY;
  return 0;
}