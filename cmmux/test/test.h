#ifndef MEMORYMUX_TEST_H
#define MEMORYMUX_TEST_H

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

typedef void (*test_function_t)();

struct test_function_node {
  test_function_t test;
  const char *test_name;
  struct test_function_node *next;
};

struct test_function_node *head = NULL;

void add_test(test_function_t fn, const char *fn_name) {
  if (head == NULL) {
    head = (struct test_function_node *) malloc(sizeof(struct test_function_node));
    head->test = fn;
    head->test_name = strdup(fn_name);
    head->next = NULL;
  } else {
    struct test_function_node *node = (struct test_function_node *) malloc(sizeof(struct test_function_node));
    node->test = fn;
    node->test_name = strdup(fn_name);
    node->next = head;
    head = node;
  }
}

#define ADD_TEST(f) add_test(&f, #f)

struct test_stats {
  int successes;
  int failures;
  int tc_successes;
  int tc_failures;
};

struct test_stats stats = {.successes = 0, .failures = 0, .tc_successes = 0, .tc_failures = 0};

void set_console_red() {
  printf("\033[1;31m");
}

void set_console_yellow() {
  printf("\033[1;33m");
}

void reset_console_color() {
  printf("\033[0m");
}

void run_tests() {
  for (struct test_function_node *node = head; node != NULL; node = node->next) {
    int failures_before = stats.failures;
    node->test();
    int failures_after = stats.failures;
    if (failures_after > failures_before) {
      stats.tc_failures++;
    } else {
      stats.tc_successes++;
    }
  }
}

#define RUN_TESTS run_tests()

void print_summary() {
  if (stats.failures == 0) {
    set_console_yellow();
    printf("===============================================================================\n");
    printf("All tests passed");
    reset_console_color();
    printf(" (%d assertions)\n", stats.successes);
  } else {
    set_console_yellow();
    fprintf(stdout, "===============================================================================\n");
    reset_console_color();
    printf("assertions:\t%d", stats.successes + stats.failures);
    set_console_yellow();
    printf(" |\t%d passed", stats.successes);
    set_console_red();
    printf(" |\t%d failed\n", stats.failures);
    reset_console_color();
  }
}

#define PRINT_SUMMARY print_summary()

#define ASSERT_TRUE(condition) if(!condition) {\
  stats.failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\tCondition %s is false, expected true\n\n", #condition);\
  reset_console_color();\
} else {\
  stats.successes++;\
}

#define ASSERT_FALSE(condition) if(condition) {\
  stats.failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\tCondition %s is true, expected false\n\n", #condition);\
  reset_console_color();\
} else {\
  stats.successes++;\
}

#define ASSERT_EQ(a, b) if(a != b) {\
  stats.failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\t%s is not equal to %s\n\n", #a, #b);\
  reset_console_color();\
} else {\
  stats.successes++;\
}

#define ASSERT_STREQ(a, b) if(strcmp(a, b)) {\
  stats.failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\t%s (%s) is not equal to %s (%s)\n\n", #a, a, #b, b);\
  reset_console_color();\
} else {\
  stats.successes++;\
}

#endif //MEMORYMUX_TEST_H
