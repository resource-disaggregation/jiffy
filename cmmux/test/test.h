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
  int a_successes;
  int a_failures;
  int tc_successes;
  int tc_failures;
};

struct test_stats t_stats = {.a_successes = 0, .a_failures = 0, .tc_successes = 0, .tc_failures = 0};

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
    int failures_before = t_stats.a_failures;
    node->test();
    int failures_after = t_stats.a_failures;
    if (failures_after > failures_before) {
      t_stats.tc_failures++;
    } else {
      t_stats.tc_successes++;
    }
  }
}

#define RUN_TESTS run_tests()

void print_summary() {
  if (t_stats.a_failures == 0) {
    set_console_yellow();
    printf("===============================================================================\n");
    printf("All tests passed");
    reset_console_color();
    printf(" (%d assertions in %d test cases)\n", t_stats.a_successes, t_stats.tc_successes);
  } else {
    set_console_yellow();
    fprintf(stdout, "===============================================================================\n");
    reset_console_color();
    printf("assertions:\t%d", t_stats.a_successes + t_stats.a_failures);
    set_console_yellow();
    printf(" |\t%d passed", t_stats.a_successes);
    set_console_red();
    printf(" |\t%d failed\n", t_stats.a_failures);
    reset_console_color();
    printf("test cases:\t%d", t_stats.tc_successes + t_stats.tc_failures);
    set_console_yellow();
    printf(" |\t%d passed", t_stats.tc_successes);
    set_console_red();
    printf(" |\t%d failed\n", t_stats.tc_failures);
    reset_console_color();
  }
}

#define PRINT_SUMMARY print_summary()

#define ASSERT_TRUE(condition) if(!condition) {\
  t_stats.a_failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\tCondition %s is false, expected true\n\n", #condition);\
  reset_console_color();\
} else {\
  t_stats.a_successes++;\
}

#define ASSERT_FALSE(condition) if(condition) {\
  t_stats.a_failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\tCondition %s is true, expected false\n\n", #condition);\
  reset_console_color();\
} else {\
  t_stats.a_successes++;\
}

#define ASSERT_EQ(a, b) if(a != b) {\
  t_stats.a_failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\t%s is not equal to %s\n\n", #a, #b);\
  reset_console_color();\
} else {\
  t_stats.a_successes++;\
}

#define ASSERT_STREQ(a, b) if(strcmp(a, b)) {\
  t_stats.a_failures++;\
  set_console_yellow();\
  printf("\n=== Assertion failure at %s:%d (%s) ===\n", __FILE__, __LINE__, __func__);\
  set_console_red();\
  printf("\t%s (%s) is not equal to %s (%s)\n\n", #a, a, #b, b);\
  reset_console_color();\
} else {\
  t_stats.a_successes++;\
}

#endif //MEMORYMUX_TEST_H
