#ifndef MEMORYMUX_LOGGING_H
#define MEMORYMUX_LOGGING_H

#ifdef __cplusplus
extern "C" {
#endif

#define ALL 0
#define TRACE 1
#define DEBUG 2
#define INFO 3
#define WARN 4
#define ERROR 5
#define FATAL 6
#define OFF 7

int configure_logging(int log_level);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_LOGGING_H
