#ifndef PERF_H_
#define PERF_H_

#include <papi.h>
#include <stdint.h>

#ifndef USE_PERF
#define USE_PERF
#endif

typedef long long int counter_t;

struct perf_t {
  int EventSet;
  counter_t *values;
};

void perf_register_thread();
void perf_unregister_thread();

void perf_lib_init(const char *perfcfg, const char *perfout);
void perf_lib_cleanup();

perf_t* perf_init();
void perf_destroy(perf_t *perf);
void perf_start(perf_t *perf);
void perf_stop(perf_t *perf);
void perf_print(perf_t *perf);
void perf_aggregate(perf_t *total, perf_t *perf);



#endif // PERF_H_
