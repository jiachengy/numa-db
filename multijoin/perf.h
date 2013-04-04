#ifndef PERF_H_
#define PERF_H_

#include <papi.h>
#include <stdint.h>

#define MAX_PERF_EVENTS 8



#ifndef PER_CORE
#define PER_CORE 1
#endif

#ifndef PER_SYSTEM
#define PER_SYSTEM 0
#endif


#if PER_CORE == 1

#ifndef PERF_PARTITION
#define PERF_PARTITION 1
#endif

#ifndef PERF_JOIN
#define PERF_JOIN 0
#endif

#endif

typedef long long int counter_t;

struct perf_counter_t {
  long tick;
  counter_t value[MAX_PERF_EVENTS];
};

perf_counter_t perf_counter_diff(perf_counter_t before, perf_counter_t after);
void perf_counter_aggr(perf_counter_t *lhs, perf_counter_t rhs);


struct perf_t {
  int EventSet;
};

void perf_register_thread();
void perf_unregister_thread();

void perf_lib_init(const char *perfcfg, const char *perfout);
void perf_lib_cleanup();

perf_t* perf_init();
void perf_destroy(perf_t *perf);
void perf_start(perf_t *perf);
void perf_stop(perf_t *perf);
void perf_print(perf_counter_t counter);
perf_counter_t perf_read(perf_t *perf);

extern perf_counter_t PERF_COUNTER_INITIALIZER;

#endif // PERF_H_
