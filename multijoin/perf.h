#ifndef PERF_H_
#define PERF_H_

#include <papi.h>
#include <stdint.h>

#ifndef USE_PERF
#define USE_PERF
#endif


#ifndef PER_CORE
#define PER_CORE 1
#endif

#ifndef PER_SOCKET
#define PER_SOCKET 0
#endif

#ifndef PER_SYSTEM
#define PER_SYSTEM 0
#endif


#if PER_CORE == 1

#ifndef PERF_PARTITION
#define PERF_PARTITION 0
#endif

#ifndef PERF_JOIN
#define PERF_JOIN 0
#endif

#ifndef PERF_ALL
#define PERF_ALL 1
#endif

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
void perf_reset(perf_t *perf);
void perf_stop(perf_t *perf); /* stop the counter, but do not write the counter values */
void perf_print(perf_t *perf);
void perf_aggregate(perf_t *total, perf_t *perf);
void perf_read(perf_t *perf);
void perf_accum(perf_t *perf);


#endif // PERF_H_
