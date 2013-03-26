#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>
#include <numa.h>

#ifndef NEXT_POW_2
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif


uint32_t cpus();
uint32_t num_numa_nodes();


void cpu_bind(int cpu);
void node_bind(int node);
void cpu_membind(int cpu);
void node_membind(int node);

int get_running_cpu();
int get_running_node();

int cpu_of_thread_rr(int tid);
int node_of_cpu(int cpu);

void* alloc_interleaved(size_t sz);
void* alloc(size_t sz);
void dealloc(void *p, size_t sz);
uint64_t micro_time(void);

// random number generators

typedef struct {
    uint32_t num[624];
    uint64_t index;
} mt_state_t;


mt_state_t *mt_init(uint32_t seed);
uint32_t mt_next(mt_state_t *state);


#endif // UTIL_H_
