#ifndef UTIL_H_
#define UTIL_H_

void cpu_bind(int cpu);
void memory_bind(int cpu_id);
int node_of_cpu(int cpu);
void* alloc_interleaved(size_t sz);
void* alloc(size_t sz);
void* alloc_on_node(size_t sz, int node);
void dealloc(void *p, size_t sz);
uint64_t micro_time(void);
int num_numa_nodes();

// random number generators

typedef struct {
    uint32_t num[624];
    uint64_t index;
} mt_state_t;


mt_state_t *mt_init(uint32_t seed);
uint32_t mt_next(mt_state_t *state);

#endif // UTIL_H_
