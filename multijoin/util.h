#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>

void cpu_bind(int cpu);
void memory_bind(int cpu_id);
int node_of_cpu(int cpu);
void* alloc_interleaved(size_t sz);
void* alloc(size_t sz);
void dealloc(void *p, size_t sz);
uint64_t micro_time(void);

#endif // UTIL_H_
