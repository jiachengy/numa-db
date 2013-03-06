#ifndef UTIL_H_
#define UTIL_H_

void cpu_bind(int cpu);
int node_of_cpu(int cpu);
void* alloc(size_t sz);
void dealloc(void *p, size_t sz);
uint64_t micro_time(void);

#endif // UTIL_H_
