#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sched.h>
#include <numa.h>
#include <math.h>

#include "util.h"

int cpus(void)
{
	char cpu_name[40];
	struct stat st;
	strcpy(cpu_name, "/sys/devices/system/cpu/cpu");
	char *cpu_num_ptr = &cpu_name[strlen(cpu_name)];
	int cpu_num = -1;
	do {
		sprintf(cpu_num_ptr, "%d", ++cpu_num);
	} while (stat(cpu_name, &st) == 0);
	return cpu_num;
}

void cpu_bind(int cpu)
{
	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	CPU_SET(cpu, &cpu_set);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
}

int node_of_cpu(int cpu)
{
	return numa_node_of_cpu(cpu);
}

void* alloc(size_t sz)
{
	return numa_alloc_local(sz);
}

void dealloc(void *p, size_t sz)
{
	numa_free(p, sz);
}

uint64_t micro_time(void)
{
	struct timeval t;
	struct timezone z;
	gettimeofday(&t, &z);
	return t.tv_sec * 1000000 + t.tv_usec;
}
