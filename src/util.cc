#include <assert.h>
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

int num_numa_nodes(void)
{
  return numa_num_configured_nodes();
}

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

void memory_bind(int cpu_id)
{
  char numa_id_str[12];
  struct bitmask *numa_node;
  int numa_id = numa_node_of_cpu(cpu_id);
  sprintf(numa_id_str, "%d", numa_id);
  numa_node = numa_parse_nodestring(numa_id_str);
  numa_set_membind(numa_node);
  numa_free_nodemask(numa_node);
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

// better not to use this feature
void* alloc_on_node(size_t sz, int node)
{
  return numa_alloc_onnode(sz, node);
}


// be careful when used in global thread
void* alloc_interleaved(size_t sz)
{
  bitmask *prev = numa_get_membind(); // save prev mask
  numa_set_membind(numa_all_nodes_ptr); // rebind to all nodes
  void *data = numa_alloc_interleaved(sz);
  // restore prev mask
  numa_set_membind(prev);
  return data;
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

mt_state_t *mt_init(uint32_t seed)
{
  uint64_t i;
  mt_state_t *state = (mt_state_t*)malloc(sizeof(mt_state_t));
  assert(state != NULL);
  uint32_t *n = state->num;
  n[0] = seed;
  for (i = 0 ; i != 623 ; ++i)
    n[i + 1] = 0x6c078965 * (n[i] ^ (n[i] >> 30));
  state->index = 624;
  return state;
}

uint32_t mt_next(mt_state_t *state)
{
  uint32_t y, *n = state->num;
  if (state->index == 624) {
    uint64_t i = 0;
    do {
      y = n[i] & 0x80000000;
      y += n[i + 1] & 0x7fffffff;
      n[i] = n[i + 397] ^ (y >> 1);
      n[i] ^= 0x9908b0df & -(y & 1);
    } while (++i != 227);
    n[624] = n[0];
    do {
      y = n[i] & 0x80000000;
      y += n[i + 1] & 0x7fffffff;
      n[i] = n[i - 227] ^ (y >> 1);
      n[i] ^= 0x9908b0df & -(y & 1);
    } while (++i != 624);
    state->index = 0;
  }
  y = n[state->index++];
  y ^= (y >> 11);
  y ^= (y << 7) & 0x9d2c5680;
  y ^= (y << 15) & 0xefc60000;
  y ^= (y >> 18);
  return y;
}
