#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sched.h>
#include <numa.h>
#include <math.h>

#include "util.h"
#include "params.h"

#define INGATAN
#ifdef INGATAN
int cores[4][16] = {
  {0,1,2,3,4,5,6,7,32,33,34,35,36,37,38,39},
  {8,9,10,11,12,13,14,15,40,41,42,43,44,45,46,47},
  {16,17,18,19,20,21,22,23,48,49,50,51,52,53,54,55},
  {24,25,26,27,28,29,30,31,56,57,58,59,60,61,62,63},
};
#endif

int cpu_of_thread_rr(int tid)
{
  int node = tid % num_numa_nodes();
  int index = tid / 4;
  return cores[node][index];
}

#ifdef DEBUG
void
logging(const char * format, ...)
{
  va_list args;
  va_start(args, format);
  fprintf(stderr, "[DEBUG]:");
  vfprintf(stderr, format, args);
  va_end(args);
}
#else
void logging(const char * format, ...) {}
#endif


uint32_t cpus(void)
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

uint32_t num_numa_nodes()
{
  return numa_max_node() + 1;
}

void cpu_membind(int cpu_id)
{
  char numa_id_str[12];
  struct bitmask *numa_node;
  int numa_id = numa_node_of_cpu(cpu_id);
  sprintf(numa_id_str, "%d", numa_id);
  numa_node = numa_parse_nodestring(numa_id_str);
  numa_set_membind(numa_node);
  numa_free_nodemask(numa_node);
}

void node_membind(int node)
{
  numa_run_on_node(node);
  struct bitmask *numa_node = numa_get_run_node_mask();
  numa_set_membind(numa_node);
}

void cpu_bind(int cpu)
{
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(cpu, &cpu_set);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
}


void node_bind(int node)
{
  int cpu = cores[node][0];
  cpu_bind(cpu);
}


int get_running_cpu()
{
  return sched_getcpu();
}

int get_running_node()
{
  int cpu = sched_getcpu();
  return node_of_cpu(cpu);
}

int node_of_cpu(int cpu)
{
  return numa_node_of_cpu(cpu);
}

int cpu_of_node(int node, int idx)
{
  return cores[node][idx];
}

void* alloc_aligned(size_t sz, uint64_t align)
{
  void* ptr;
  int retval;
  retval = posix_memalign((void**)&ptr, align, sz);

  assert(retval == 0);

  return ptr;
}


void* alloc(size_t sz)
{
 // return numa_alloc_local(sz);
 
  void* ptr;
  int retval;
  retval = posix_memalign((void**)&ptr, CACHE_LINE_SIZE, sz);

  assert(retval == 0);

  return ptr;
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
  free(p);
   // numa_free(p, sz);
}

uint64_t micro_time(void)
{
  struct timeval t;
  struct timezone z;
  gettimeofday(&t, &z);
  return t.tv_sec * 1000000 + t.tv_usec;
}


rand_state_t *rand_init(uint32_t seed)
{
  rand_state_t *state = (rand_state_t*)malloc(sizeof(rand_state_t));
  uint32_t *n = state->num;
  uint64_t i;
  n[0] = seed;
  for (i = 0 ; i != 623 ; ++i)
    n[i + 1] = 0x6c078965 * (n[i] ^ (n[i] >> 30));
  state->index = 624;
  return state;
}

uint32_t rand_next(rand_state_t *state)
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


int log_2(uint64_t x)
{
	int p = 0;
	while ((((uint64_t) 1) << p) < x) p++;
	return p;
}

int power_of_2(uint64_t x)
{
	return x && (x & (x - 1)) == 0;
}
