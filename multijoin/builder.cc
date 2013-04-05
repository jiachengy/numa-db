#include <stdio.h>
#include <assert.h>
#include <pthread.h>

#include "params.h"
#include "builder.h"
#include "util.h"

#define RAND_RANGE(N, STATE) (rand_next(STATE) % N)

void
knuth_shuffle(tuple_t *tuples, size_t ntuples, uint32_t seed)
{
  rand_state *state = rand_init(seed);
  for (int i = ntuples - 1; i > 0; i--) {
    int32_t  j = RAND_RANGE(i,state);
    tuple_t tmp = tuples[i];
    tuples[i] = tuples[j];
    tuples[j] = tmp;
  }
  free(state);
}

void
knuth_shuffle_rel(relation_t *rel, uint32_t seed)
{
  size_t ntuples_per_node = rel->ntuples / rel->nnodes;

  rand_state *state = rand_init(seed);
  for (int i = rel->ntuples - 1; i > 0; i--) {
    int32_t j = RAND_RANGE(i, state);

    int node_j = j / ntuples_per_node;
    int offset_j = j - ntuples_per_node * node_j;

    int node_i = i / ntuples_per_node;
    int offset_i = i - ntuples_per_node * node_i;

    tuple_t tmp = rel->tuples[node_i][offset_i];
    rel->tuples[node_i][offset_i] = rel->tuples[node_j][offset_j];
    rel->tuples[node_j][offset_j] = tmp;
  }
  free(state);
}


relation_t*
relation_init(uint32_t nnodes)
{
  relation_t *rel = (relation_t*)malloc(sizeof(relation_t));
  rel->nnodes = nnodes;
  rel->tuples = (tuple_t**)malloc(sizeof(tuple_t*) * nnodes);
  rel->ntuples_on_node = (size_t*)malloc(sizeof(size_t*) * nnodes);
  memset(rel->tuples, 0x0, sizeof(tuple_t*) * nnodes);
  memset(rel->ntuples_on_node, 0x0, sizeof(size_t*) * nnodes);
  return rel;
}

void
relation_destroy(relation_t *rel)
{
  for (uint32_t node = 0; node < rel->nnodes; ++node) {
    if (rel->tuples[node] != NULL)
      dealloc(rel->tuples[node], sizeof(tuple_t) * rel->ntuples_on_node[node]);
  }
  free(rel->tuples);
  free(rel->ntuples_on_node);
  free(rel);
}

void
random_gen(tuple_t *tuple, size_t ntuples, const int32_t maxid)
{
  uint32_t seed = rand();
  rand_state *state = rand_init(seed);

  for (uint32_t i = 0; i < ntuples; i++) {
    tuple[i].key = RAND_RANGE(maxid, state);
    tuple[i].payload = tuple[i].key;
  }

  free(state);
}

void
random_unique_gen(tuple_t *tuple, size_t ntuples)
{

  for (uint32_t i = 0; i < ntuples; i++) {
    tuple[i].key = i + 1;
    tuple[i].payload = tuple[i].key;
  }

  uint32_t seed = rand();
  knuth_shuffle(tuple, ntuples, seed);
}


void*
build_fk_thread(void *params)
{
  build_arg_t *arg = (build_arg_t*)params;
  int cpu = arg->cpu;
  int node = arg->node;
  relation_t *rel = arg->rel;

  cpu_bind(cpu);
  cpu_membind(cpu);

  if (cpu == cpu_of_node(node, 0)) {
    size_t ntuples_on_node = arg->rel->ntuples_on_node[node];
    tuple_t *tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples_on_node);
    memset(tuples, 0x0, sizeof(tuple_t) * ntuples_on_node);
    rel->tuples[node] = tuples;
  }

  pthread_barrier_wait(arg->barrier_alloc);

  tuple_t *tuples = rel->tuples[node] + arg->offset;
  assert(tuples != NULL);

  random_gen(tuples, arg->ntuples, arg->maxid);

  /*
  
  int iters = arg->ntuples / arg->maxid;

  for (int iter = 0; iter < iters; ++iter) {
    random_unique_gen(tuples, arg->maxid);
    tuples += arg->maxid * iter;
  }
  
  int remainder = arg->ntuples % arg->maxid;
  if (remainder > 0) {
    random_unique_gen(tuples, remainder);
  }
  */

  return NULL;
}


void*
build_pk_thread(void *params)
{
  build_arg_t *arg = (build_arg_t*)params;
  int cpu = arg->cpu;
  int node = arg->node;
  relation_t *rel = arg->rel;

  cpu_bind(cpu);
  cpu_membind(cpu);

  if (cpu == cpu_of_node(node, 0)) {
    size_t ntuples_on_node = arg->rel->ntuples_on_node[node];
    tuple_t *tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples_on_node);
    memset(tuples, 0x0, sizeof(tuple_t) * ntuples_on_node);
    rel->tuples[node] = tuples;
  }

  pthread_barrier_wait(arg->barrier_alloc);

  tuple_t *tuples = rel->tuples[node] + arg->offset;
  assert(tuples != NULL);

  int firstkey = arg->firstkey;
  for (uint32_t i = 0; i < arg->ntuples; i++) {
    tuples->key = firstkey++;
    tuples->payload = tuples->key;
    tuples++;
  }

  uint32_t state = rand();
  tuples = rel->tuples[node] + arg->offset;
  knuth_shuffle(tuples, arg->ntuples, state);

  return NULL;
}

relation_t *
parallel_build_relation_fk(const size_t ntuples, const int32_t maxid, const uint32_t nnodes, const uint32_t nthreads)
{
  assert(nnodes <= num_numa_nodes());

  relation_t *rel = relation_init(nnodes);

  assert(nthreads >= nnodes);

  rel->ntuples = ntuples;
  rel->nnodes = nnodes;

  build_arg_t args[nthreads];
  pthread_t threads[nthreads];
  pthread_barrier_t barriers[nnodes];

  size_t ntuples_per_node = ntuples / nnodes;
  size_t ntuples_on_lastnode = ntuples - ntuples_per_node * (nnodes - 1);
  size_t nthreads_per_node = nthreads / nnodes;
  size_t nthreads_on_lastnode = nthreads - nthreads_per_node * (nnodes - 1);

  int tid = 0;
  for (uint32_t node = 0; node < nnodes; ++node) {
    rel->ntuples_on_node[node] = (node == nnodes-1) ? ntuples_on_lastnode : ntuples_per_node;
    size_t nthreads_on_node = (node == nnodes - 1) ? nthreads_on_lastnode : nthreads_per_node;
    size_t ntuples_per_thread = rel->ntuples_on_node[node] / nthreads_on_node;
    size_t ntuples_lastthread = rel->ntuples_on_node[node] - ntuples_per_thread * (nthreads_on_node - 1);

    pthread_barrier_init(&barriers[node], NULL, nthreads_on_node);

    int offset = 0;
    for (uint32_t t = 0; t < nthreads_on_node; t++, tid++) {
      args[tid].tid = tid;
      args[tid].cpu = cpu_of_node(node, t);
      args[tid].node = node;
      args[tid].offset = offset;
      args[tid].maxid = maxid;
      args[tid].ntuples = (t == nthreads_on_node - 1) ? ntuples_lastthread : ntuples_per_thread;
      
      offset += (args[tid].ntuples);

      args[tid].barrier_alloc = &barriers[node];
      args[tid].rel = rel;
    }
  }

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_create(&threads[i], NULL, build_fk_thread, (void*)&args[i]);

  for (uint32_t i = 0; i < nthreads; i++) {
    pthread_join(threads[i], NULL);
  }

  for (uint32_t node = 0; node < nnodes; ++node)
    pthread_barrier_destroy(&barriers[node]);

  return rel;
}


relation_t *
parallel_build_relation_pk(size_t ntuples, uint32_t nnodes, uint32_t nthreads)
{
  assert(nnodes <= num_numa_nodes());

  relation_t *rel = relation_init(nnodes);

  assert(nthreads >= nnodes);

  rel->ntuples = ntuples;

  build_arg_t args[nthreads];
  pthread_t threads[nthreads];
  pthread_barrier_t barriers[nnodes];

  size_t ntuples_per_node = ntuples / nnodes;
  size_t ntuples_on_lastnode = ntuples - ntuples_per_node * (nnodes - 1);
  size_t nthreads_per_node = nthreads / nnodes;
  size_t nthreads_on_lastnode = nthreads - nthreads_per_node * (nnodes - 1);

  int tid = 0;
  int firstkey = 1; // we start from 1
  for (uint32_t node = 0; node < nnodes; ++node) {
    rel->ntuples_on_node[node] = (node == nnodes-1) ? ntuples_on_lastnode : ntuples_per_node;
    size_t nthreads_on_node = (node == nnodes - 1) ? nthreads_on_lastnode : nthreads_per_node;
    size_t ntuples_per_thread = rel->ntuples_on_node[node] / nthreads_on_node;
    size_t ntuples_lastthread = rel->ntuples_on_node[node] - ntuples_per_thread * (nthreads_on_node - 1);

    pthread_barrier_init(&barriers[node], NULL, nthreads_on_node);

    int offset = 0;
    for (uint32_t t = 0; t < nthreads_on_node; t++, tid++) {
      args[tid].tid = tid;
      args[tid].cpu = cpu_of_node(node, t);
      args[tid].node = node;
      args[tid].offset = offset;
      args[tid].firstkey = firstkey;
      args[tid].ntuples = (t == nthreads_on_node - 1) ? ntuples_lastthread : ntuples_per_thread;
      
      offset += (args[tid].ntuples);
      firstkey += (args[tid].ntuples);

      args[tid].barrier_alloc = &barriers[node];
      args[tid].rel = rel;
    }
  }

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_create(&threads[i], NULL, build_pk_thread, (void*)&args[i]);

  for (uint32_t i = 0; i < nthreads; i++) {
    pthread_join(threads[i], NULL);
  }

  for (uint32_t node = 0; node < nnodes; ++node)
    pthread_barrier_destroy(&barriers[node]);

  uint32_t seed = rand();
  knuth_shuffle_rel(rel, seed);

  return rel;
}

relation_t *
build_relation_pk(size_t ntuples)
{
  relation_t *rel = relation_init(1);
  rel->ntuples = ntuples;

  node_bind(0);
  node_membind(0);

  tuple_t * tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples);
  memset(tuples, 0, sizeof(tuple_t) * ntuples);
  
  random_unique_gen(tuples, ntuples);

  rel->tuples[0] = tuples;
  rel->ntuples_on_node[0] = ntuples;
  return rel;
}


relation_t *
build_relation_pk_onnode(size_t ntuples, uint32_t node)
{
  assert(node < num_numa_nodes());

  node_bind(node);

  relation_t *rel = relation_init(num_numa_nodes());
  rel->ntuples = ntuples;

  tuple_t * tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples);
  memset(tuples, 0, sizeof(tuple_t) * ntuples);
  
  random_unique_gen(tuples, ntuples);

  for (uint32_t i = 0; i != rel->nnodes; ++i) {
    rel->tuples[i] = NULL;
    rel->ntuples_on_node[i] = 0;
  }

  rel->tuples[node] = tuples;
  rel->ntuples_on_node[node] = ntuples;

  return rel;
}

