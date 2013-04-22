#include <stdio.h>
#include <assert.h>
#include <pthread.h>

#include "params.h"
#include "builder.h"
#include "util.h"

#define RAND_RANGE(N, STATE) (rand_next(STATE) % N)
#define RAND_RANGE2(MIN, MAX, STATE) ((rand_next(STATE) + MIN) % MAX)


#ifdef COLUMN_WISE
void knuth_shuffle(intkey_t * key, value_t * value, size_t ntuples)
{
	rand_state *state = rand_init(rand());
  for (int i = ntuples - 1; i != 0; i--) {
    int32_t  j = RAND_RANGE(i,state);
    intkey_t tmpkey = key[i];
	value_t tmpval = value[i];
    key[i] = key[j];
	value[i] = value[j];
    key[j] = tmpkey;
    value[j] = tmpval;
  }
  free(state);
}
#else
void knuth_shuffle(tuple_t *tuples,  size_t ntuples)
{
  rand_state *state = rand_init(rand());
  for (int i = ntuples - 1; i != 0; i--) {
    int32_t  j = RAND_RANGE(i,state);
    tuple_t tmp = tuples[i];
    tuples[i] = tuples[j];
    tuples[j] = tmp;
  }
  free(state);
}
#endif

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

#ifdef COLUMN_WISE
    intkey_t tmpkey = rel->key[node_i][offset_i];
    value_t tmpval = rel->value[node_i][offset_i];
    rel->key[node_i][offset_i] = rel->key[node_j][offset_j];
    rel->value[node_i][offset_i] = rel->value[node_j][offset_j];
    rel->key[node_j][offset_j] = tmpkey;
    rel->value[node_j][offset_j] = tmpval;
#else
    tuple_t tmp = rel->tuples[node_i][offset_i];
    rel->tuples[node_i][offset_i] = rel->tuples[node_j][offset_j];
    rel->tuples[node_j][offset_j] = tmp;
#endif
  }
  free(state);
}


relation_t*
relation_init(uint32_t nnodes)
{
  relation_t *rel = (relation_t*)malloc(sizeof(relation_t));
  rel->nnodes = nnodes;
#ifdef COLUMN_WISE
  rel->key = (intkey_t**)malloc(sizeof(intkey_t*) * nnodes);
  rel->value = (value_t**)malloc(sizeof(value_t*) * nnodes);
  memset(rel->key, 0x0, sizeof(intkey_t*) * nnodes);
  memset(rel->value, 0x0, sizeof(value_t*) * nnodes);
#else
  rel->tuples = (tuple_t**)malloc(sizeof(tuple_t*) * nnodes);
  memset(rel->tuples, 0x0, sizeof(tuple_t*) * nnodes);
#endif
  rel->ntuples_on_node = (size_t*)malloc(sizeof(size_t*) * nnodes);
  memset(rel->ntuples_on_node, 0x0, sizeof(size_t*) * nnodes);
  return rel;
}

void
relation_destroy(relation_t *rel)
{
  for (uint32_t node = 0; node < rel->nnodes; ++node) {
#ifdef COLUMN_WISE
    if (rel->key[node] != NULL)
      dealloc(rel->key[node], sizeof(intkey_t) * rel->ntuples_on_node[node]);
    if (rel->value[node] != NULL)
      dealloc(rel->value[node], sizeof(value_t) * rel->ntuples_on_node[node]);
#else
    if (rel->tuples[node] != NULL)
      dealloc(rel->tuples[node], sizeof(tuple_t) * rel->ntuples_on_node[node]);
#endif
  }
#ifdef COLUMN_WISE
  free(rel->key);
  free(rel->value);
#else
  free(rel->tuples);
#endif
  free(rel->ntuples_on_node);
  free(rel);
}

#ifdef COLUMN_WISE
void
random_gen(intkey_t *key, value_t *value, size_t tuples, const int32_t minid, const int32_t maxid)
{
  uint32_t seed = rand();
  rand_state *state = rand_init(seed);

  for (uint32_t i = 0; i != tuples; ++i) {
    key[i] = RAND_RANGE2(minid, maxid, state);
    value[i] = key[i];
  }

  free(state);
}

void
random_unique_gen(intkey_t *key, value_t *value, size_t ntuples)
{
  for (uint32_t i = 0; i < ntuples; i++) {
	key[i] = i + 1;
	value[i] = key[i];
  }
  knuth_shuffle(key, value, ntuples);
}
#else
void
random_gen(tuple_t *tuple, size_t ntuples, const int32_t minid, const int32_t maxid)
{
  uint32_t seed = rand();
  rand_state *state = rand_init(seed);

  for (uint32_t i = 0; i < ntuples; i++) {
    tuple[i].key = RAND_RANGE2(minid, maxid, state);
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

  knuth_shuffle(tuple, ntuples);
}
#endif

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
#ifdef COLUMN_WISE
    rel->key[node] = (intkey_t*)alloc(sizeof(intkey_t) * ntuples_on_node);
    rel->value[node] = (value_t*)alloc(sizeof(value_t) * ntuples_on_node);
    memset(rel->key[node], 0x0, sizeof(intkey_t) * ntuples_on_node);
    memset(rel->value[node], 0x0, sizeof(value_t) * ntuples_on_node);
#else
    tuple_t *tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples_on_node);
    memset(tuples, 0x0, sizeof(tuple_t) * ntuples_on_node);
    rel->tuples[node] = tuples;
#endif
  }

  pthread_barrier_wait(arg->barrier_alloc);


#ifdef COLUMN_WISE
  intkey_t * key = rel->key[node] + arg->offset;
  value_t * value = rel->value[node] + arg->offset;
  random_gen(key, value, arg->ntuples, arg->minid, arg->maxid);
#else
  tuple_t *tuples = rel->tuples[node] + arg->offset;
  random_gen(tuples, arg->ntuples, arg->minid, arg->maxid);
#endif
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
#ifdef COLUMN_WISE
    rel->key[node] = (intkey_t*)alloc(sizeof(intkey_t) * ntuples_on_node);
    rel->value[node] = (value_t*)alloc(sizeof(value_t) * ntuples_on_node);
    memset(rel->key[node], 0x0, sizeof(intkey_t) * ntuples_on_node);
    memset(rel->value[node], 0x0, sizeof(value_t) * ntuples_on_node);
#else
    tuple_t *tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples_on_node);
    memset(tuples, 0x0, sizeof(tuple_t) * ntuples_on_node);
    rel->tuples[node] = tuples;
#endif
  }

  pthread_barrier_wait(arg->barrier_alloc);

#ifdef COLUMN_WISE
  intkey_t * key = rel->key[node] + arg->offset;
  value_t * value = rel->value[node] + arg->offset;
  random_gen(key, value, arg->ntuples, arg->minid, arg->maxid);
#else
  tuple_t *tuples = rel->tuples[node] + arg->offset;
  random_gen(tuples, arg->ntuples, arg->minid, arg->maxid);
#endif

  int firstkey = arg->firstkey;
#ifdef COLUMN_WISE
  for (uint32_t i = 0; i != arg->ntuples; ++i) {
    key[i] = firstkey++;
    value[i] = key[i];
  }
  knuth_shuffle(key, value, arg->ntuples);
#else
  for (uint32_t i = 0; i < arg->ntuples; i++) {
    tuples->key = firstkey++;
    tuples->payload = tuples->key;
    tuples++;
  }
  tuples = rel->tuples[node] + arg->offset;
  knuth_shuffle(tuples, arg->ntuples);
#endif

  return NULL;
}

relation_t *
parallel_build_relation_fk(const size_t ntuples, 
                           const int32_t minid, const int32_t maxid,
                           const uint32_t nnodes, const uint32_t nthreads)
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
      args[tid].minid = minid;
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

/*
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
*/

/*
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
*/

void*
build_scalar_thread(void *params)
{
  build_arg_t *arg = (build_arg_t*)params;
  int cpu = arg->cpu;
  int node = arg->node;
  relation_t *rel = arg->rel;

  cpu_bind(cpu);
  cpu_membind(cpu);

  if (cpu == cpu_of_node(node, 0)) {
    size_t ntuples_on_node = arg->rel->ntuples_on_node[node];
#ifdef COLUMN_WISE
    rel->key[node] = (intkey_t*)alloc(sizeof(intkey_t) * ntuples_on_node);
    rel->value[node] = (value_t*)alloc(sizeof(value_t) * ntuples_on_node);
    memset(rel->key[node], 0x0, sizeof(intkey_t) * ntuples_on_node);
    memset(rel->value[node], 0x0, sizeof(value_t) * ntuples_on_node);
#else
    tuple_t *tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples_on_node);
    memset(tuples, 0x0, sizeof(tuple_t) * ntuples_on_node);
    rel->tuples[node] = tuples;
#endif
  }

  pthread_barrier_wait(arg->barrier_alloc);

  size_t ntuples = arg->ntuples;

#ifdef COLUMN_WISE
  intkey_t *key = rel->key[node] + arg->offset;
  value_t *value = rel->value[node] + arg->offset;
  for (uint32_t i = 0; i != arg->scalars; ++i) {
    key[i] = 1;
	value[i] = key[i];
  }
#else
  tuple_t *tuples = rel->tuples[node] + arg->offset;
  assert(tuples != NULL);
  for (uint32_t i = 0; i != arg->scalars; ++i) {
    tuples[i].key = 1;
    tuples[i].payload = tuples[i].key;
  }
#endif

  rand_state *state = rand_init(rand());

#ifdef COLUMN_WISE
  // generate keys from 2 - maxid
  for (uint32_t i = arg->scalars; i != ntuples; ++i) {
    key[i] = RAND_RANGE2(2, arg->maxid, state);
    value[i] = key[i];
  }
  knuth_shuffle(key, value, ntuples);
#else
  // generate keys from 2 - maxid
  for (uint32_t i = arg->scalars; i != ntuples; ++i) {
    tuples[i].key = RAND_RANGE2(2, arg->maxid, state);
    tuples[i].payload = tuples[i].key;
  }
  knuth_shuffle(tuples, ntuples);
#endif

   free(state);

  return NULL;
}


relation_t *
build_scalar_skew(const size_t ntuples, const int32_t maxid,
                  const uint32_t nnodes, const uint32_t nthreads,
                  int scalar_skew)
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

  size_t scalars_per_thread = scalar_skew / nthreads;
  size_t scalars_lastthread = scalar_skew - scalars_per_thread * (nthreads-1);

  uint32_t tid = 0;
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
      args[tid].scalars = (tid == nthreads-1) ? scalars_lastthread : scalars_per_thread;
    }
  }

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_create(&threads[i], NULL, build_scalar_thread, (void*)&args[i]);

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_join(threads[i], NULL);

  for (uint32_t node = 0; node < nnodes; ++node)
    pthread_barrier_destroy(&barriers[node]);

  return rel;
}




/* node 0 has q times more tuples than other nodes */
relation_t *
build_placement_skew(const size_t ntuples, const int32_t minid, const int32_t maxid,
                     const uint32_t nnodes, const uint32_t nthreads,
                     int q)
{
  assert(nnodes <= num_numa_nodes());

  relation_t *rel = relation_init(nnodes);

  assert(nthreads >= nnodes);

  rel->ntuples = ntuples;
  rel->nnodes = nnodes;

  build_arg_t args[nthreads];
  pthread_t threads[nthreads];
  pthread_barrier_t barriers[nnodes];

  size_t ntuples_per_node = ntuples / (q + nnodes - 1); // different from uniform case
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
      args[tid].minid = minid;
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


relation_t * build_zipf(size_t tuples, uint8_t nodes, double theta)
{

  uint64_t *data[nodes], size[nodes];

  for (int i = 0; i != nodes; ++i) {
    size[i] = tuples / nodes;
    data[i] = NULL;
  }

  uint64_t bits[65];
  
  zipf(data, size, numa, theta, 1, bits);

  // copy data 

}
