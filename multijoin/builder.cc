#include <assert.h>
#include <pthread.h>

#include "builder.h"
#include "util.h"

#define RAND_RANGE(N, STATE) (rand_next(STATE) % N)

void
knuth_shuffle(tuple_t *tuples, size_t ntuples, uint32_t seed)
{
  rand_state *state = rand_init(seed);
  for (int i = ntuples - 1; i > 0; i--) {
    int32_t  j = RAND_RANGE(i,state);
    intkey_t tmp = tuples[i].key;
    tuples[i].key = tuples[j].key;
    tuples[j].key = tmp;
  }
  free(state);
}

void
knuth_shuffle_rel(relation_t *rel, uint32_t seed)
{
  size_t ntuples_per_node = rel->ntuples / num_numa_nodes();

  rand_state *state = rand_init(seed);
  for (int i = rel->ntuples - 1; i > 0; i--) {
    int32_t j = RAND_RANGE(i, state);

    int node_j = j / ntuples_per_node;
    int offset_j = j - ntuples_per_node * node_j;

    int node_i = i / ntuples_per_node;
    int offset_i = i - ntuples_per_node * node_i;

    intkey_t tmp = rel->tuples[node_i][offset_i].key;
    rel->tuples[node_i][offset_i].key = rel->tuples[node_j][offset_j].key;
    rel->tuples[node_j][offset_j].key = tmp;
  }
  free(state);
}


relation_t*
relation_init()
{
  relation_t *rel = (relation_t*)malloc(sizeof(relation_t));
  rel->tuples = (tuple_t**)malloc(sizeof(tuple_t*) * num_numa_nodes());
  rel->ntuples_on_node = (size_t*)malloc(sizeof(size_t*) * num_numa_nodes());
  memset(rel->tuples, 0x0, sizeof(tuple_t*) * num_numa_nodes());
  memset(rel->ntuples_on_node, 0x0, sizeof(size_t*) * num_numa_nodes());
  return rel;
}

void
relation_destroy(relation_t *rel)
{
  for (int node = 0; node < num_numa_nodes(); ++node) {
    if (rel->tuples[node] != NULL)
      dealloc(rel->tuples[node], sizeof(tuple_t) * rel->ntuples_on_node[node]);
  }
  free(rel->tuples);
  free(rel->ntuples_on_node);
  free(rel);
}


void*
build_pk_thread(void *params)
{
  build_arg_t *arg = (build_arg_t*)params;
  int cpu = arg->cpu;
  int node = arg->node;
  relation_t *rel = arg->rel;

  cpu_bind(cpu);

  if (cpu == cpu_of_node(node, 0)) {
    size_t ntuples_on_node = arg->rel->ntuples_on_node[node];
    tuple_t *tuples = (tuple_t*)alloc(sizeof(tuple_t) * ntuples_on_node);
    memset(tuples, 0x0, sizeof(tuple_t) * ntuples_on_node);
    rel->tuples[node] = tuples;
  }

  pthread_barrier_wait(arg->barrier_alloc);

  // every one starts to build from its offset
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
parallel_build_relation_pk(size_t ntuples, uint32_t nthreads)
{
  relation_t *rel = relation_init();
  uint32_t nnodes = num_numa_nodes();

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
  int firstkey = 1; // we start from 1
  for (uint32_t node = 0; node < nnodes; ++node) {
    rel->ntuples_on_node[node] = (node == nnodes-1) ? ntuples_on_lastnode : ntuples_per_node;
    size_t nthreads_on_node = (node == nnodes - 1) ? nthreads_per_node : nthreads_on_lastnode;
    size_t ntuples_per_thread = rel->ntuples_on_node[node] / nthreads_on_node;
    size_t ntuples_lastthread = rel->ntuples_on_node[node] - ntuples_per_thread * (nthreads_on_node - 1);

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

    pthread_barrier_init(&barriers[node], NULL, nthreads_on_node);
  }

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_create(&threads[i], NULL, build_pk_thread, (void*)&args[i]);

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_join(threads[i], NULL);

  for (uint32_t i = 0; i < nthreads; i++) {
    pthread_barrier_destroy(args[i].barrier_alloc);
  }

  uint32_t seed = rand();
  knuth_shuffle_rel(rel, seed);

  return rel;
}



