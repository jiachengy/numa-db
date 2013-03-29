#ifndef BUILDER_H_
#define BUILDER_H_

#include <pthread.h>

#include "types.h" // relation_t

struct build_arg_t
{
  int tid;
  int cpu;
  int node;
  
  int offset; /* offset within the group */
  int firstkey;
  size_t ntuples;
  int maxid; /* for foreign key build */

  pthread_barrier_t *barrier_alloc;

  relation_t *rel;
};

relation_t* relation_init(uint32_t nnodes);
void relation_destroy(relation_t *rel);

relation_t* parallel_build_relation_pk(size_t ntuples, uint32_t nnodes, uint32_t nthreads);
relation_t *parallel_build_relation_fk(const size_t ntuples, const int32_t maxid, const uint32_t nnodes, const uint32_t nthreads);

#endif // BUILDER_H_
