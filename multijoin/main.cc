#include <stdio.h>
#include <pthread.h>
#include <papi.h>

#include "env.h"
#include "builder.h"
#include "multijoin.h"

#include "perf.h" // papi

int main(int argc, char *argv[])
{
  srand(time(NULL));

#ifdef USE_PERF
  perf_lib_init(NULL, NULL);
#endif
  int nodes = 1;
  int nthreads = 1;

  if (argc > 1)
    nthreads = atoi(argv[1]);
  if (argc > 2)
    nodes = atoi(argv[2]);
  
  size_t rsize = 1024 * 1024 * 16; // 128M
  //  size_t ssize = 128 * 1024 * 1024; // 128M

  relation_t * relR = parallel_build_relation_pk(rsize, 1, 1);
  //  relation_t *relS = parallel_build_relation_fk(ssize, rsize, 1, 8);
  logging("Building table with %ld tuples done.\n", rsize);

  size_t capacity = rsize * 8;
  Environment *env = new Environment(nodes, nthreads * nodes, capacity);

  logging("Environment initialized.\n");

  //  env->RadixPartition(relR);
  env->TwoPassPartition(relR);
  //  env->TwoPassPartition(relR, relS);


  logging("Query initialized.\n");



  Run(env);

  logging("Query done.\n");


#ifdef USE_PERF
  perf_lib_cleanup();
#endif

  delete env;

  return 0;
}
