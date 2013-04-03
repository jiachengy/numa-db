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

  int nthreads = 1;
  size_t rsize = 1024 * 1024 * 16; // 128M
  //  size_t ssize = 128 * 1024 * 1024; // 128M

  relation_t * relR = parallel_build_relation_fk(rsize, 1 << 30, 1, 8);
  //  relation_t *relS = parallel_build_relation_fk(ssize, rsize, 1, 8);
  logging("Building table with %ld tuples done.\n", rsize);

  size_t capacity = rsize * 32;
  Environment *env = new Environment(1, nthreads, capacity);

  logging("Environment initialized.\n");

  env->RadixPartition(relR);

  logging("Query initialized.\n");

  // //  env->TwoPassPartition(relR, relS);

  Run(env);

  logging("Query done.\n");


#ifdef USE_PERF
  perf_lib_cleanup();
#endif

  delete env;

  return 0;
}
