#include <stdio.h>
#include <pthread.h>
#include <papi.h>

#include "env.h"
#include "builder.h"
#include "multijoin.h"

#include "perf.h" // papi

config_t gConfig;

int main(int argc, char *argv[])
{
  srand(time(NULL));

#ifdef USE_PERF
  perf_lib_init(NULL, NULL);
#endif

  int nodes = 1;
  int nthreads = 1;
  int tps_skew = 1;
  double scalar_skew = 0;

  if (argc > 1)
    nthreads = atoi(argv[1]);
  if (argc > 2)
    nodes = atoi(argv[2]);
  if (argc > 3)
    tps_skew = atoi(argv[3]);
  if (argc > 4)
    scalar_skew = atof(argv[4]);

  size_t rsize = 1024L * 1024L * 128; // 128M
  size_t ssize = 1024L * 1024L * 128; // 16M

  //  relation_t * relR = build_placement_skew(rsize, rsize, nodes, nthreads, tps_skew);
  relation_t * relR = build_scalar_skew(rsize, rsize, nodes, nthreads, scalar_skew);
  logging("Building R table with %ld tuples done.\n", rsize);

  relation_t *relS = parallel_build_relation_fk(ssize, rsize, nodes, nthreads);
  logging("Building S table with %ld tuples done.\n", ssize);

  gConfig.mem_per_thread = rsize / nthreads * 64;

  Environment *env = new Environment(nodes, nthreads);

  logging("Environment initialized.\n");

  //   env->PartitionAndBuild(relR);
  //  env->RadixPartition(relR);
  //  env->TwoPassPartition(relR);
  //  env->TwoPassPartition(relR, relS);
  env->Hashjoin(relR, relS);

  logging("Query initialized.\n");

  Run(env);

  logging("Query done.\n");


#ifdef USE_PERF
  perf_lib_cleanup();
#endif

  delete env;

  return 0;
}
