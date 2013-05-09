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
  size_t rsize = 1024L * 1024L * 1024L;
  size_t ssize = 1024L * 1024L * 1024L;

  int nodes = 1;
  int nthreads = 1;
  int tps_skew = 1;
  double scalar_ratio_r = 0.1;
  double scalar_ratio_s = 0.1;
  int scalar_skew_r = 1;
  int scalar_skew_s = 1;

  if (argc > 1)
    nthreads = atoi(argv[1]);
  if (argc > 2)
    nodes = atoi(argv[2]);
  if (argc > 3)
    tps_skew = atoi(argv[3]);
  if (argc > 4) {
    scalar_ratio_r = atof(argv[4]);
    scalar_skew_r = rsize * scalar_ratio_r;
  }
  if (argc > 5) {
    scalar_ratio_s = atof(argv[5]);
    scalar_skew_s = ssize * scalar_ratio_s;
  }

  //  logging("run with scalar skew: %d, %d\n", scalar_skew_r, scalar_skew_s);

  //  relation_t *relR = parallel_build_relation_fk(rsize, 2, rsize, nodes, nthreads);
  //  relation_t * relR = build_placement_skew(rsize, 2, rsize, nodes, nthreads, tps_skew);
  relation_t * relR = build_scalar_skew(rsize, rsize, nodes, nthreads, scalar_skew_r);
  logging("Building R table with %ld tuples done.\n", rsize);

  // relation_t * relS = parallel_build_relation_pk(ssize, nodes, nthreads);
   relation_t * relS = build_scalar_skew(ssize, ssize, nodes, nthreads, 1);
  //  relation_t *relS = parallel_build_relation_fk(ssize, 2, rsize, nodes, nthreads);
  //  relation_t * relS = build_placement_skew(ssize, 2, ssize, nodes, nthreads, tps_skew);
   logging("Building S table with %ld tuples done.\n", ssize);
   
  // gConfig.large_mem_per_thread = rsize / nthreads * 2;
  // gConfig.small_mem_per_thread = rsize / nthreads * 4;
  // gConfig.hashtable_per_thread = 0;

  gConfig.large_mem_per_thread = (rsize+ssize) / nthreads * 4;
  gConfig.small_mem_per_thread = (rsize+ssize) / nthreads * 5;
  gConfig.hashtable_per_thread = 1024 * 1024 * 1024 / nthreads * 2;

  Environment *env = new Environment(nodes, nthreads);

  logging("Environment initialized.\n");

  //   env->PartitionAndBuild(relR);
  //   env->RadixPartition(relR);
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
