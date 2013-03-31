#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <glog/logging.h> // logging
#include <gflags/gflags.h> // logging

#include "params.h"
#include "types.h"
#include "builder.h"
#include "table.h"
#include "recycler.h"
#include "multijoin.h"

#include "perf.h" // papi

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

size_t Params::kMaxHtTuples = 0;

using namespace std;

void
radix_cluster(tuple_t *  outRel,
              tuple_t * inRel,
              size_t ntuples,
              int R,
              int D)
{

  long t = micro_time();
  perf_t *perf = perf_init();
  perf_start(perf);

  uint32_t i;
  uint32_t M = ((1 << D) - 1) << R;
  uint32_t offset;
  uint32_t fanOut = 1 << D;

  /* the following are fixed size when D is same for all the passes,
     and can be re-used from call to call. Allocating in this function
     just in case D differs from call to call. */
  uint32_t dst[fanOut];
  int32_t hist[fanOut];

  memset(hist, 0x0, sizeof(int32_t) * fanOut);

  /* count tuples per cluster */
  for( i=0; i < ntuples; i++ ){
    uint32_t idx = HASH_BIT_MODULO(inRel[i].key, M, R);
    hist[idx]++;
  }

  offset = 0;
  /* determine the start and end of each cluster depending on the counts. */
  for ( i=0; i < fanOut; i++ ) {
    /* dst[i]      = outRel->tuples + offset; */
    /* determine the beginning of each partitioning by adding some
       padding to avoid L1 conflict misses during scatter. */
    dst[i] = offset;
    //    dst[i] = offset + i * SMALL_PADDING_TUPLES;
    offset += hist[i];
  }

  /* copy tuples to their corresponding clusters at appropriate offsets */
  for( i=0; i < ntuples; i++ ){
    uint32_t idx   = HASH_BIT_MODULO(inRel[i].key, M, R);
    outRel[ dst[idx] ] = inRel[i];
    ++dst[idx];
  }

  t = micro_time() - t;

  perf_stop(perf);
  perf_read(perf);
  perf_print(perf);
  perf_destroy(perf);

  LOG(INFO) << "Eclapsed time: " << (t / 1000);
}


int main(int argc, char *argv[])
{
  srand(time(NULL));

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

#ifdef USE_PERF
  perf_lib_init(NULL, NULL);
#endif

  int nthreads = atoi(argv[1]);
      
  size_t rsize = 16 * 1024 * 1024; // 128M
  //  size_t ssize = 128 * 1024 * 1024; // 128M

  relation_t *relR = parallel_build_relation_fk(rsize, rsize, 1, 8);
  //  relation_t *relS = parallel_build_relation_fk(ssize, rsize, 1, 8);

  LOG(INFO) << "Building tables done.";
  LOG(INFO) << "==============================";

  // cpu_bind(0);
  // tuple_t *out = (tuple_t*)alloc(sizeof(tuple_t) * rsize);
  // memset(out, 0x0, sizeof(tuple_t) * rsize);
  // radix_cluster(out, relR->tuples[0], rsize, Params::kOffsetPass1, Params::kNumBitsPass1);

  size_t mem_limit = 1024L  * 1024L * 1024L * 8; // 4GB
  
  Params::kMaxHtTuples = 0; // rsize / Params::kFanoutPass1 * 2;
  LOG(INFO) << "max ht tuples: " << Params::kMaxHtTuples;

  Environment *env = new Environment(1, nthreads, mem_limit);
  LOG(INFO) << "Env set up.";

  //  HashJoin(env, relR, relS);
  //  RadixPartition(env, relR);

  //    env->RadixPartition(relR);
  env->TwoPassPartition(relR, NULL);

  Run(env);

#ifdef USE_PERF
  perf_lib_cleanup();
#endif

  delete env;

  return 0;
}
