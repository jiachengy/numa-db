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

size_t Params::kNtuples = 0;
size_t Params::kMaxHtTuples = 0;


using namespace std;

int main(int argc, char *argv[])
{
  srand(time(NULL));

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

#ifdef USE_PERF
  perf_lib_init(NULL, NULL);
#endif

  if (argc != 2) {
    cout << "Usage: ./test <nthreads>" << endl;
    exit(1);
  }
      
  //  int nthreads = atoi(argv[1]);
  size_t ntuples = 128 * 1024 * 1024; // 128M
  //  size_t mem_limit = 1024L  * 1024L * 1024L * 16; // 4GB
  
  // Params::kNtuples = ntuples;
  // Params::kMaxHtTuples = ntuples / Params::kFanoutPass1 * 2;
  // LOG(INFO) << "max ht tuples: " << Params::kMaxHtTuples;

  // Environment *env = new Environment(nthreads, mem_limit);
  // LOG(INFO) << "Env set up.";

  relation_t *relR = parallel_build_relation_pk(ntuples, 4);
  //  relation_t *relS = parallel_build_relation_pk(ntuples, 4);

  // LOG(INFO) << "Building tables done.";

  // HashJoin(env, relR, relS);

  // LOG(INFO) << "Hash join done.";

  // delete env;
  // LOG(INFO) << "Env deleted.";


#ifdef USE_PERF
  perf_lib_cleanup();
#endif

  return 0;
}
