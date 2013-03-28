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
      
  int nthreads = atoi(argv[1]);
  size_t sz = Params::kPartitionSize * 16;
  size_t mem_limit = 1024L  * 1024L * 1024L * 4; // 4GB

  Environment *env = new Environment(nthreads, mem_limit);
  LOG(INFO) << "Env set up.";

  Table *relR = env->BuildTable(sz);
  Table *relS = env->BuildTable(sz);
  LOG(INFO) << "Building tables done.";

  HashJoin(env, relR, relS);

  LOG(INFO) << "Hash join done.";

  delete env;
  LOG(INFO) << "Env deleted.";


#ifdef USE_PERF
  perf_lib_cleanup();
#endif

  return 0;
}
