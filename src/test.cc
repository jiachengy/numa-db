#include <iostream>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "numadb.h"
#include "builder.h"
#include "plan.h"
#include "optimizer.h"
#include "engine.h"
#include "atomic.h"

using namespace std;

QueryEngine qe;

int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	FLAGS_logtostderr = true;

	size_t nthreads = 1;
	
	Optimizer optimizer;
	Plan *plan = optimizer.Compile(nthreads);

	qe.Query(plan);
}
