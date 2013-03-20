#include <iostream>
#include <pthread.h>
#include <glog/logging.h> // logging
#include <gflags/gflags.h> // logging

#include "types.h"
#include "builder.h"
#include "table.h"
#include "recycler.h"
#include "multijoin.h"

using namespace std;

int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	FLAGS_logtostderr = true;


    int nthreads = atoi(argv[0]);

	Table *relR = new Table(0, OpPartition, 4, 0, 64);
	Table *relS = new Table(1, OpPartition, 4, 0, 64);
	TableBuilder tb;
	
	size_t sz = Partition::kPartitionSize * 64 * 16;
	tb.Build(relR, sz);
    tb.Build(relS, sz);


    Hashjoin(relR, relS, nthreads);

	delete relR;
    delete relS;
}
