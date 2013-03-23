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

    if (argc != 2) {
      cout << "Usage: ./test <nthreads>" << endl;
      exit(1);
    }
      
    int nthreads = atoi(argv[1]);

	Table *relR = new Table(4, 0);
	Table *relS = new Table(4, 0);
	TableBuilder tb;


	size_t sz = Partition::kPartitionSize * 128;
	LOG(INFO) << "Building tables.";
	tb.Build(relR, sz, nthreads);
    tb.Build(relS, sz, nthreads);
	LOG(INFO) << "Building done.";

    Hashjoin(relR, relS, nthreads);

	LOG(INFO) << "Hash join done.";

    //	delete relR;
    //    delete relS;

	LOG(INFO) << "Delete table done";

}
