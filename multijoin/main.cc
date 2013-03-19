#include <iostream>
#include <glog/logging.h> // logging
#include <gflags/gflags.h> // logging

#include "types.h"
#include "builder.h"
#include "table.h"
#include "recycler.h"

using namespace std;

int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	FLAGS_logtostderr = true;


	Recycler re(100, 4);

	for (int i = 0; i < 200; i++) {
		Partition *p = re.GetSlot(1);
	}

	for (int i = 0; i < 200; i++) {
		Partition *p = re.GetSlot(2);
	}


	for (int i = 0; i < 200; i++) {
		Partition *p = re.GetSlot(3);
	}


	for (int i = 0; i < 200; i++) {
		Partition *p = re.GetSlot(0);
	}

	// Table *table = new Table(0, OpPartition, 4, 0, 64);
	// TableBuilder tb;
	
	// size_t sz = Partition::kPartitionSize * 64 * 16;
	// tb.Build(table, sz);

	// delete table;
}
