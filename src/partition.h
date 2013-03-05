#ifndef PARTITION_H_
#define PARTITION_H_

#include <stdint.h>
#include <vector>

#include "numadb.h"
#include "ptable.h"
#include "executor.h"


class Partition
{
 public:
	// Location and cpu info
	int node;
	int cpu; // we have one thread on each cpu

	// ready and done
	bool ready = false;
	bool done = false;

	// Data dependency
	std::vector<Partition> waitfors;
	std::vector<Partition> waitbys;

	// Sharing info
	bool shared = false;

	// Partition-level property
	bool sorted = false;
	int pkey = false;

	// Partition data
	size_t size;
	data_t *data;
	rid_t *rids;

	// Current position
	unsigned int curpos = 0;

	// Pipeline Operation
	Executor *excute = NULL;
	Partition *output = NULL;

	// Parent reference
	PTable *table;
};


#endif // PARTITION_H_
