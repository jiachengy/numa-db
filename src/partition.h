#ifndef PARTITION_H_
#define PARTITION_H_

#include <stdint.h>
#include <vector>

#include "numadb.h"
#include "executor.h"
#include "ptable.h"

class Block;
class Partition;

typedef void (*Exec)(Block block, PTable *out);

struct Block
{
	data_t *data_;
	rid_t *rids_;
	size_t size_;

	Block(data_t *data, rid_t *rids, size_t sz) {
		data_ = data;
		rids_ = rids;
		size_ = sz;
	}
	bool empty() { return size_ == 0; }
};



class Partition
{
 public:
	Partition() {
		node = cpu = -1;
		ready = done = false;
		shared = sorted = false;
		pkey = 0;
		size = 0;
		curpos = 0;
		exec = NULL;
		output = NULL;
		table = NULL;
	}

	bool Eop() {
		return curpos == -1;
	}

	Block Next() {
		size_t sz = BLOCK_SIZE;
		int pos = curpos;
		if (curpos + sz > size) {
			sz = size - curpos;
			curpos = -1;
		}
		return Block(data+curpos, rids+curpos, sz);
	}

	// Location and cpu info
	int node;
	int cpu; // we have one thread on each cpu

	// ready and done
	bool ready;
	bool done;

	// Data dependency
	std::vector<Partition> waitfors;
	std::vector<Partition> waitbys;

	// Sharing info
	bool shared;

	// Partition-level property
	bool sorted;
	int pkey;

	// Partition data
	size_t size;
	data_t *data;
	rid_t *rids;

	// Current position
	unsigned int curpos;

	// Pipeline Operation
	Exec exec;
	PTable *output;

	// Parent reference
	PTable *table;
};


#endif // PARTITION_H_
