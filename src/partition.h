#ifndef PARTITION_H_
#define PARTITION_H_

#include <stdint.h>
#include <vector>

#include "hashtable.h"
#include "numadb.h"
#include "executor.h"
#include "ptable.h"

class Block;
class Partition;

typedef void (*Exec)(Block block, Partition *out);

struct Block
{
	data_t *data;
	rid_t *rids;
	size_t size;

	Block(data_t *d, rid_t *r, size_t sz) {
		data = d;
		rids = r;
		size = sz;
	}
	bool empty() { return size == 0; }
};



class Partition
{
 public:
	Partition(int node, int cpu) {
		this->node = node;
		this->cpu = cpu;
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
		size_t sz = block_size;
		int pos = curpos;
		if (curpos + sz > size) {
			sz = size - curpos;
			curpos = -1;
		}
		else
			curpos += block_size;
		return Block(data+pos, rids+pos, sz);
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
	int curpos;

	// Pipeline Operation
	Exec exec;
	Partition *output;

	// Parent reference
	PTable *table;
};



class HTPartition : public Partition
{
 private:
 public:
 HTPartition(int node, int cpu, size_t ngroups)
	 : Partition(node, cpu) {
		ngroups_ = ngroups;
		hashtable = NULL;
	}
	size_t ngroups_;
	HashTable *hashtable;
};

#endif // PARTITION_H_
