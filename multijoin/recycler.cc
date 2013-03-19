#include <glog/logging.h>
#include <cassert>
#include <numa.h>

#include "recycler.h"
#include "util.h" // node of cpu

Recycler::Recycler(size_t alloc_size, uint32_t nnodes) {
	alloc_size_ = alloc_size;
	nnodes_ = nnodes;
	freelists_.resize(nnodes);
	for (uint32_t node = 0; node < nnodes; node++) {
		Alloc(node);
	}
}

Recycler::~Recycler() {
	for (uint32_t node = 0; node < nnodes_; node++) {
		list<Partition*> &free = freelists_[node];
		for (list<Partition*>::iterator it = free.begin();
			 it != free.end(); it++) {
			delete *it;
		}
		free.clear();
	}
}

	
// get an empty partition
Partition* Recycler::GetSlot(int node) {
	list<Partition*> &free = freelists_[node];
	if (free.empty())
		Alloc(node);

	Partition *p = free.front();
	free.pop_front();
	return p;
}
	
// put back the partition for recycling
void Recycler::Putback(Partition *p) {
	int node = p->node();
	list<Partition*> &free = freelists_[node];
	free.push_back(p);
}


void Recycler::Alloc(int node)
{
	// bind memeory
	numa_run_on_node(node);
	struct bitmask *numa_node = numa_get_run_node_mask();
	numa_set_membind(numa_node);

	list<Partition*> &free = freelists_[node];
	free.resize(alloc_size_);
		
	for (list<Partition*>::iterator it = free.begin();
		 it != free.end(); it++) {
		*it = new Partition(node, -1);
	}

	// after allocating, reset mem binding
	numa_set_membind(numa_all_nodes_ptr);
}
