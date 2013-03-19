#ifndef RECYCLER_H_
#define RECYCLER_H_

#include <vector>
#include <list>
#include "table.h"

using namespace std;

class Recycler
{
 private:
	size_t alloc_size_;
	uint32_t nnodes_;
	vector<list<Partition*> > freelists_;
	void Alloc(int node);

 public:
	Recycler(size_t alloc_size, uint32_t nnodes);
	~Recycler();	
	// get an empty partition
	Partition* GetSlot(int node);	
	// put back the partition for recycling
	void Putback(Partition *p);
};

#endif // RECYCLER_H_
