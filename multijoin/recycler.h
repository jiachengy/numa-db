#ifndef RECYCLER_H_
#define RECYCLER_H_


#include <list>
#include "partition.h"

class Recycler
{
 private:
	std::list<Partition*> free_[4];
 public:
	void Alloc(size_t nslots);
	
	// get an empty partition
	Partition *GetEmptyPartition(int node);
	
	// push back the partition to allow recycling
	void Recycle(int node, Partition *part);
};

#endif // RECYCLER_H_
