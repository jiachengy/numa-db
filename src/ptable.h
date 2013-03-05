#ifndef PTABLE_H
#define PTABLE_H

#include <iostream>
#include <vector>

#include "partition.h"

class Partition;

class PTable
{
 private:
	std::vector<Partition*> partitions_;

 public:
	PTable() {}
	~PTable() {}

	size_t size() { return partitions_.size(); }
	void AddPartition(Partition* p) { partitions_.push_back(p); }
	Partition* GetPartition(unsigned int i) { return partitions_[i]; }
};

#endif // PTABLE_H
