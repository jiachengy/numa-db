#ifndef BUILDER_H_
#define BUILDER_H_

#include "partition.h"
#include "ptable.h"

class PartitionBuilder;
class TableBuilder;

struct BuildArg
{
	size_t size;
	int cpu;
	Partition* p;
	PTable *table;
	PartitionBuilder *builder;
};


class PartitionBuilder
{
 private:
 public:
	Partition *Build(size_t size, int cpu, PTable *table);
};

class TableBuilder
{
 private:
	PartitionBuilder pbuilder_;

 public:
	PTable *Build(size_t nparts, size_t psize);
};

#endif // BUILDER_H_
