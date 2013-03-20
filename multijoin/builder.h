#ifndef BUILDER_H_
#define BUILDER_H_

#include "table.h"

class PartitionBuilder;
class TableBuilder;

struct BuildArg
{
	int cpu;
	uint32_t nparts;

	Partition** partitions;
	PartitionBuilder *builder;
};


class PartitionBuilder
{
 private:
 public:
	Partition *Build(size_t size);
};

class TableBuilder
{
 private:
	PartitionBuilder pbuilder_;
	static void* build(void *params);
 public:
	void Build(Table *table, size_t sz, uint32_t nthreads);
};

#endif // BUILDER_H_
