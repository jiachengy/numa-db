#ifndef BUILDER_H_
#define BUILDER_H_

#include "table.h"
#include "recycler.h"

class PartitionBuilder;
class TableBuilder;

struct BuildArg
{
  int cpu;
  uint32_t nparts;

  Partition **partitions;
  PartitionBuilder *builder;
  Recycler *recycler;
};


class PartitionBuilder
{
 private:
 public:
  Partition *Build(size_t size, Recycler *recycler);
};

class TableBuilder
{
 private:
  Recycler **recyclers_;
  PartitionBuilder pbuilder_;
  static void* build(void *params);
 public:
  TableBuilder(Recycler **recyclers) : recyclers_(recyclers) {}

  void Build(Table *table, size_t sz, uint32_t nthreads);
};

#endif // BUILDER_H_
