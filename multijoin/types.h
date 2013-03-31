#ifndef TYPES_H_
#define TYPES_H_

#include <stdint.h>
#include <stdlib.h>

typedef int32_t intkey_t;
typedef int32_t value_t;

struct tuple_t {
  intkey_t key;
  value_t  payload;
};

enum OpType {
  OpNone,
  OpPartition,
  OpPartition2,
  OpBuild,
  OpProbe,
  OpUnitProbe,
};

enum ShareLevel {
  ShareLocal,
  ShareNode,
  ShareGlobal
};


struct relation_t
{
  tuple_t **tuples;
  size_t *ntuples_on_node;
  size_t ntuples;
  uint32_t nnodes;
};


#endif // TYPES_H_
