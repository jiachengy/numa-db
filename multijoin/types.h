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

typedef struct {
#ifdef COLUMN_WISE
  uint32_t data[16];
#else
  uint64_t data[8];
#endif
} cache_line_t;


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

#ifdef COLUMN_WISE
  intkey_t **key;
  value_t **value;
#else
	tuple_t **tuples;
#endif

  size_t *ntuples_on_node;
  size_t ntuples;
  uint32_t nnodes;
};


#endif // TYPES_H_
