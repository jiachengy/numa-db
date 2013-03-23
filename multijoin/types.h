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
  OpBuild,
  OpProbe,
  OpUnitProbe,
};

#endif // TYPES_H_
