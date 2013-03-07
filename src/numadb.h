#ifndef NUMADB_H_
#define NUMADB_H_

#include <iostream>
#include <stdint.h>

typedef int32_t data_t;
typedef uint32_t val_t;
typedef int32_t rid_t;

extern size_t block_size;
extern size_t ngroups;
extern bool local;

#endif // NUMADB_H_
