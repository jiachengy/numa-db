#ifndef NUMADB_H_
#define NUMADB_H_

#include <iostream>
#include <stdint.h>

typedef int32_t data_t;
typedef uint32_t val_t;
typedef int32_t rid_t;

const size_t inflate = 4;

extern size_t block_size;
extern size_t ngroups;
extern bool local;
extern bool local2;
extern int hops;
extern int rrobin[];

extern bool roundrobin;

#endif // NUMADB_H_
