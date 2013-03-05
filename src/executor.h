#ifndef EXECUTOR_H_
#define EXECUTOR_H_

#include "numadb.h"

typedef void (*Executor)(data_t *data, rid_t *rids, unsigned int size, Partition *output);

#endif // EXECUTOR_H_
