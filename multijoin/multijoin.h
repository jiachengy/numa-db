#ifndef MULTIJOIN_H_
#define MULTIJOIN_H_

#include "env.h"
#include "table.h"

void HashJoin(Environment *env, Table *relR, Table *relS);

#endif // MULTIJOIN_H_
