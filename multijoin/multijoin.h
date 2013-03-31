#ifndef MULTIJOIN_H_
#define MULTIJOIN_H_

#include "env.h"
#include "types.h"

void HashJoin(Environment *env, relation_t *relR, relation_t *relS);

void Run(Environment *env);


#endif // MULTIJOIN_H_
