#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include "plan.h"

class Optimizer
{
 public:
	Plan* Compile(size_t nworkers);
};

#endif // OPTIMIZER_H_
