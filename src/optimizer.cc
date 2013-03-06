#include "builder.h"
#include "optimizer.h"

Plan* test_plan(size_t nworkers)
{
	TableBuilder builder;
	PTable *table = builder.Build(4,1000);

	Plan *plan = new Plan;
	plan->leaves.push_back(table);
	plan->nworkers = 4;
	return plan;
}


Plan* Optimizer::Compile(size_t nworkers)
{
	return test_plan(nworkers);
}
