#include "builder.h"
#include "optimizer.h"
#include "partition.h"
#include "relop.h"

Plan* test_plan(size_t nworkers)
{
	TableBuilder builder;
	PTable *table = builder.Build(nworkers,1000);

	PTable *hashtable = new PTable;

	for (unsigned int i = 0; i < table->size(); i++) {
		HTPartition *ht = new HTPartition();
		hashtable->AddPartition(ht);
		Partition *p = table->GetPartition(i);
		p->output = ht;
		p->ready = true;
		p->exec = LocalAggregation;
	}

	Plan *plan = new Plan;
	plan->leaves.push_back(table);
	plan->nworkers = nworkers;
	return plan;
}


Plan* Optimizer::Compile(size_t nworkers)
{
	return test_plan(nworkers);
}
