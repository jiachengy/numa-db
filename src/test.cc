#include <stdlib.h>
#include <iostream>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "numadb.h"
#include "builder.h"
#include "plan.h"
#include "optimizer.h"
#include "engine.h"
#include "atomic.h"
#include "relop.h"
#include "util.h"

using namespace std;

QueryEngine qe;

size_t ngroups = 0;
size_t block_size = 0;
bool local = true;

Plan* local_plan(size_t nworkers, size_t ntuples, size_t ngroups)
{
	TableBuilder builder;
	PTable *table = builder.Build(nworkers, ntuples / nworkers);

	PTable *hashtable = new PTable;

	for (unsigned int i = 0; i < table->size(); i++) {
		HTPartition *ht = new HTPartition(node_of_cpu(i), i, ngroups);
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

Plan* global_plan(size_t nworkers, size_t ntuples, size_t ngroups)
{
	TableBuilder builder;
	PTable *table = builder.Build(nworkers, ntuples / nworkers);

	PTable *hashtable = new PTable;
	HTPartition *ht = new HTPartition(-1, -1, ngroups);
	hashtable->AddPartition(ht);
	
	for (unsigned int i = 0; i < table->size(); i++) {
		Partition *p = table->GetPartition(i);
		p->output = ht;
		p->ready = true;
		p->exec = GlobalAggregation;
	}

	Plan *plan = new Plan;
	plan->leaves.push_back(table);
	plan->nworkers = nworkers;
	return plan;
}


int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	FLAGS_logtostderr = true;

	size_t nthreads = atoi(argv[1]);
	size_t ntuples = atoi(argv[2]);
	ngroups = atoi(argv[3]);
	block_size = atoi(argv[4]);	
	
	if (argv[5][0]=='l')
		local = true;
	else
		local = false;


	Plan *plan = NULL;
	plan = local_plan(nthreads, ntuples, ngroups);

	qe.Query(plan);
}
