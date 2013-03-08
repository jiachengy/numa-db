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
size_t block_size = 10000;
bool local = true;
bool local2 = true;
bool roundrobin = false;
int hops = 1;

int cores[4][16] = {
	{0,1,2,3,4,5,6,7,32,33,34,35,36,37,38,39},
    {8,9,10,11,12,13,14,15,40,41,42,43,44,45,46,47},
	{16,17,18,19,20,21,22,23,48,49,50,51,52,53,54,55},
	{24,25,26,27,28,29,30,31,56,57,58,59,60,61,62,63},
};

int rrobin[] = {0,8,16,24,1,9,17,25};
int linear[] = {0,1,2,3,4,5,6,7};

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
	ht->hashtable = new GlobalAggrTable(ht->ngroups_ * inflate);

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

	// if (argv[4][0] == 'r')
	// 	roundrobin = true;
	// else
	// 	roundrobin = false;
	
	if (argv[4][0]=='l')
		local = true;
	else
		local = false;

	// if (argv[6][0]=='l')
	// 	local2 = true;
	// else
	// 	local2 = false;

	// hops = atoi(argv[7]);

	Plan *plan = NULL;
	//	plan = local_plan(nthreads, ntuples, ngroups);

	plan = local_plan(nthreads, ntuples, ngroups);

	qe.Query(plan);
}
