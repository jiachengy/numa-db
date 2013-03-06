#include <glog/logging.h>

#include "engine.h"

void QueryEngine::initpipe(Plan *plan)
{
	for (int i = 0; i < plan->leaves.size(); i++) {
		PTable *t = plan->leaves[i];
		for (int pid = 0; pid < t->size(); pid++) {
			Partition *p = t->GetPartition(pid);
			actives_[p->cpu].push(p);
		}
	}
}

void QueryEngine::start()
{
	for (int i = 0; i < nworkers_; i++) {
		executors_[i].cpu_ = i;
		executors_[i].active_ = &actives_[i];
		executors_[i].start();
	}
}

void QueryEngine::stop()
{
	for (int i = 0; i < nworkers_; i++) {
		executors_[i].stop();
	}
}

void QueryEngine::Query(Plan *plan)
{
	nworkers_ = plan->nworkers;
	actives_ = new queue<Partition*>[nworkers_];
	blocked_ = new queue<Partition*>[nworkers_];
	executors_ = new Executor[nworkers_];

	// 1. fill the pipeline with the leaf level of the plan
	initpipe(plan);

	// 2. start workers
	start();

	// 3. synchronize
	// blocked by the finish signal of the output
	
	stop();
}

