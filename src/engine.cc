#include <glog/logging.h>

#include "util.h"
#include "engine.h"

void QueryEngine::initpipe(Plan *plan)
{
	for (unsigned int i = 0; i < plan->leaves.size(); i++) {
		PTable *t = plan->leaves[i];
		for (unsigned int pid = 0; pid < t->size(); pid++) {
			Partition *p = t->GetPartition(pid);
			actives_[p->cpu].push(p);
		}
	}
}

void QueryEngine::start()
{
	for (unsigned int i = 0; i < nworkers_; i++) {
		executors_[i].cpu_ = i;
		executors_[i].active_ = &actives_[i];
		executors_[i].start();
	}
}

void QueryEngine::stop()
{
	for (unsigned int i = 0; i < nworkers_; i++) {
		executors_[i].stop();
	}
}

void QueryEngine::Query(Plan *plan)
{
	nworkers_ = plan->nworkers;
	actives_ = new queue<Partition*>[nworkers_];
	blocked_ = new queue<Partition*>[nworkers_];
	executors_ = new Executor[nworkers_];
	done_count_ = 0;
	pthread_mutex_init(&done_mutex_, NULL);
	pthread_cond_init(&done_cv_, NULL);

	// 1. fill the pipeline with the leaf level of the plan
	initpipe(plan);

	long t = micro_time();

	// 2. start workers
	start();

	// 3. synchronize
	// blocked by the finish signal of the output
	pthread_mutex_lock(&done_mutex_);
	while (done_count_ < nworkers_)
		pthread_cond_wait(&done_cv_, &done_mutex_);
	pthread_mutex_unlock(&done_mutex_);

	stop();
	LOG(INFO) << "Eclapsed: " << (micro_time() - t) / 1000 << " msec";

	pthread_mutex_destroy(&done_mutex_);
	pthread_cond_destroy(&done_cv_);
}

