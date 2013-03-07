#include <glog/logging.h>
#include <pthread.h>
#include "engine.h"
#include "executor.h"

#include "util.h"

extern QueryEngine qe;

void* Executor::execute(void *args)
{
	Executor *self = (Executor*)args;
	cpu_bind(self->cpu_);

	queue<Partition*> *active = self->active_;

	LOG(INFO) << "Thread " << self->cpu_ << " is starting.";

	while (!active->empty()) {
		Partition *current = active->front();
		HTPartition *out = (HTPartition*)current->output;
		out->hashtable = new LocalAggrTable(out->ngroups_ * 4, (out->node+1) % num_numa_nodes());

		long t = micro_time();
		while (!current->Eop()) {
			Block b = current->Next();
			if (b.empty())
				break;

			current->exec(b, current->output);	
		}
		LOG(INFO) << "Time: " << (micro_time() - t) / 1000 << "msec";
		LOG(INFO) << "Thread " << self->cpu_ << " local aggregation done.";
		active->pop();
	}
	// TODO
	// communicate with query engine, try stealing
		
	// indicate the engine, the thread has finished working
	// but only needs to indicate once!
	pthread_mutex_lock(&qe.done_mutex_);
	qe.done_count_++;
	if (qe.done_count_ == qe.nworkers_)
		pthread_cond_signal(&qe.done_cv_);
	pthread_mutex_unlock(&qe.done_mutex_);

	// why spin falls into a dead loop?
	// while (!self->exit_)
	// 	;
	
	LOG(INFO) << "Thread " << self->cpu_ << " is existing!";
	pthread_exit(NULL);
}


void Executor::start()
{
	pthread_create(&thread_, NULL, &Executor::execute, (void*)this);
}


void Executor::stop()
{
	exit_ = true;

	pthread_join(thread_, NULL);
}
