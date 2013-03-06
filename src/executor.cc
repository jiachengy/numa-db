#include <glog/logging.h>
#include <pthread.h>
#include "executor.h"

#include "util.h"

void* Executor::execute(void *args)
{
	Executor *self = (Executor*)args;
	cpu_bind(self->cpu_);

	queue<Partition*> *active = self->active_;

	LOG(INFO) << "Thread " << self->cpu_ << " is starting.";

	while (!self->exit_) {
		while (!active->empty()) {
			Partition *current = active->front();
			while (!current->Eop()) {
				Block b = current->Next();
				if (b.empty()) break;
				
				// the exec func decides where to write the output
				current->exec(b, current->output);	
			}

			// check the result
			LocalAggrTable *ht = (LocalAggrTable*)(((HTPartition*)current->output)->hashtable);
			for (int i = 0; i < 1000; i++) {
				LOG(INFO) << ht->Get(i);
			}

			LOG(INFO) << "Thread " << self->cpu_ << " local aggregation done.";
			active->pop();
		}
		// TODO
		// communicate with query engine, try stealing
	}

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
