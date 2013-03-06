#include <glog/logging.h>
#include <pthread.h>
#include "executor.h"

#include "util.h"

void* Executor::execute(void *args)
{
	Executor *self = (Executor*)args;
	cpu_bind(self->cpu_);

	queue<Partition*> *active = self->active_;

	while (!self->exit_) {
		while (!active->empty()) {
			Partition *current = active->front();

			// check whether we need a new output partition
			// but actually it depends on the algorithm
			// so only the exec knows it
			// let the exec worries about it.

			while (!current->Eop()) {
				Block b = current->Next();
				if (b.empty()) break;
				current->exec(b, current->output);	
			}
		}
		
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
