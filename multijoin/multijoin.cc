#include <pthread.h>
#include <numa.h>

#include "types.h"
#include "taskqueue.h"
#include "util.h"
#include "multijoin.h"

void* work_thread(void *param)
{
	thread_t *args = (thread_t*)param;
	int tid = args->tid;
	cpu_bind(tid);

	// node shared queue
	Taskqueue *queue = args->node->queue;
	
	while (queue->Empty()) {
		Task *task = queue->Fetch();
		task->Run(args);
		
		delete task; // destroy the task after processing
	}

	return NULL;
}


// two way hash join
void Hashjoin(int nthreads)
{
	// init the environment
	Environment *env = new Environment(nthreads);


	// init the task queues
	env->CreateJoinTasks();


	pthread_t threads[nthreads];
	// start threads
	for (int i = 0; i < nthreads; i++) {
		pthread_create(&threads[i], NULL, work_thread, (void*)&env->threads[i]);
	}

	// join threads
	for (int i = 0; i < nthreads; i++) {
		pthread_join(threads[i], NULL);
	}

	delete env;
}
