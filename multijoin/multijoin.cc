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

	Taskqueue *queue = args->queue;
	
	while (queue->Empty()) {
		Task *task = queue->Fetch();
		task->Run(args);
		
		delete task; // destroy the task after processing
	}

	return NULL;
}

// two way hash join
void hashjoin(Table *relR, Table *relS, int nthreads) // ncpus?
{
	// init 
	global_t global;
	thread_t *args = (thread_t*)malloc(sizeof(thread_t) * nthreads);

	global.nthreads = nthreads;
	global.nnodes = numa_max_node();
	global.nodes = (node_t*)malloc(sizeof(node_t) * global.nnodes);
	
	thread_t *thread = args;
	int tid = 0;
	for (int i = 0; i < global.nnodes; i++) {
		node_t *node = &global.nodes[i];
		node->nthreads = nthreads / global.nnodes;

		node->locals = thread;
		node->next_node = (i + 1) % global.nnodes;

		for (int j = 0; j < node->nthreads; j++, thread++, tid++) {
			thread->tid = tid;
			thread->node = node;
			thread->global = &global;
		}
	}


	pthread_t threads[nthreads];
	// start threads
	for (int i = 0; i < nthreads; i++) {
		pthread_create(&threads[i], NULL, work_thread, (void*)&args[i]);
	}


	// join threads
	for (int i = 0; i < nthreads; i++) {
		pthread_join(threads[i], NULL);
	}

	// clean up
	free(args);
	free(global.nodes);
}
