#ifndef MULTIJOIN_H_
#define MULTIJOIN_H_

typedef struct global_t global_t;
typedef struct node_t node_t;
typedef struct thread_t thread_t;

#include "taskqueue.h"

struct global_t {
	int nthreads;
	int nnodes;
	node_t *nodes; // all nodes structure
	thread_t *threads;
};

struct node_t {
	int nthreads;
	thread_t *locals; // group of threads on that node
	int next_node; // next node to steal
};


// a thread should have its own queue
// because it will have different orders for build and probe
struct thread_t {
	int tid;
	Taskqueue *queue;

	node_t *node; // pointer to local node info
	global_t *global; // pointer to global info

	int next_cpu; // next cpu to steal
};

#endif // MULTIJOIN_H_
