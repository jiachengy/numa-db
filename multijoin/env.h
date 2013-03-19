#ifndef ENV_H_
#define ENV_H_

#include <numa.h>
#include <map>

class Environment;

typedef struct node_t node_t;
typedef struct thread_t thread_t;

#include "taskqueue.h"
#include "table.h"

struct node_t {
	int node_id;
	int nthreads;
	thread_t *locals; // group of threads on that node

	Taskqueue *queue;
	int next_node; // next node to steal
};


struct thread_t {
	int tid;
	Tasklist *localtasks;

	node_t *node; // pointer to local node info
	Environment *env; // pointer to global info
};



class Environment
{
 private:
	// general info
	int nthreads_;
	int nnodes_;

	// node and thread info
	node_t *nodes_; // all nodes structure
	thread_t *threads_;

	// table info
	std::vector<Table*> tables_;	
 public:
	Environment(int nthreads) {
		nthreads_ = nthreads;
		nnodes_ = numa_max_node();
		nodes_ = (node_t*)malloc(sizeof(node_t) * nnodes_);
		threads_ = (thread_t*)malloc(sizeof(thread_t) * nthreads);

		// be careful about thread - node binding	
		thread_t *thread = threads_;
		int tid = 0;
		for (int i = 0; i < env->nnodes; i++) {
			node_t *node = &env->nodes[i];
			node->nthreads = nthreads / env->nnodes;
			node->locals = thread;
			node->next_node = (i + 1) % env->nnodes;
			node->queue = new Taskqueue();

			for (int j = 0; j < node->nthreads; j++, thread++, tid++) {
				thread->tid = tid;
				thread->node = node;
				thread->localtasks = NULL;
				thread->env = env;
			}
		}
	}

	~Environment() {
		free(threads_);
		free(nodes_);
	}


	void CreateJoinTasks() {
		// create table structures
		Table *rt = new Table(0, OpPartition, 0);
		Table *st = new Table(1, OpPartition, 0);
		// Build tables
		rt->Build();
		st->Build();

		Table *rparted = new Table(2, OpBuild, nthreads * NPARTS);
		Table *sparted = new Table(3, OpProbe, nthreads * NPARTS);
		Table *rhash = new Table(4, OpNone, nthreads_);
		Table *result = new Table(5, OpNone, nthreads_);

		for (int node = 0; node < env->nnodes; node++) {
			Tasklist *partR = new Tasklist(rt, rparted, 0);
			Tasklist *partS = new Tasklist(st, sparted, 1);
		
			// create partition task from table R and table S
			std::list<Partition*>& parts = rt->GetPartitions(node);
			for (int i = 0; i < parts.size(); i++) {
				partR->AddTask(new PartitionTask(parts[i], OFFSET, NBITS, rparted));
			}
			parts = st->GetPartitions(node);
			for (int i = 0; i < parts.size(); i++) {
				partS->AddTask(new PartitionTask(parts[i], OFFSET, NBITS, sparted));
			}

			Tasklist *buildR = new Tasklist(rparted, rhash, 2);
			Tasklist *probeS = new Tasklist(sparted, result, 3);

			Taskqueue *tq = env->nodes[node].queue;
			tq->AddList(partR);
			tq->AddList(buildR);
			tq->AddList(partS);
			tq->AddList(probeS);

			tq->Unblock(partR);
			tq->Unblock(partS);
		}

	}


};

#endif // ENV_H_
