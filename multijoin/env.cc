#include "env.h"
#include "params.h"
#include "hashjoin.h"

using namespace std;

Environment::Environment(int nthreads)
{
	nthreads_ = nthreads;
	nnodes_ = numa_max_node();
	nodes_ = (node_t*)malloc(sizeof(node_t) * nnodes_);
	threads_ = (thread_t*)malloc(sizeof(thread_t) * nthreads);

	for (int node = 0; node < nnodes_; node++) {
		node_t *n = &nodes_[node];
		n->node_id = node;
		n->nthreads = nthreads / nnodes_;
		n->queue = new Taskqueue();
	}

	for (int tid = 0; tid < nthreads; tid++) {
		thread_t *t = &threads_[tid];
		// be careful, we are not doing round robin here
		int cpu = tid % cpus();
		int node = node_of_cpu(cpu);

		t->tid = tid;
		t->cpu = cpu;
		t->node_id = node;
		t->node = &nodes_[node];
		t->localtasks = NULL;
		t->env = this;
	}
}

Environment::~Environment()
{
	for (int node = 0; node < nnodes_; node++) {
		delete nodes_[node].queue;
	}

	for (int tid = 0; tid < nthreads_; tid++) {
		if (threads_[tid].localtasks != NULL)
			delete threads_[tid].localtasks;
	}

	free(threads_);
	free(nodes_);
}


// pass in two built table R and S
void Environment::CreateJoinTasks(Table *rt, Table *st)
{
	rt->set_id(0);
	st->set_id(1);
	rt->set_type(OpPartition);
	st->set_type(OpPartition);

	Table *rparted = new Table(2, OpBuild, nnodes_,
							   Params::kFanoutPass1,
							   nthreads_ * Params::kFanoutPass1);
	Table *sparted = new Table(3, OpProbe, nnodes_, 
							   Params::kFanoutPass1,
							   nthreads_ * Params::kFanoutPass1);
	Table *rhash = new Table(4, OpNone, nnodes_, Params::kFanoutPass1, Params::kFanoutPass1);
	Table *result = new Table(5, OpNone, nnodes_, Params::kFanoutPass1, nthreads_);

	for (int node = 0; node < nnodes_; node++) {
		Tasklist *partR = new Tasklist(rt, rparted, rt->id(), rt->id());
		Tasklist *partS = new Tasklist(st, sparted, st->id(), st->id());
		
		// create partition task from table R and table S
		list<Partition*>& parts = rt->GetPartitionsByNode(node);
		for (list<Partition*>::iterator it = parts.begin(); 
			 it != parts.end(); it++) {
			partR->AddTask(new PartitionTask(*it, 0, Params::kNumBitsPass1, rparted));
		}
		parts = st->GetPartitionsByNode(node);
		for (list<Partition*>::iterator it = parts.begin(); 
			 it != parts.end(); it++) {
			partS->AddTask(new PartitionTask(*it, 0, Params::kNumBitsPass1, sparted));
		}

		Tasklist *buildR = new Tasklist(rparted, rhash, rparted->id(), rparted->id());
		Tasklist *probeS = new Tasklist(sparted, result, sparted->id(), sparted->id());

		Taskqueue *tq = nodes_[node].queue;
		tq->AddList(partR);
		tq->AddList(buildR);
		tq->AddList(partS);
		tq->AddList(probeS);

		tq->Unblock(0);
		tq->Unblock(1);
	}

}
