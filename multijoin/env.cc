#include "env.h"
#include "params.h"
#include "hashjoin.h"

using namespace std;

Environment::Environment(int nthreads)
{
  nthreads_ = nthreads;
  nnodes_ = num_numa_nodes();
  nodes_ = (node_t*)malloc(sizeof(node_t) * nnodes_);
  threads_ = (thread_t*)malloc(sizeof(thread_t) * nthreads);
  done_ = false;

  for (int node = 0; node < nnodes_; node++) {
    node_t *n = &nodes_[node];
    n->node_id = node;
    n->nthreads = nthreads / nnodes_; // this is not accurate, unless we assign the threads round robin
    n->groups = (thread_t**)malloc(sizeof(thread_t*) * n->nthreads);
    n->queue = new Taskqueue();
  }

  int node_idx[nnodes_];
  memset(node_idx, 0, sizeof(int) * nnodes_);

  for (int tid = 0; tid < nthreads; tid++) {
    thread_t *t = &threads_[tid];
    int cpu = cpu_of_thread_rr(tid); // round robin
    int node = node_of_cpu(cpu);
    LOG(INFO) << "Thread " << tid << " is assigned to cpu[" << cpu <<"] and node[" << node << "]";

    t->tid = tid;
    t->cpu = cpu;
    t->node_id = node;
    t->node = &nodes_[node];
    t->localtasks = NULL;
    t->stolentasks = NULL;
    t->env = this;

    t->local = 0;
    t->shared = 0;
    t->remote = 0;
    nodes_[node].groups[node_idx[node]++] = t;
  }
}

Environment::~Environment()
{
  for (int node = 0; node < nnodes_; node++) {
    delete nodes_[node].queue;
    free(nodes_[node].groups);
  }

  for (int tid = 0; tid < nthreads_; tid++) {
    if (threads_[tid].localtasks != NULL)
      delete threads_[tid].localtasks;
  }

  free(threads_);
  free(nodes_);
}

// Test Partition Task
void Environment::TestPartition(Table *rt, Table *st)
{
  rt->set_id(0);
  //	st->set_id(1);
  rt->set_type(OpPartition);
  //	st->set_type(OpPartition);

  Table *rparted = new Table(1, OpBuild, nnodes_,
                             Params::kFanoutPass1,
                             nthreads_ * Params::kFanoutPass1);
  // Table *sparted = new Table(3, OpProbe, nnodes_, 
  //                            Params::kFanoutPass1,
  //                            nthreads_ * Params::kFanoutPass1);

  // For Catelog
  AddTable(rt);
  //  AddTable(st);
  AddTable(rparted);
  //  AddTable(sparted);

  for (int node = 0; node < nnodes_; node++) {
    Tasklist *partR = new Tasklist(rt, rparted, rt->id(), rt->id());
    //    Tasklist *partS = new Tasklist(st, sparted, st->id(), st->id());
		
    // create partition task from table R and table S
    list<Partition*>& parts = rt->GetPartitionsByNode(node);
    for (list<Partition*>::iterator it = parts.begin(); 
         it != parts.end(); it++) {
      partR->AddTask(new PartitionTask(OpPartition, *it, rt, rparted, 0, Params::kNumBitsPass1));
    }

    LOG(INFO) << parts.size() << " partitions are added to tasklist on " << node;
    // parts = st->GetPartitionsByNode(node);
    // for (list<Partition*>::iterator it = parts.begin(); 
    //      it != parts.end(); it++) {
    //   partS->AddTask(new PartitionTask(OpPartition, *it, 0, Params::kNumBitsPass1, sparted));
    // }

    Taskqueue *tq = nodes_[node].queue;
    tq->AddList(partR);
    //    tq->AddList(partS);

    tq->Unblock(0);
    //    tq->Unblock(1);

    LOG(INFO) << "Active size:" << tq->active_size();
  }

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

  // For Catelog
  AddTable(rt);
  AddTable(st);
  AddTable(rparted);
  AddTable(sparted);
  AddTable(rhash);
  AddTable(result);

  for (int node = 0; node < nnodes_; node++) {
    Tasklist *partR = new Tasklist(rt, rparted, rt->id(), rt->id());
    Tasklist *partS = new Tasklist(st, sparted, st->id(), st->id());
		
    // create partition task from table R and table S
    list<Partition*>& parts = rt->GetPartitionsByNode(node);
    for (list<Partition*>::iterator it = parts.begin(); 
         it != parts.end(); it++) {
      partR->AddTask(new PartitionTask(OpPartition, *it, rt, rparted, 0, Params::kNumBitsPass1));
    }
    parts = st->GetPartitionsByNode(node);
    for (list<Partition*>::iterator it = parts.begin(); 
         it != parts.end(); it++) {
      partS->AddTask(new PartitionTask(OpPartition, *it, st, sparted, 0, Params::kNumBitsPass1));
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
