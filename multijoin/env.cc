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

  LOG(INFO) << "structure deallocate done.";

  // deallocate tables
  for (vector<Table*>::iterator it = tables_.begin();
       it != tables_.end(); it++) {
    LOG(INFO) << "table id: " << (*it)->id();
    delete *it;
  }

  LOG(INFO) << "tables deallocate done.";


  // deallocate tasks
  for (vector<Tasklist*>::iterator it = tasks_.begin();
       it != tasks_.end(); it++) {
    delete *it;
  }

  LOG(INFO) << "tasks deallocate done.";
}

// Test Partition Task
void Environment::CreateJoinTasks(Table *rt, Table *st)
{
  rt->set_type(OpPartition);
  st->set_type(OpPartition);

  Table *rparted = new Table(OpBuild, nnodes_,
                             Params::kFanoutPass1,
                             nthreads_ * Params::kFanoutPass1);
  Table *sparted = new Table(OpProbe, nnodes_, 
                             Params::kFanoutPass1,
                             nthreads_ * Params::kFanoutPass1);

  Table *rhash = new Table(OpProbe, nnodes_, Params::kFanoutPass1, 0);

  Table *result = new Table(OpNone, nnodes_, 
                            Params::kFanoutPass1,
                            nthreads_ * Params::kFanoutPass1);

  // For Catelog
  AddTable(rt);
  AddTable(st);
  AddTable(rparted);
  AddTable(sparted);
  AddTable(rhash);
  AddTable(result);

  // the hash tasks are shared by all threads, and we can init the tasks in advance
  // how do we deallocate this list????
  Tasklist *buildR = new Tasklist(rparted, rhash, ShareGlobal);
  for (int key = 0; key < Params::kFanoutPass1; key++) {
    Task *task = new BuildTask(OpBuild, rparted, rhash, sparted, result, key);
    buildR->AddTask(task);
  }
  tasks_.push_back(buildR);

  for (int node = 0; node < nnodes_; node++) {
    Tasklist *partR = new Tasklist(rt, rparted, ShareNode);
    tasks_.push_back(partR);

    Tasklist *partS = new Tasklist(st, sparted, ShareNode);
    tasks_.push_back(partS);

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

    Tasklist *probeS = new Tasklist(sparted, result, ShareNode);
    tasks_.push_back(probeS);

    Taskqueue *tq = nodes_[node].queue;
    tq->AddList(partR);
    tq->AddList(partS);
    tq->AddList(buildR);
    tq->AddList(probeS);

    tq->Unblock(partR->id());
    tq->Unblock(partS->id());
    //    tq->Unblock(probeS->id()); // although probing is unblocked, but it is always empty until the first hash table is built

    LOG(INFO) << "Active size:" << tq->active_size();
  }

}
