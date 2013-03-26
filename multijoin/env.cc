#include "env.h"
#include "params.h"
#include "hashjoin.h"
#include "builder.h"

using namespace std;

Environment::Environment(int nthreads, size_t memory_limit)
  : nthreads_(nthreads), nnodes_(num_numa_nodes()),
    memlimit_(memory_limit), done_(false)
{
  nodes_ = (node_t*)malloc(sizeof(node_t) * nnodes_);
  threads_ = (thread_t*)malloc(sizeof(thread_t) * nthreads);
  recyclers_ = (Recycler**)malloc(sizeof(Recycler*) * nnodes_);

  for (uint32_t node = 0; node < nnodes_; node++) {
    node_t *n = &nodes_[node];
    n->node_id = node;
    n->nthreads = nthreads / nnodes_; // this is not accurate, unless we assign the threads round robin
    n->groups = (thread_t**)malloc(sizeof(thread_t*) * n->nthreads);
    n->queue = new Taskqueue();
    n->next_node = (node + 1) % nnodes_;
    n->next_cpu = 0;
    pthread_mutex_init(&n->lock, NULL);
  }

#ifdef PRE_ALLOC
  Init();
#endif

  int node_idx[nnodes_];
  memset(node_idx, 0, sizeof(int) * nnodes_);

  for (int tid = 0; tid < nthreads; tid++) {
    thread_t *t = &threads_[tid];
    int cpu = cpu_of_thread_rr(tid); // round robin
    int node = node_of_cpu(cpu);

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

    t->recycler = recyclers_[node];

    nodes_[node].groups[node_idx[node]++] = t;
  }
}

Environment::~Environment()
{
  for (uint32_t node = 0; node < nnodes_; node++) {
    delete nodes_[node].queue;
    free(nodes_[node].groups);
    pthread_mutex_destroy(&nodes_[node].lock);

    delete recyclers_[node];
  }

  free(threads_);
  free(nodes_);
  free(recyclers_);

  LOG(INFO) << "deallocate structure";
  
  // deallocate tables
  int i = 0;
  for (vector<Table*>::iterator it = tables_.begin();
       it != tables_.end(); it++, i++)
    delete *it;

  LOG(INFO) << "deallocate tables";

  // deallocate tasks
  for (vector<Tasklist*>::iterator it = tasks_.begin();
       it != tasks_.end(); it++)
    delete *it;

  LOG(INFO) << "deallocate tasklists";

  for (vector<Tasklist*>::iterator it = probes_.begin();
       it != probes_.end(); it++)
    delete *it;

  LOG(INFO) << "deallocate probes";

}

struct InitArg
{
  int node;
  size_t capacity;
  Recycler *recycler;
};

void*
Environment::init_thread(void *params)
{
  InitArg *args = (InitArg*)params;
  
  node_bind(args->node);
  args->recycler = new Recycler(args->node, args->capacity);
  
  return NULL;
}

void
Environment::Init()
{
  pthread_t threads[nnodes_];
  InitArg args[nnodes_];
  for (uint32_t i = 0; i < nnodes_; i++) {
    args[i].node = i;
    args[i].capacity = memlimit_;
    pthread_create(&threads[i], NULL, &Environment::init_thread, (void*)&args[i]);
  }

  for (uint32_t i = 0; i < nnodes_; i++) {
    pthread_join(threads[i], NULL);
    recyclers_[i] = args[i].recycler;
  }
}


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

  Table *rbuild = new Table(OpProbe, nnodes_, Params::kFanoutPass1, 0);

  Table *result = new Table(OpNone, nnodes_, 
                            Params::kFanoutPass1,
                            nthreads_ * Params::kFanoutPass1);

  // Initialize probe lists
  for (int key = 0; key < Params::kFanoutPass1; ++key)
    probes_.push_back(new Tasklist(sparted, result, ShareLocal));

  build_ = rbuild;

  // Table Catelog
  AddTable(rt);
  AddTable(st);
  AddTable(rparted);
  AddTable(sparted);
  AddTable(rbuild);
  AddTable(result);

  Tasklist *buildR = new Tasklist(rparted, rbuild, ShareGlobal);
  for (int key = 0; key < Params::kFanoutPass1; key++) {
    Task *task = new BuildTask(OpBuild, rparted, rbuild, sparted, key);
    buildR->AddTask(task);
  }
  tasks_.push_back(buildR);

  for (uint32_t node = 0; node < nnodes_; node++) {
    Tasklist *partR = new Tasklist(rt, rparted, ShareNode);
    tasks_.push_back(partR);

    Tasklist *partS = new Tasklist(st, sparted, ShareNode);
    tasks_.push_back(partS);

    // create partition task from table R and table S
    list<Partition*>& pr = rt->GetPartitionsByNode(node);
    for (list<Partition*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      partR->AddTask(new PartitionTask(OpPartition, *it, rt, rparted, 0, Params::kNumBitsPass1));
    }

    list<Partition*>& ps = st->GetPartitionsByNode(node);
    for (list<Partition*>::iterator it = ps.begin(); 
         it != ps.end(); it++) {
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
  }

}


Table*
Environment::BuildTable(size_t sz)
{
  Table *table = new Table(nnodes_, 0);

  TableBuilder tb(recyclers_);
  tb.Build(table, sz, nthreads_);

  return table;
}
