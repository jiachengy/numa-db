#include "env.h"
#include "params.h"
#include "hashjoin.h"
#include "builder.h"

#include "perf.h"

using namespace std;

buffer_t*
buffer_init(int table, int radix, int partitions)
{
  buffer_t* buffer = (buffer_t*)malloc(sizeof(buffer_t));
  buffer->table = table;
  buffer->radix = radix;
  buffer->partitions = partitions;
  buffer->partition = (partition_t**)malloc(sizeof(partition_t*) * partitions);
  memset(buffer->partition, 0x0, sizeof(partition_t*) * partitions);
  return buffer;
}


void
buffer_destroy(buffer_t *buffer)
{
  free(buffer->partition);
  free(buffer);
}


bool
buffer_compatible(buffer_t *buffer, int table, int radix)
{
  return (buffer && buffer->table == table && buffer->radix == radix);
}


Environment::Environment(uint32_t nnodes, uint32_t nthreads, size_t capacity)
  : nthreads_(nthreads), nnodes_(nnodes),
    capacity_(capacity), done_(false)
{
  nodes_ = (node_t*)malloc(sizeof(node_t) * nnodes_);
  threads_ = (thread_t*)malloc(sizeof(thread_t) * nthreads);
  memm_ = (Memory**)malloc(sizeof(Memory*) * nnodes_);

  uint32_t nthreads_per_node = nthreads / nnodes_;
  uint32_t nthreads_lastnode = nthreads - nthreads_per_node * (nnodes_ - 1);

  for (uint32_t node = 0; node < nnodes_; node++) {
    node_t *n = &nodes_[node];
    n->node_id = node;
    n->nthreads = (node == nnodes_ - 1) ? nthreads_lastnode : nthreads_per_node;
    n->groups = (thread_t**)malloc(sizeof(thread_t*) * n->nthreads);
    n->queue = new Taskqueue();
    n->next_node = (node + 1) % nnodes_;
    n->next_cpu = 0;
    pthread_mutex_init(&n->lock, NULL);
  }

  Init();
  
  int tid = 0;
  for (uint32_t nid = 0; nid < nnodes_; ++nid) {
    node_t *node = &nodes_[nid];
    for (uint32_t t = 0; t < nodes_[nid].nthreads; t++, tid++) {
      thread_t *thread = &threads_[tid];
      int cpu = cpu_of_node(nid, t); // round robin
      thread->tid = tid;
      thread->tid_of_node = t;
      thread->cpu = cpu;
      thread->node_id = nid;
      thread->node = node;
      thread->batch_task = NULL;
      thread->localtasks = NULL;
      thread->stolentasks = NULL;
      thread->env = this;
      thread->memm = memm_[nid];

      thread->local = 0;
      thread->shared = 0;
      thread->remote = 0;

      thread->buffer = NULL;

      node->groups[t] = thread;
    }
  }

  /*
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
  */
}

Environment::~Environment()
{
  for (uint32_t node = 0; node < nnodes_; node++) {
    delete nodes_[node].queue;
    free(nodes_[node].groups);
    pthread_mutex_destroy(&nodes_[node].lock);

    delete memm_[node];
  }

  free(threads_);
  free(nodes_);
  free(memm_);
  
  // deallocate tables
  int i = 0;
  for (vector<Table*>::iterator it = tables_.begin();
       it != tables_.end(); it++, i++)
    delete *it;

  // deallocate tasks
  for (vector<Tasklist*>::iterator it = tasks_.begin();
       it != tasks_.end(); it++)
    delete *it;

  for (vector<Tasklist*>::iterator it = probes_.begin();
       it != probes_.end(); it++)
    delete *it;
}

struct InitArg
{
  int node;
  size_t capacity;
  Memory *memm;
};

void*
Environment::init_thread(void *params)
{
  InitArg *args = (InitArg*)params;
  
  node_bind(args->node);
  args->memm = new Memory(args->node, args->capacity);
  
  return NULL;
}

void
Environment::Init()
{
  pthread_t threads[nnodes_];
  InitArg args[nnodes_];
  for (uint32_t i = 0; i < nnodes_; i++) {
    args[i].node = i;
    args[i].capacity = capacity_;
    pthread_create(&threads[i], NULL, &Environment::init_thread, (void*)&args[i]);
  }

  for (uint32_t i = 0; i < nnodes_; i++) {
    pthread_join(threads[i], NULL);
    memm_[i] = args[i].memm;
  }
}

void
Environment::Reset()
{
  Table::ResetId();
  
  tables_.clear();
  tasks_.clear();

  for (uint32_t node = 0; node < nnodes_; node++) {
    nodes_[node].queue = new Taskqueue();
  }

  for (uint32_t t = 0; t < nthreads_; t++) {
    threads_[t].batch_task = NULL;
    threads_[t].localtasks = NULL;
    threads_[t].stolentasks = NULL;
    threads_[t].local = 0;
    threads_[t].shared = 0;
    threads_[t].remote = 0;
  }

  done_ = false;
}


void
Environment::RadixPartition(relation_t *rel)
{
  Table *rt = Table::BuildTableFromRelation(rel);
  rt->set_type(OpPartition);

  Table *pass1tb = new Table(OpNone, nnodes_,
                             Params::kFanoutPass1);

  // Table Catelog
  AddTable(rt);
  AddTable(pass1tb);

  for (uint32_t node = 0; node < nnodes_; node++) {
    Tasklist *pass1tasks = new Tasklist(rt, pass1tb, ShareNode);
    tasks_.push_back(pass1tasks);

    Taskqueue *tq = nodes_[node].queue;
    tq->AddList(pass1tasks);
    tq->Unblock(pass1tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      pass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1, NULL));
    }
  }
}



void
Environment::TwoPassPartition(relation_t *relR)
{
  Table *rt = Table::BuildTableFromRelation(relR);
  rt->set_type(OpPartition);

  Table *rpass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);

  Table *rpass2tb = new Table(OpNone, nnodes_, Params::kFanoutTotal);


  // Table Catelog
  AddTable(rt);
  AddTable(rpass1tb);
  AddTable(rpass2tb);

  // Global accessible p2tasks
  P2Task ***p2tasksR = (P2Task***)malloc(sizeof(P2Task**) * nnodes_);
  for (uint32_t node = 0; node < nnodes_; ++node) {
    P2Task **p2_tasks_on_node = (P2Task**)malloc(sizeof(P2Task*) * Params::kFanoutPass1);
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      p2_tasks_on_node[key] = new P2Task(key, rpass1tb, rpass2tb);
    }
    p2tasksR[node] = p2_tasks_on_node;
  }


  for (uint32_t node = 0; node < nnodes_; node++) {
    Taskqueue *tq = nodes_[node].queue;

    Tasklist *rpass1tasks = new Tasklist(rt, rpass1tb, ShareNode);
    tasks_.push_back(rpass1tasks);

    Tasklist *rpass2tasks = new Tasklist(rpass1tb, rpass2tb, ShareNode);    
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      rpass2tasks->AddTask(p2tasksR[node][key]);
    }

    tasks_.push_back(rpass2tasks);

    tq->AddList(rpass1tasks);
    tq->AddList(rpass2tasks);
    tq->Unblock(rpass1tasks->id());
    tq->Unblock(rpass2tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      rpass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1, p2tasksR));
    }
  }
}


void
Environment::TwoPassPartition(relation_t *relR, relation_t *relS)
{
  Table *rt = Table::BuildTableFromRelation(relR);
  rt->set_type(OpPartition);

  Table *rpass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);


  Table *rpass2tb = new Table(OpBuild, nnodes_, Params::kFanoutTotal);


  Table *st = Table::BuildTableFromRelation(relS);
  st->set_type(OpPartition);

  Table *spass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);

  Table *spass2tb = new Table(OpNone, nnodes_, Params::kFanoutTotal);


  // Table Catelog
  AddTable(rt);
  AddTable(rpass1tb);
  AddTable(rpass2tb);
  AddTable(st);
  AddTable(spass1tb);
  AddTable(spass2tb);

  // Global accessible p2tasks
  P2Task ***p2tasksR = (P2Task***)malloc(sizeof(P2Task**) * nnodes_);
  P2Task ***p2tasksS = (P2Task***)malloc(sizeof(P2Task**) * nnodes_);
  for (uint32_t node = 0; node < nnodes_; ++node) {
    P2Task **p2_tasks_on_nodeR = (P2Task**)malloc(sizeof(P2Task*) * Params::kFanoutPass1);
    P2Task **p2_tasks_on_nodeS = (P2Task**)malloc(sizeof(P2Task*) * Params::kFanoutPass1);
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      p2_tasks_on_nodeR[key] = new P2Task(key, rpass1tb, rpass2tb);
      p2_tasks_on_nodeS[key] = new P2Task(key, spass1tb, spass2tb);
    }
    p2tasksR[node] = p2_tasks_on_nodeR;
    p2tasksS[node] = p2_tasks_on_nodeS;
  }


  for (uint32_t node = 0; node < nnodes_; node++) {
    Taskqueue *tq = nodes_[node].queue;

    Tasklist *rpass1tasks = new Tasklist(rt, rpass1tb, ShareNode);
    Tasklist *rpass2tasks = new Tasklist(rpass1tb, rpass2tb, ShareNode);    
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      rpass2tasks->AddTask(p2tasksR[node][key]);
    }
    tasks_.push_back(rpass1tasks);
    tasks_.push_back(rpass2tasks);

    Tasklist *spass1tasks = new Tasklist(st, spass1tb, ShareNode);
    Tasklist *spass2tasks = new Tasklist(spass1tb, spass2tb, ShareNode);
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      spass2tasks->AddTask(p2tasksS[node][key]);
    }
    tasks_.push_back(spass1tasks);
    tasks_.push_back(spass2tasks);

    tq->AddList(rpass1tasks);
    tq->AddList(rpass2tasks);
    tq->AddList(spass1tasks);
    tq->AddList(spass2tasks);
    tq->Unblock(rpass1tasks->id());
    tq->Unblock(rpass2tasks->id());
    tq->Unblock(spass1tasks->id());
    tq->Unblock(spass2tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      rpass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1, p2tasksR));
    }

    //    create partition task from table S
    list<partition_t*>& ps = st->GetPartitionsByNode(node);
    for (list<partition_t*>::iterator it = ps.begin(); 
         it != ps.end(); it++) {
      spass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1, p2tasksS));
    }
  }
}
