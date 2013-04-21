#include "env.h"
#include "params.h"
#include "hashjoin.h"
#include "builder.h"

#include "perf.h"

using namespace std;

buffer_t*
buffer_init(Table * table, int radix, int partitions)
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
buffer_compatible(buffer_t *buffer, Table * table, int radix)
{
  return (buffer && buffer->table == table && buffer->radix == radix);
}


Environment::Environment(uint32_t nnodes, uint32_t nthreads)
  : nthreads_(nthreads), nnodes_(nnodes), done_(false)
{
  nodes_ = (node_t*)malloc(sizeof(node_t) * nnodes_);
  threads_ = (thread_t*)malloc(sizeof(thread_t) * nthreads);
  queries_ = 0;

  uint32_t nthreads_per_node = nthreads / nnodes_;
  uint32_t nthreads_lastnode = nthreads - nthreads_per_node * (nnodes_ - 1);

  for (uint32_t node = 0; node != nnodes_; ++node) {
    node_t *n = &nodes_[node];
    n->node_id = node;
    n->nthreads = (node == nnodes_ - 1) ? nthreads_lastnode : nthreads_per_node;
    n->groups = (thread_t**)malloc(sizeof(thread_t*) * n->nthreads);
    n->queue = new Taskqueue();
    n->next_node = (node + 1) % nnodes_;
    n->next_cpu = 0;
    pthread_mutex_init(&n->lock, NULL);
  }

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

      thread->local = 0;
      thread->shared = 0;
      thread->remote = 0;

      thread->buffer = NULL;
      pthread_mutex_init(&thread->lock, NULL);
      thread->stage_counter = (perf_counter_t*)malloc(STAGES * sizeof(perf_counter_t));
      for (int i = 0; i != STAGES; ++i)
        thread->stage_counter[i] = PERF_COUNTER_INITIALIZER;
      thread->total_counter = PERF_COUNTER_INITIALIZER;
      node->groups[t] = thread;
    }
  }

  InitMem();
}

Environment::~Environment()
{
  for (uint32_t node = 0; node < nnodes_; node++) {
    delete nodes_[node].queue;
    free(nodes_[node].groups);
    pthread_mutex_destroy(&nodes_[node].lock);
  }

  for (uint32_t tid = 0; tid != nthreads_; tid++)
    pthread_mutex_destroy(&threads_[tid].lock);

  free(threads_);
  free(nodes_);
  
  // deallocate tables
  int i = 0;
  for (vector<Table*>::iterator it = tables_.begin();
       it != tables_.end(); it++, i++)
    delete *it;

  // deallocate tasks
  for (vector<Tasklist*>::iterator it = tasks_.begin();
       it != tasks_.end(); it++)
    delete *it;
}

void*
init_thread_mem(void *params)
{
  uint32_t partitions = 1024 * 1024 * 16 / Params::kFanoutTotal;
  NEXT_POW_2(partitions);
  partitions >>= 2;
   //  partitions >>= 1; // make it larger than needed

  thread_t *args = (thread_t*)params;
  cpu_bind(args->cpu);
#ifdef COLUMN_WISE
  args->wc_buf_key = (cache_line_t*)alloc_aligned(Params::kFanoutPass2 * sizeof(cache_line_t), CACHE_LINE_SIZE);
  args->wc_part_key = (intkey_t**)malloc(Params::kFanoutPass2 * sizeof(intkey_t*));
  args->wc_buf_value = (cache_line_t*)alloc_aligned(Params::kFanoutPass2 * sizeof(cache_line_t), CACHE_LINE_SIZE);
  args->wc_part_value = (value_t**)malloc(Params::kFanoutPass2 * sizeof(value_t*));
  args->part_key = (intkey_t**)malloc(partitions * sizeof(intkey_t*));
  args->part_value = (value_t**)malloc(partitions * sizeof(value_t*));
#else
  args->wc_buf = (cache_line_t*)alloc_aligned(Params::kFanoutPass2 * sizeof(cache_line_t), CACHE_LINE_SIZE);
  args->wc_part = (tuple_t**)malloc(Params::kFanoutPass2 * sizeof(tuple_t*));
  args->part = (tuple_t**)malloc(partitions * sizeof(tuple_t*));
#endif

  args->wc_count = (uint32_t*)malloc(Params::kFanoutPass2 * sizeof(uint32_t));
  args->hist = (uint32_t*)malloc(partitions * sizeof(uint32_t));


  args->memm[0] = new Memory(args->node_id, gConfig.mem_per_thread,
                             Params::kMaxTuples, 0);
  args->memm[1] = new Memory(args->node_id, gConfig.mem_per_thread,
                             Params::kSmallMaxTuples, 1024*1024*128);

  return NULL;
}


void
Environment::InitMem()
{
  pthread_t threads[nthreads_];
  for (uint32_t t = 0; t != nthreads_; t++)
    pthread_create(&threads[t], NULL, init_thread_mem, (void*)&threads_[t]);

  for (uint32_t t = 0; t != nthreads_; t++)
    pthread_join(threads[t], NULL);
}


void
Environment::RadixPartition(relation_t *rel)
{
  ++queries_;

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
    logging("Partitions on node[%d]: %d\n", node, pr.size());
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      pass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }
  }
}



void
Environment::TwoPassPartition(relation_t *relR)
{
  ++queries_;

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
  AddP2Tasks(p2tasksR, rpass1tb->id());
  for (uint32_t node = 0; node != nnodes_; ++node) {
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
    // do not activate them here
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      //           rpass2tasks->AddTask(p2tasksR[node][key]);
      //           p2tasksR[node][key]->set_schedule(true);
    }

    tasks_.push_back(rpass2tasks);

    tq->AddList(rpass1tasks);
    tq->AddList(rpass2tasks);
    tq->Unblock(rpass1tasks->id());
    //    tq->Unblock(rpass2tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    logging("Partitions on node[%d]: %d\n", node, pr.size());
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      rpass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }
  }
}


void
Environment::TwoPassPartition(relation_t *relR, relation_t *relS)
{
  queries_ += 2;

  Table *rt = Table::BuildTableFromRelation(relR);
  rt->set_type(OpPartition);
  Table *rpass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);
  Table *rpass2tb = new Table(OpNone, nnodes_, Params::kFanoutTotal);

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
  AddP2Tasks(p2tasksR, rpass1tb->id());
  AddP2Tasks(p2tasksS, spass1tb->id());
  for (uint32_t node = 0; node != nnodes_; ++node) {
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
    tasks_.push_back(rpass1tasks);

    Tasklist *spass1tasks = new Tasklist(st, spass1tb, ShareNode);
    tasks_.push_back(spass1tasks);

    Tasklist *rpass2tasks = new Tasklist(rpass1tb, rpass2tb, ShareNode);    
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      // rpass2tasks->AddTask(p2tasksR[node][key]);
      // p2tasksR[node][key]->schedule();
    }
    tasks_.push_back(rpass2tasks);

    Tasklist *spass2tasks = new Tasklist(spass1tb, spass2tb, ShareNode);    
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      // spass2tasks->AddTask(p2tasksS[node][key]);
      // p2tasksS[node][key]->schedule();
    }
    tasks_.push_back(spass2tasks);


    tq->AddList(rpass1tasks);
    tq->AddList(rpass2tasks);
    tq->Unblock(rpass1tasks->id());
    tq->Unblock(rpass2tasks->id());
    tq->AddList(spass1tasks);
    tq->AddList(spass2tasks);
    tq->Unblock(spass1tasks->id());
    tq->Unblock(rpass2tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    logging("Partitions on node[%d]: %d\n", node, pr.size());
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      rpass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }

    list<partition_t*>& ps = st->GetPartitionsByNode(node);
    logging("Partitions on node[%d]: %d\n", node, ps.size());
    for (list<partition_t*>::iterator it = ps.begin(); 
         it != ps.end(); it++) {
      spass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }
  }

}




void
Environment::PartitionAndBuild(relation_t *relR)
{
  ++queries_;

  Table *rt = Table::BuildTableFromRelation(relR);
  rt->set_type(OpPartition);

  Table *rpass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);
  Table *rpass2tb = new Table(OpBuild, nnodes_, Params::kFanoutTotal);
  Table *rbuild = new Table(OpNone, nnodes_, Params::kFanoutTotal);

  // Table Catelog
  AddTable(rt);
  AddTable(rpass1tb);
  AddTable(rpass2tb);
  AddTable(rbuild);

  // Global accessible p2tasks
  P2Task ***p2tasksR = (P2Task***)malloc(sizeof(P2Task**) * nnodes_);
  AddP2Tasks(p2tasksR, rpass1tb->id());
  for (uint32_t node = 0; node != nnodes_; ++node) {
    P2Task **p2_tasks_on_node = (P2Task**)malloc(sizeof(P2Task*) * Params::kFanoutPass1);
    for (int key = 0; key < Params::kFanoutPass1; ++key)
      p2_tasks_on_node[key] = new P2Task(key, rpass1tb, rpass2tb);
    p2tasksR[node] = p2_tasks_on_node;
  }

  Tasklist *buildR = new Tasklist(rpass2tb, rbuild, ShareGlobal);
  for (int i = 0; i != Params::kFanoutTotal; ++i)
    buildR->AddTask(new BuildTask(NULL, i));

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
    tq->AddList(buildR);
    tq->Unblock(rpass1tasks->id());
    //    tq->Unblock(rpass2tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    logging("Partitions on node[%d]: %d\n", node, pr.size());
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      rpass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }
  }
}


void
Environment::Hashjoin(relation_t *relR, relation_t *relS)
{
  queries_ += 1;

  Table *rt = Table::BuildTableFromRelation(relR);
  rt->set_type(OpPartition);

#ifdef TWO_PASSES
  Table *rpass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);
  Table *rpass2tb = new Table(OpBuild, nnodes_, Params::kFanoutTotal);
  logging("two passes\n");
#else
  Table *rpass1tb = new Table(OpBuild, nnodes_, Params::kFanoutPass1);
#endif

  Table *st = Table::BuildTableFromRelation(relS);
  st->set_type(OpPartition);

#ifdef TWO_PASSES
  Table *spass1tb = new Table(OpPartition2, nnodes_, Params::kFanoutPass1);
  Table *spass2tb = new Table(OpProbe, nnodes_, Params::kFanoutTotal);
#else
  Table *spass1tb = new Table(OpProbe, nnodes_, Params::kFanoutPass1);
#endif

  Table *rbuild = new Table(OpProbe, nnodes_, Params::kFanoutTotal);

  Table *result = new Table(OpNone, nnodes_, Params::kFanoutTotal);


  // Table Catelog
  AddTable(rt);
  AddTable(rpass1tb);
#ifdef TWO_PASSES
  AddTable(rpass2tb);
#endif
  AddTable(rbuild);
  AddTable(st);
  AddTable(spass1tb);
#ifdef TWO_PASSES
  AddTable(spass2tb);
#endif
  AddTable(result);
  build_ = rbuild;

#ifdef TWO_PASSES  
  // Global accessible p2tasks
  P2Task ***p2tasksR = (P2Task***)malloc(sizeof(P2Task**) * nnodes_);
  P2Task ***p2tasksS = (P2Task***)malloc(sizeof(P2Task**) * nnodes_);
  AddP2Tasks(p2tasksR, rpass1tb->id());
  AddP2Tasks(p2tasksS, spass1tb->id());
  for (uint32_t node = 0; node != nnodes_; ++node) {
    P2Task **p2_tasks_on_nodeR = (P2Task**)malloc(sizeof(P2Task*) * Params::kFanoutPass1);
    P2Task **p2_tasks_on_nodeS = (P2Task**)malloc(sizeof(P2Task*) * Params::kFanoutPass1);
    for (int key = 0; key < Params::kFanoutPass1; ++key) {
      p2_tasks_on_nodeR[key] = new P2Task(key, rpass1tb, rpass2tb);
      p2_tasks_on_nodeS[key] = new P2Task(key, spass1tb, spass2tb);
    }
    p2tasksR[node] = p2_tasks_on_nodeR;
    p2tasksS[node] = p2_tasks_on_nodeS;
  }
#endif
  Table *build_table, *probe_table;
#ifdef TWO_PASSES  
  build_table = rpass2tb;
  probe_table = spass2tb;
#else
  build_table = rpass1tb;
  probe_table = spass1tb;
#endif  

  // build tasks
  Tasklist *buildR = new Tasklist(build_table, rbuild, ShareGlobal);
  for (int i = 0; i != Params::kFanoutTotal; ++i)
    buildR->AddTask(new BuildTask(probe_table, i));

  // probe tasks
  ProbeTask **probetasks = (ProbeTask**)malloc(sizeof(ProbeTask*) * Params::kFanoutTotal);
  for (int radix = 0; radix != Params::kFanoutTotal; ++radix) {
    probetasks[radix] = new ProbeTask(probe_table, result, radix);
  }
  AddProbeTasks(probetasks, probe_table->id());


  for (uint32_t node = 0; node < nnodes_; node++) {
    Taskqueue *tq = nodes_[node].queue;

    Tasklist *rpass1tasks = new Tasklist(rt, rpass1tb, ShareNode);
    tasks_.push_back(rpass1tasks);

    Tasklist *spass1tasks = new Tasklist(st, spass1tb, ShareNode);
    tasks_.push_back(spass1tasks);

#ifdef TWO_PASSES
    Tasklist *rpass2tasks = new Tasklist(rpass1tb, rpass2tb, ShareNode);    
    for (int key = 0; key < Params::kFanoutPass1; ++key)
      rpass2tasks->AddTask(p2tasksR[node][key]);
    tasks_.push_back(rpass2tasks);

    Tasklist *spass2tasks = new Tasklist(spass1tb, spass2tb, ShareNode);    
    for (int key = 0; key < Params::kFanoutPass1; ++key)
      spass2tasks->AddTask(p2tasksS[node][key]);
    tasks_.push_back(spass2tasks);
#endif

    Tasklist *probetasks = new Tasklist(probe_table, result, ShareNode);

    tq->AddList(rpass1tasks);
#ifdef TWO_PASSES
    tq->AddList(rpass2tasks);
#endif
    tq->AddList(spass1tasks);
#ifdef TWO_PASSES
    tq->AddList(spass2tasks);
#endif
    tq->AddList(buildR);
    tq->AddList(probetasks);
    tq->Unblock(rpass1tasks->id());
    //    tq->Unblock(rpass2tasks->id());
    tq->Unblock(spass1tasks->id());
    //    tq->Unblock(spass2tasks->id());

    // create partition task from table R
    list<partition_t*>& pr = rt->GetPartitionsByNode(node);
    logging("Partitions on node[%d]: %d\n", node, pr.size());
    for (list<partition_t*>::iterator it = pr.begin(); 
         it != pr.end(); it++) {
      rpass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }

    list<partition_t*>& ps = st->GetPartitionsByNode(node);
    logging("Partitions on node[%d]: %d\n", node, ps.size());
    for (list<partition_t*>::iterator it = ps.begin(); 
         it != ps.end(); it++) {
      spass1tasks->AddTask(new PartitionTask(*it, Params::kOffsetPass1, Params::kNumBitsPass1));
    }
  }
}
