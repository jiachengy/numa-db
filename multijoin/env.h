#ifndef ENV_H_
#define ENV_H_

#include <pthread.h>
#include <numa.h>

#include "taskqueue.h"
#include "table.h"
#include "memory.h"
#include "perf.h"

typedef struct thread_t thread_t;
typedef struct node_t node_t;

class Environment;
class P2Task;
class ProbeTask;

struct buffer_t {
  Table * table; // get p2tasks by table
  int radix;
  int shift;

  partition_t **partition;
  int partitions;

  size_t tuples;
  //  BlockList *localblocks;
  vector<partition_t*> *localblocks;
};

buffer_t* buffer_init(Table *table, int radix, int shift, int partitions);
void buffer_destroy(buffer_t *buffer);
bool buffer_compatible(buffer_t *buffer, Table *table, int radix);


struct node_t {
  int node_id;
  uint32_t nthreads;
  thread_t **groups;

  uint32_t next_node;
  uint32_t next_cpu;

  Taskqueue *queue;
  pthread_mutex_t lock;
};


struct thread_t {
  int tid;

  int tid_of_node;
  int cpu;
  int node_id;

  /* a batch task is a composite task */
  /* e.g. partition pass2 or probing */
  Task *batch_task;
  /* localtasks store the tasklists in the batch task */
  /* localtasks and batch_task have to be updated at the same time */
  Tasklist *localtasks;
  Tasklist *stolentasks; // stolen

  node_t *node; // pointer to local node info
  Environment *env; // pointer to global info
  Memory *memm[2]; // memory manager

  // local buffer
  buffer_t *buffer;
  pthread_mutex_t lock;


  uint32_t *hist; // hist holder for build
#ifdef COLUMN_WISE
  intkey_t **part_key; // output buffer holder for build
  value_t **part_value; // output buffer holder for build

  cache_line_t *wc_buf_key;
  cache_line_t *wc_buf_value;
  intkey_t **wc_part_key;
  value_t **wc_part_value;
#else
  tuple_t **part; // output buffer holder for build
  cache_line_t *wc_buf;
  tuple_t **wc_part;
#endif
  uint32_t *wc_count;

  // statistics
  uint32_t local;
  uint32_t shared;
  uint32_t remote;

  // performance counters
  perf_t *perf; 
  perf_counter_t *stage_counter;
  perf_counter_t total_counter;


};

using namespace std;

class Environment
{
 private:
  // general info
  const size_t nthreads_;
  const size_t nnodes_;
  //  const size_t capacity_;

  // node and thread info
  node_t *nodes_; // all nodes structure
  thread_t *threads_;
  //  Memory **memm_;

  // table info
  vector<Table*> tables_;	

  // all task lists
  vector<Tasklist*> tasks_;

  // all task lists
  vector<P2Task***> p2tasks_;

  // all probe lists
  vector<ProbeTask**> probetasks_;

  // probe lists
  //  vector<Tasklist*> probes_;

  // build table
  Table *build_;

  // indicate the query is finished.
  int queries_;
  bool done_;

  pthread_barrier_t barrier_;

 public:
  Environment(uint32_t nnodes, uint32_t nthreads);
  ~Environment();
  void InitMem();

  node_t *nodes() { return nodes_; }
  thread_t *threads() { return threads_; }
  int nthreads() { return nthreads_; }

  int nthreads_per_node() { return nthreads_ / nnodes_; }

  int nnodes() { return nnodes_; }

  int queries() { return queries_; }
  void commit() { --queries_; }

  bool done() { return done_; }
  void set_done() { done_ = true; }
  //  vector<Tasklist*>& probes() { return probes_; }
  Table* build_table() { return build_; }
  int num_tables() { return tables_.size(); }

  Table* GetTable(int table_id) {
    return tables_[table_id];
  }

  Table* output_table() {
    return tables_[tables_.size() - 1];
  }

  void AddTable(Table *table) {
    tables_.push_back(table);
  }

  void SyncBarrier() {
    pthread_barrier_wait(&barrier_);
  }

  void AddP2Tasks(P2Task *** p2tasks, uint32_t table_id) {
    if (table_id >= p2tasks_.size())
      p2tasks_.resize(table_id + 1);
    p2tasks_[table_id] = p2tasks;
  }

  void AddProbeTasks(ProbeTask ** probetasks, uint32_t table_id) {
    if (table_id >= probetasks_.size())
      probetasks_.resize(table_id + 1);
    probetasks_[table_id] = probetasks;
  }

  P2Task*** GetP2TaskByTable(int table_id) {
    return p2tasks_[table_id];
  }

  ProbeTask** GetProbeTaskByTable(int table_id) {
    return probetasks_[table_id];
  }

  void PartitionAndBuild(relation_t *relR);
  void TwoPassPartition(relation_t *relR);
  void TwoPassPartition(relation_t *relR, relation_t *relS);
  void RadixPartition(relation_t *rel);
  void Hashjoin(relation_t *relR, relation_t *relS);

};

#endif // ENV_H_
