#ifndef ENV_H_
#define ENV_H_

#include <pthread.h>
#include <numa.h>

class Environment;

typedef struct node_t node_t;
typedef struct thread_t thread_t;

#include "taskqueue.h"
#include "table.h"
#include "memory.h"
#include "builder.h"

#include "perf.h"

struct buffer_t {
  int table;
  int radix;
  
  partition_t **partition;
  int partitions;
};

buffer_t* buffer_init(int table, int key, int partitions);
void buffer_destroy(buffer_t *buffer);
bool buffer_compatible(buffer_t *buffer, int table, int radix);


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
  Memory *memm; // memory manager

  /* local buffer */
  buffer_t *buffer;

  // counters
  uint32_t local;
  uint32_t shared;
  uint32_t remote;

#ifdef USE_PERF
#if PER_CORE == 1
  perf_t *perf;
#endif
#endif
};

using namespace std;

class Environment
{
 private:
  // general info
  const size_t nthreads_;
  const size_t nnodes_;
  const size_t capacity_;

  // node and thread info
  node_t *nodes_; // all nodes structure
  thread_t *threads_;
  Memory **memm_;

  // table info
  vector<Table*> tables_;	

  // all task lists
  vector<Tasklist*> tasks_;

  // probe lists
  vector<Tasklist*> probes_;

  // build table
  Table *build_;

  // indicate the query is finished.
  bool done_;

  static void*init_thread(void *params);
  void Init();

 public:
  Environment(uint32_t nnodes, uint32_t nthreads, size_t memory_limit);
  ~Environment();

  node_t *nodes() { return nodes_; }
  thread_t *threads() { return threads_; }
  int nthreads() { return nthreads_; }

  int nthreads_per_node() { return nthreads_ / nnodes_; }

  int nnodes() { return nnodes_; }
  bool done() { return done_; }
  void set_done() { done_ = true; }
  vector<Tasklist*>& probes() { return probes_; }
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

  void Reset();

  void TwoPassPartition(relation_t *relR);
  void TwoPassPartition(relation_t *relR, relation_t *relS);
  void RadixPartition(relation_t *rel);
};

#endif // ENV_H_
