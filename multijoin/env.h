#ifndef ENV_H_
#define ENV_H_

#include <pthread.h>
#include <numa.h>

class Environment;

typedef struct node_t node_t;
typedef struct thread_t thread_t;

#include "taskqueue.h"
#include "table.h"

struct node_t {
  int node_id;
  int nthreads;
  thread_t **groups;

  int next_node;
  int next_cpu;

  Taskqueue *queue;
  pthread_mutex_t lock;
};


struct thread_t {
  int tid;
  int cpu;
  int node_id;
  Tasklist *localtasks;
  Tasklist *stolentasks; // stolen

  node_t *node; // pointer to local node info
  Environment *env; // pointer to global info

  // counters
  uint32_t local;
  uint32_t shared;
  uint32_t remote;
};

using namespace std;

class Environment
{
 private:
  // general info
  const int nthreads_;
  const int nnodes_;

  // node and thread info
  node_t *nodes_; // all nodes structure
  thread_t *threads_;

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

 public:
  Environment(int nthreads);
  ~Environment();

  node_t *nodes() { return nodes_; }
  thread_t *threads() { return threads_; }
  int nthreads() { return nthreads_; }
  int nnodes() { return nnodes_; }
  bool done() { return done_; }
  void set_done() { done_ = true; }
  vector<Tasklist*>& probes() { return probes_; }
  Table* build_table() { return build_; }
  int num_tables() { return tables_.size(); }

  Table* GetTable(int table_id) {
    return tables_[table_id];
  }

  void AddTable(Table *table) {
    tables_.push_back(table);
  }

  void CreateJoinTasks(Table *rt, Table *st);
};

#endif // ENV_H_
