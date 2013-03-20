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
  thread_t **groups;

  Taskqueue *queue;
};


struct thread_t {
  int tid;
  int cpu;
  int node_id;
  Tasklist *localtasks;
  Tasklist *stolentasks; // stolen

  node_t *node; // pointer to local node info
  Environment *env; // pointer to global info
};

using namespace std;

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
  vector<Table*> tables_;	

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

  Table* GetTable(int table_id) {
    return tables_[table_id];
  }

  void AddTable(Table *table) {
    tables_.push_back(table);
  }

  void TestPartition(Table *rt, Table *st);

  void CreateJoinTasks(Table *rt, Table *st);
};

#endif // ENV_H_
