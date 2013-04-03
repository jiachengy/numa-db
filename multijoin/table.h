#ifndef TABLE_H_
#define TABLE_H_

#include <cstring>
#include <vector>
#include <list>
#include <cassert>
#include <pthread.h>

class Partition;
class Table;

#include "params.h"
#include "hashtable.h" // hashtable_t
#include "types.h" // OpType
#include "util.h" // get_running_node

using namespace std;

struct partition_t {
  int node; // numa location
  int radix; // partition key

  tuple_t * tuple;
  size_t tuples;
  uint64_t offset;
  hashtable_t * hashtable;

  bool done; // indidate the partition has been processed
  bool ready; // indicate that the partiiton is ready to be processed

  ShareLevel share;
  pthread_mutex_t mutex; // protect tuple place allocation
};
// __attribute__((aligned(CACHE_LINE_SIZE)));

partition_t* partition_init(int node);
void partition_destroy(partition_t * partition);
void partition_reset(partition_t * partition);

class Table {
 private:
  static int __autoid__;
  static const int kInvalidId = -1;

  const int id_;
  OpType type_;
	
  std::vector<std::list<partition_t*> > pnodes_;
  uint32_t nnodes_;

  vector<list<partition_t*> > pkeys_;
  uint32_t nkeys_;
	
  bool ready_;
  bool done_;
	
  uint32_t nparts_;
  uint32_t done_count_;

  // output buffer
  //  size_t nbuffers_; 
  //  vector<vector<partition_t* > > buffers_;

  // locking
  pthread_mutex_t mutex_;

 public:
  static void ResetId() { __autoid__ = 0; }

  // constructor for creating base table
  Table(uint32_t nnodes, uint32_t nkeys);
  // construtor for intermediate tables
  Table(OpType type, uint32_t nnodes, uint32_t nkeys /*, size_t nbuffers */);
  ~Table();

  void AddPartition(partition_t *p);
  void Commit(int size = 1);

  list<partition_t*>& GetPartitionsByNode(int node) {return pnodes_[node];}
  list<partition_t*>& GetPartitionsByKey(int key) { return pkeys_[key]; }

  uint32_t nnodes() { return nnodes_; }
  uint32_t nkeys() { return nkeys_; }
  OpType type() { return type_; }
  void set_type(OpType type) { type_ = type;}
  int id() { return id_;}
  bool ready() { return ready_; }
  void set_ready() { ready_ = true; }
  bool done() { return done_;}
  void set_done() { done_ = true; }
  uint32_t done_count() { return done_count_; }
  uint32_t nparts() { return nparts_; }

  static Table* BuildTableFromRelation(relation_t *rel);
};


#endif // TABLE_H_
