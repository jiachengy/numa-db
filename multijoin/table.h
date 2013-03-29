#ifndef TABLE_H_
#define TABLE_H_

#include <cstring>
#include <list>
#include <cassert>
#include <glog/logging.h>
#include <pthread.h>

class Partition;
class Table;

#include "hashtable.h" // hashtable_t
#include "types.h" // OpType
#include "util.h" // get_running_node

using namespace std;

struct block_t {
  tuple_t *tuples;
  size_t size;

  block_t(tuple_t *ts, size_t sz) : tuples(ts), size(sz) {}
};


class Partition {
 private:
  const int node_; // numa location
  int key_; // partition key

  bool done_; // indidate the partition has been processed
  bool ready_; // indicate that the partiiton is ready to be processed

  tuple_t *tuples_;
  size_t size_;
  uint32_t curpos_;
  hashtable_t *hashtable_;


 public:
  Partition(int node, int key) 
    : node_(node), key_(key),
    done_(false), ready_(false),
    tuples_(NULL), size_(0), curpos_(0),
    hashtable_(NULL) {}
  ~Partition();


  // Called only if recycler is not used
  void Alloc();
  void Dealloc();

  void Reset();
  void Append(tuple_t tuple) { tuples_[size_++] = tuple; }

  tuple_t* tuples() { return tuples_; }
  void set_tuples(tuple_t *tuples) { tuples_ = tuples; }

  size_t size() { return size_; }
  void set_size(size_t sz) { size_ = sz; }

  hashtable_t* hashtable() { return hashtable_; }
  void set_hashtable(hashtable_t *ht) { hashtable_ = ht; }

  int node() { return node_; }

  int key() { return key_;}
  void set_key(int key) { key_ = key;}
	
  bool ready() { return ready_; }
  void set_ready() { ready_ = true; }
  bool done() { return done_; }
  void set_done() { done_ = true; }
};

class Table {
 private:
  static int __autoid__;
  static const int kInvalidId = -1;

  const int id_;
  OpType type_;
	
  std::vector<std::list<Partition*> > pnodes_;
  uint32_t nnodes_;

  vector<list<Partition*> > pkeys_;
  uint32_t nkeys_;
	
  bool ready_;
  bool done_;
	
  uint32_t nparts_;
  uint32_t done_count_;

  // output buffer
  size_t nbuffers_; 
  Partition** buffers_;

  // locking
  pthread_mutex_t mutex_;

 public:
  static void ResetId() { __autoid__ = 0; }

  // constructor for creating base table
  Table(uint32_t nnodes, uint32_t nkeys);
  // construtor for intermediate tables
  Table(OpType type, uint32_t nnodes, uint32_t nkeys, size_t nbuffers);
  ~Table();

  void AddPartition(Partition *p);
  void Commit(int size = 1);

  list<Partition*>& GetPartitionsByNode(int node) {return pnodes_[node];}
  list<Partition*>& GetPartitionsByKey(int key) { return pkeys_[key]; }

  Partition* GetBuffer(int buffer_id) {return buffers_[buffer_id];}
  void SetBuffer(int buffer_id, Partition *buffer) {buffers_[buffer_id] = buffer;}

  uint32_t nnodes() { return nnodes_; }
  uint32_t nkeys() { return nkeys_; }
  int nbuffers() { return nbuffers_; }
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
