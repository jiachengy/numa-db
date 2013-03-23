#ifndef TABLE_H_
#define TABLE_H_

#include <cstring>
#include <list>
#include <cassert>
#include <glog/logging.h>

class Partition;
class Table;

#include "types.h" // OpType
#include "util.h" // get_running_node

using namespace std;

struct block_t {
	tuple_t *tuples;
	size_t size;
block_t(tuple_t *ts, size_t sz) : tuples(ts), size(sz) {}
};

struct entry_t {
  tuple_t tuple;
  int next;
};

struct hashtable_t {
  entry_t *next;
  int *bucket;
  uint32_t nbuckets;
  uint32_t ntuples;
};

class Partition {
 private:
  int node_; // numa location
  int key_; // partition key

  hashtable_t *hashtable_;

  tuple_t *tuples_; // actual data
  size_t size_;
  uint32_t curpos_;

  bool done_; // indidate the partition has been processed
  bool ready_; // indicate that the partiiton is ready to be processed

 public:
  static const size_t kBlockSize = 1024;
  static const size_t kPartitionSize = 32768;

  Partition(int node, int key) {
    // NOTE: the constructor must run on the right node
    assert(get_running_node() == node);

    node_ = node;
    key_ = key;
    size_ = 0;
    curpos_ = 0;
    done_ = false;
    ready_ =false;
    tuples_ = NULL;
    hashtable_ = NULL;
  }

  ~Partition() {
    if (tuples_) {
      dealloc(tuples_, sizeof(tuple_t) * kPartitionSize);
      tuples_ = NULL;
    }

    if (hashtable_) {
      dealloc(hashtable_->next, sizeof(entry_t) * hashtable_->ntuples);
      dealloc(hashtable_->bucket, sizeof(int) * hashtable_->nbuckets);
      free(hashtable_);
      hashtable_ = NULL;
    }
  }

  void Alloc() {
    if (!tuples_) {
      tuples_ = (tuple_t*)alloc(sizeof(tuple_t) * kPartitionSize);
      memset(tuples_, 0, sizeof(tuple_t) * kPartitionSize);
    }
  }

  block_t NextBlock() {
    size_t sz = kBlockSize;
    uint32_t pos = curpos_;
    if (curpos_ + sz >= size_) {
      sz = size_ - curpos_;
      curpos_ = -1;
      set_done();
    }
    else
      curpos_ += kBlockSize;
    return block_t(tuples_ + pos, sz);
  }

  void Reset() { 	// used by the recycler
    // node will not be changed
    key_ = -1;
    done_ = false;
    ready_ =false;
    size_ = 0;
    curpos_ = 0;
    hashtable_ = NULL;
  }

  void Append(tuple_t tuple) {
    tuples_[size_++] = tuple;
  }


  // for efficiency purpose, we expose the 
  // raw data and allow us to set the size at once
  tuple_t* tuples() { return tuples_; }
  void set_size(size_t sz) { size_ = sz; }
  size_t size() { return size_; }
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

  int id_;
  OpType type_;
	
  std::vector<std::list<Partition*> > pnodes_;
  uint32_t nnodes_;

  std::vector<std::list<Partition*> > pkeys_;
  uint32_t nkeys_;
	
  bool ready_;
  bool done_;
	
  uint32_t nparts_;
  uint32_t done_count_;

  // output buffer
  size_t nbuffers_; 
  Partition** buffers_;

 public:
  // constructor for creating base table
  Table(uint32_t nnodes, uint32_t nkeys);
  // construtor for intermediate tables
  Table(OpType type, uint32_t nnodes, uint32_t nkeys, size_t nbuffers);
  ~Table();

  void AddPartition(Partition *p) {
    p->set_ready(); // p is read only now

    nparts_++;
    pnodes_[p->node()].push_back(p);

    // if it has a partition key
    if (nkeys_)
      pkeys_[p->key()].push_back(p);
  }

  std::list<Partition*>& GetPartitionsByNode(int node) {
    return pnodes_[node];
  }


  std::list<Partition*>& GetPartitionsByKey(int key) {
    return pkeys_[key];
  }

  // commit when a partition has finished processing
  // by default commit 1
  // but we can commit more than 1 at a time
  void Commit(int size = 1) {
    done_count_ += size;
    if (done_count_ == nparts_)
      set_done();
  }

  Partition* GetBuffer(int buffer_id) {
    return buffers_[buffer_id];
  }

  void SetBuffer(int buffer_id, Partition *buffer) {
    buffers_[buffer_id] = buffer;
  }

  int nbuffers() { return nbuffers_; }
  OpType type() { return type_; }
  void set_type(OpType type) { type_ = type;}
  int id() { return id_;}
  //  void set_id(int id) { id_ = id;}
  bool ready() { return ready_; }
  void set_ready() { ready_ = true; }
  bool done() { return done_;}
  void set_done() { done_ = true; }
  uint32_t done_count() { return done_count_; }
  uint32_t nparts() { return nparts_; }
};


#endif // TABLE_H_
