#ifndef TABLE_H_
#define TABLE_H_

#include <cstring>
#include <vector>
#include <list>
#include <cassert>
#include <pthread.h>

typedef struct partition_t partition_t;

class Table;

#include "params.h"
#include "hashtable.h" // hashtable_t
#include "types.h" // OpType
#include "util.h" // get_running_node
#include "memory.h"

using namespace std;

struct partition_t {
  int node; // numa location
  int radix; // partition key

  size_t capacity;

#ifdef COLUMN_WISE
  intkey_t * key;
  value_t * value;
#else  
  tuple_t * tuple;
#endif
  size_t tuples;
  uint64_t offset;
  hashtable_t * hashtable;

  bool done; // indidate the partition has been processed
  bool ready; // indicate that the partiiton is ready to be processed

  ShareLevel share;
  pthread_mutex_t mutex; // protect tuple place allocation

  Memory *memm;
};

typedef vector<vector<partition_t*> > block_grid_t;

class BlockIterator
{
 private:
  block_grid_t &grid_;
  int current_radix_;
  int current_pos_;

 public:
 BlockIterator(block_grid_t &grid) :grid_(grid) {
    current_radix_ = 0;
    current_pos_ = 0;
  }

  partition_t* getNext() {
    partition_t * next = grid_[current_radix_][current_pos_];
    ++current_pos_;
    return next;
  }

  bool hasNext() {
    if (current_radix_ == grid_.size())
      return false;
    if (current_pos_ < grid_[current_radix_].size())
      return true;
    
    while (++current_radix_ != grid_.size()) {
      if (!grid_[current_radix_].empty()) {
        current_pos_ = 0;
        return true;
      }      
    }

    return false;
  }
};

class BlockList
{
 private:
  block_grid_t grid_;
  int blocks_;
  size_t tuples_;

 public:
 BlockList(int partitions) : 
  blocks_(0), tuples_(0) {
    grid_.resize(partitions);
  }

  BlockIterator iterator() {
    return BlockIterator(grid_);
  }

  int blocks() { return blocks_; }

  size_t tuples() { return tuples_; }
  
  void AddBlock(partition_t* p) { 
    grid_[p->radix].push_back(p); 
    ++blocks_;
    tuples_ += p->tuples;
  }

  vector<partition_t*>& operator[] (const int index) { return grid_[index]; }
};


class BlockKeyIterator
{
 private:
  vector<BlockList> &blocks_;
  int key_;
  int current_node_;
  int current_pos_;

 public:
 BlockKeyIterator(vector<BlockList> &blocks, int key) :blocks_(blocks), key_(key) {
    current_node_ = 0;
    current_pos_ = 0;
  }

  partition_t* getNext() {
    partition_t * next = blocks_[current_node_][key_][current_pos_];
    ++current_pos_;
    return next;
  }

  bool hasNext() {
    if (current_node_ == blocks_.size())
      return false;
    if (current_pos_ < blocks_[current_node_][key_].size())
      return true;
    
    while (++current_node_ != blocks_.size()) {
      if (!blocks_[current_node_][key_].empty()) {
        current_pos_ = 0;
        return true;
      }      
    }
    return false;
  }
};

partition_t* partition_init(int node);
void partition_destroy(partition_t * partition);
void partition_reset(partition_t * partition);


class Table {
 private:
  static int __autoid__;
  static const int kInvalidId = -1;

  const int id_;
  OpType type_;
  
  vector<BlockList> blocks_;

  //  block_grid_t *pnodes_;
  uint32_t nnodes_;

  //  vector<vector<partition_t*> > pkeys_;
  uint32_t nkeys_;
	
  bool ready_;
  bool done_;

  size_t tuples_;	
  uint32_t nparts_;
  uint32_t done_count_;

  // locking
  pthread_mutex_t *mutex_; // locks of each node's blocklist
  pthread_mutex_t lock_; // lock of the entire table

 public:
  static void ResetId() { __autoid__ = 0; }

  Table(OpType type, uint32_t nnodes, uint32_t nkeys /*, size_t nbuffers */);
  ~Table();

  void BatchAddBlocks(vector<partition_t*> *local_blocks, size_t tuples, int node);
  void AddPartition(partition_t *p);
  bool Commit(int size = 1);

  BlockList& GetBlocksByNode(int node) {
    return blocks_[node];
  }

  //  vector<partition_t*>& GetPartitionsByNode(int node) {return pnodes_[node];}
  vector<partition_t*>& GetPartitionsByKey(int node, int key) { return blocks_[node][key]; }

  BlockKeyIterator GetBlocksByKey(int radix) {
    return BlockKeyIterator(blocks_, radix);
  }

  size_t tuples() { return tuples_; }

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
