#ifndef MEMORY_H_
#define MEMORY_H_

#include <pthread.h>
#include <queue>

#include "params.h"
#include "types.h"
#include "table.h"

using namespace std;

class Memory
{
 private:
  const int node_;

  size_t capacity_;
  size_t size_;
  tuple_t* base_;

  size_t capacity_hist_;
  size_t size_hist_;
  uint32_t * base_hist_;

  queue<partition_t*> freelist_;
  queue<partition_t*> freeht_;

  // Note: we do not maintain all the partitions allocated from this recycler
  // But it is a good practice to ensure everything is bookkept here.
  // list<Partition*> all_;
  //  pthread_mutex_t mutex_;

  void Alloc(size_t size);
  void AllocHT(size_t size);

 public:
  Memory(int node, size_t capacity, size_t capacity_ht);
  ~Memory();	

  tuple_t* baseptr() { return (tuple_t*)base_; }
  size_t available() { return freelist_.size(); }

  partition_t * GetPartition();	
  partition_t * GetHashtable(size_t tuples, size_t histsize);

  void Recycle(partition_t *p);

  // what if we disallow hash table recycling?
  // because we need to ZERO all elements
  //  partition_t* GetEmptyHT();
  // put back the partition for recycling
  //  void RecycleHT(partition_t *p);
};

#endif // MEMORY_H_
