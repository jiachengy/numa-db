#ifndef RECYCLER_H_
#define RECYCLER_H_

#include <pthread.h>
#include <queue>

#include "params.h"
#include "table.h"

using namespace std;

class Recycler
{
 private:
  static const size_t kAllocUnit = 1024;
  static const size_t kPreAllocHashtables = Params::kFanoutPass1 / 2;

  const int node_;

  size_t capacity_;
  size_t cur_;
  char* data_;

  queue<Partition*> freelist_;
  queue<Partition*> freehts_;

  // Note: we do not maintain all the partitions allocated from this recycler
  // But it is a good practice to ensure everything is bookkept here.
  // list<Partition*> all_;
  pthread_mutex_t mutex_;

  void Alloc(size_t size);
  void AllocHT(size_t size);

 public:
  Recycler(int node, size_t capacity);
  ~Recycler();	

  Partition* GetEmptyPartition();	


  // what if we disallow hash table recycling?
  // because we need to ZERO all elements
  Partition* GetEmptyHT();

  // put back the partition for recycling
  void Recycle(Partition *p);
  void RecycleHT(Partition *p);
  
};

#endif // RECYCLER_H_
