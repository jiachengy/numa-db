#include <cassert>
#include <numa.h>

#include "memory.h"
#include "util.h" // alloc, dealloc

Memory::Memory(int node, size_t capacity, size_t capacity_hist)
  : node_(node), capacity_(capacity), capacity_hist_(capacity_hist)
{
  node_bind(node);
  
  base_ = (tuple_t*)alloc(sizeof(tuple_t) * capacity_);
  //  base_ = (tuple_t*)alloc_aligned(sizeof(tuple_t) * capacity_, Params::kPartitionSize);
  memset(base_, 0x0, sizeof(tuple_t) * capacity_);
  size_ = 0;

  base_hist_ = (uint32_t*)alloc(sizeof(uint32_t) * capacity_hist_);
  memset(base_hist_, 0x0, sizeof(uint32_t) * capacity_hist_);
  size_hist_ = 0;

  int max_partitions = capacity / Params::kMaxTuples;
  int max_hts = Params::kFanoutTotal;
  
  Alloc(max_partitions);
  AllocHT(max_hts);

  //  pthread_mutex_init(&mutex_, NULL);
}

Memory::~Memory() {
  while (!freelist_.empty()) {
    delete freelist_.front();
    freelist_.pop();
  }

  while (!freeht_.empty()) {
    delete freeht_.front();
    freeht_.pop();
  }

  dealloc(base_, sizeof(tuple_t) * capacity_);
  dealloc(base_hist_, sizeof(uint32_t) * capacity_hist_);

  //  pthread_mutex_destroy(&mutex_);
}

void
Memory::Alloc(size_t size)
{
  for (uint32_t i = 0; i < size; i++) {
    partition_t *p = partition_init(node_);
    p->tuple = &base_[size_];
    p->offset = size_;
    size_ += Params::kMaxTuples;
    assert(size_ <= capacity_);
    freelist_.push(p);
  }
}

void
Memory::AllocHT(size_t size)
{
  for (uint32_t i = 0; i < size; i++) {
    partition_t *p = partition_init(node_);
    hashtable_t * ht = (hashtable_t*)malloc(sizeof(hashtable_t));
    p->hashtable = ht;
    freeht_.push(p);
  }
}


partition_t*
Memory::GetPartition()
{
  //  pthread_mutex_lock(&mutex_);

  if (freelist_.empty()) 
    assert(false);

  partition_t *p = freelist_.front();
  freelist_.pop();

  //  pthread_mutex_unlock(&mutex_);

  return p;
}

partition_t*
Memory::GetHashtable(size_t tuples, size_t partitions)
{
  //  pthread_mutex_lock(&mutex_);

  if (freeht_.empty())
    assert(false);

  if (freelist_.empty())
    assert(false);

  partition_t *dp = freelist_.front();
  freelist_.pop();

  partition_t *htp = freeht_.front();
  freeht_.pop();

  uint32_t * sum = &base_hist_[size_hist_];
  size_hist_ += partitions;
  assert(size_hist_ < capacity_hist_);

  //  pthread_mutex_unlock(&mutex_);

  assert(tuples < Params::kMaxTuples);
  htp->hashtable->tuple = dp->tuple;
  htp->hashtable->tuples = tuples;
  htp->hashtable->sum = sum;
  htp->hashtable->partitions = partitions - 1;

  return htp;
}

	
// put back the partition for recycling
void
Memory::Recycle(partition_t *p)
{
  //  pthread_mutex_lock(&mutex_);
  assert(p->node == node_);
  partition_reset(p);
  freelist_.push(p);
  //  pthread_mutex_unlock(&mutex_);
}


// void
// Memory::RecycleHT(partition_t *p)
// {
//   pthread_mutex_lock(&mutex_);
//   assert(p->node == node_);
//   partition_reset(p);
//   freehts_.push(p);
//   pthread_mutex_unlock(&mutex_);
// }


