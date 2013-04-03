#include <cassert>
#include <numa.h>

#include "memory.h"
#include "util.h" // alloc, dealloc

Memory::Memory(int node, size_t capacity)
  : node_(node), capacity_(capacity)
{
  node_bind(node);
  
  base_ = (tuple_t*)alloc_aligned(sizeof(tuple_t) * capacity_, Params::kPartitionSize);
  memset(base_, 0x0, sizeof(tuple_t) * capacity_);
  size_ = 0;

  int max_partitions = capacity / Params::kMaxTuples;
  logging("max partitions: %d\n", max_partitions);

  Alloc(max_partitions);

  pthread_mutex_init(&mutex_, NULL);
}

Memory::~Memory() {
  while (!freelist_.empty()) {
    delete freelist_.front();
    freelist_.pop();
  }

  while (!freehts_.empty()) {
    delete freehts_.front();
    freehts_.pop();
  }

  dealloc(base_, sizeof(tuple_t) * capacity_);

  pthread_mutex_destroy(&mutex_);
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

// void
// Memory::AllocHT(size_t size)
// {
//   size_t htsz;
//   for (uint32_t i = 0; i < size; i++) {
//     partition_t *p = partition_init(node_);
//     hashtable_t *ht = hashtable_init_noalloc(Params::kMaxHtTuples);
//     ht->next = (entry_t*)(data_ + cur_);
//     cur_ += (sizeof(entry_t) * ht->ntuples);
//     ht->bucket = (int*)(data_ + cur_);
//     cur_ += (sizeof(int) * ht->nbuckets);

//     assert(cur_ < capacity_);

//     p->hashtable = ht;
//     freehts_.push(p);

//     htsz = sizeof(entry_t) * ht->ntuples + sizeof(int) * ht->nbuckets;
//   }
// }


partition_t*
Memory::GetPartition()
{
  pthread_mutex_lock(&mutex_);

  if (freelist_.empty()) 
    assert(false);

  partition_t *p = freelist_.front();
  freelist_.pop();

  pthread_mutex_unlock(&mutex_);

  return p;
}

// partition_t*
// Memory::GetEmptyHT()
// {
//   pthread_mutex_lock(&mutex_);

//   if (freehts_.empty()) {
//     AllocHT(kAllocUnit);
//     assert(false);
//   }

//   partition_t *p = freehts_.front();
//   freehts_.pop();

//   pthread_mutex_unlock(&mutex_);

//   return p;
// }

	
// put back the partition for recycling
void
Memory::Recycle(partition_t *p)
{
  pthread_mutex_lock(&mutex_);
  assert(p->node == node_);
  partition_reset(p);
  freelist_.push(p);
  pthread_mutex_unlock(&mutex_);
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


