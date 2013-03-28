#include <glog/logging.h>
#include <cassert>
#include <numa.h>

#include "recycler.h"
#include "util.h" // alloc, dealloc

Recycler::Recycler(int node, size_t capacity)
  : node_(node), capacity_(capacity)
{
  node_bind(node);
  
  data_ = (char*)alloc(capacity_);
  assert(data_ != NULL);
  memset(data_, 0, capacity_);
  cur_ = 0;

  LOG(INFO) << "ht: "<< Recycler::kPreAllocHashtables;

  AllocHT(Recycler::kPreAllocHashtables);


  LOG(INFO) << "ht: "<< Recycler::kPreAllocHashtables;

  size_t nparts = (capacity_ - cur_) / (Params::kPartitionSize * sizeof(tuple_t));
  LOG(INFO) << "nparts: "<< nparts;

  Alloc(nparts);

  pthread_mutex_init(&mutex_, NULL);
}

Recycler::~Recycler() {
  while (!freelist_.empty()) {
    delete freelist_.front();
    freelist_.pop();
  }

  while (!freehts_.empty()) {
    delete freehts_.front();
    freehts_.pop();
  }

  dealloc(data_, capacity_);

  pthread_mutex_destroy(&mutex_);
}

void
Recycler::Alloc(size_t size)
{
  for (uint32_t i = 0; i < size; i++) {
    Partition *p = new Partition(node_, -1);
    p->set_tuples((tuple_t*)(data_ + cur_));
    cur_ += Params::kPartitionSize * sizeof(tuple_t);

    assert(cur_ <= capacity_);

    freelist_.push(p);
  }
}

void
Recycler::AllocHT(size_t size)
{
  size_t htsz;
  for (uint32_t i = 0; i < size; i++) {
    Partition *p = new Partition(node_, -1);
    hashtable_t *ht = hashtable_init_noalloc(Params::kMaxHtTuples);
    ht->next = (entry_t*)(data_ + cur_);
    cur_ += (sizeof(entry_t) * ht->ntuples);
    ht->bucket = (int*)(data_ + cur_);
    cur_ += (sizeof(int) * ht->nbuckets);

    assert(cur_ < capacity_);

    p->set_hashtable(ht);
    freehts_.push(p);

    htsz = sizeof(entry_t) * ht->ntuples + sizeof(int) * ht->nbuckets;
  }

  LOG(INFO) << kPreAllocHashtables << " hashtables of size " << htsz;
}


Partition*
Recycler::GetEmptyPartition()
{
  pthread_mutex_lock(&mutex_);

  if (freelist_.empty()) {
    Alloc(kAllocUnit);
    assert(false);
  }

  Partition *p = freelist_.front();
  freelist_.pop();

  pthread_mutex_unlock(&mutex_);

  return p;
}

Partition*
Recycler::GetEmptyHT()
{
  pthread_mutex_lock(&mutex_);

  if (freehts_.empty()) {
    AllocHT(kAllocUnit);
    assert(false);
  }

  Partition *p = freehts_.front();
  freehts_.pop();

  pthread_mutex_unlock(&mutex_);

  return p;
}

	
// put back the partition for recycling
void
Recycler::Recycle(Partition *p)
{
  pthread_mutex_lock(&mutex_);
  assert(p->node() == node_);
  p->Reset();
  freelist_.push(p);
  pthread_mutex_unlock(&mutex_);
}


void
Recycler::RecycleHT(Partition *p)
{
  pthread_mutex_lock(&mutex_);
  assert(p->node() == node_);
  p->Reset();
  freehts_.push(p);
  pthread_mutex_unlock(&mutex_);
}


