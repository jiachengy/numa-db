#include "params.h"
#include "table.h"
#include "hashtable.h"

int Table::__autoid__ = 0;

// NO memory release here
Partition::~Partition()
{
  if (tuples_) {
    //    Dealloc();
    tuples_ = NULL;
  }

  if (hashtable_) {
    //    hashtable_free(hashtable_);
    free(hashtable_); // just deallocate the hashtable_t
    hashtable_ = NULL;
  }
}

void Partition::Alloc()
{
  if (!tuples_) {
    assert(get_running_node() == node_);
    tuples_ = (tuple_t*)alloc(sizeof(tuple_t) * Params::kPartitionSize);
    memset(tuples_, 0, sizeof(tuple_t) * Params::kPartitionSize);
  }
}

void Partition::Dealloc()
{
  if (tuples_) {
    dealloc(tuples_, sizeof(tuple_t) * Params::kPartitionSize);
    tuples_ = NULL;
  }
}

block_t Partition::NextBlock() {
  size_t sz = Params::kBlockSize;
  uint32_t pos = curpos_;
  if (curpos_ + sz >= size_) {
    sz = size_ - curpos_;
    curpos_ = -1;
    set_done();
  }
  else
    curpos_ += Params::kBlockSize;
  return block_t(tuples_ + pos, sz);
}

void Partition::Reset() { 	// used by the recycler
  // node cannot be changed
  key_ = -1;
  done_ = false;
  ready_ =false;
  size_ = 0;
  curpos_ = 0;

  if (hashtable_)
    hashtable_reset(hashtable_);
}


Table::Table(uint32_t nnodes, uint32_t nkeys)
  : id_(__autoid__++), ready_(false), done_(false),
    nparts_(0), done_count_(0)
{
  type_ = OpNone;

  nkeys_ = nkeys;
  if (nkeys_)
    pkeys_.resize(nkeys);

  nnodes_ = nnodes;
  pnodes_.resize(nnodes);

  nbuffers_ = 0;
  buffers_ = NULL;

  pthread_mutex_init(&mutex_, NULL);
}


Table::Table(OpType type, uint32_t nnodes, uint32_t nkeys, size_t nbuffers)
  : id_(__autoid__++), ready_(false), done_(false),
    nparts_(0), done_count_(0)
{
  type_ = type;

  nkeys_ = nkeys;
  if (nkeys_)
    pkeys_.resize(nkeys);

  nnodes_ = nnodes;
  pnodes_.resize(nnodes);

  nbuffers_ = nbuffers;
  buffers_ = (Partition**)malloc(sizeof(Partition*) * nbuffers);
  memset(buffers_, 0, sizeof(Partition*) * nbuffers);

  pthread_mutex_init(&mutex_, NULL);
}

Table::~Table() {
  if (buffers_)
    free(buffers_);

  for (uint32_t node = 0; node < nnodes_; ++node) {
    list<Partition*> &pnode = pnodes_[node];
    for (list<Partition*>::iterator it = pnode.begin();
         it != pnode.end(); ++it) {
      assert(*it != NULL);
      delete *it;
    }
  }
}

void Table::AddPartition(Partition *p)
{
  pthread_mutex_lock(&mutex_);

  p->set_ready(); // p is read only now
  pnodes_[p->node()].push_back(p);
  if (nkeys_)
    pkeys_[p->key()].push_back(p);
  nparts_++;

  pthread_mutex_unlock(&mutex_);
}

void Table::Commit(int size)
{
  pthread_mutex_lock(&mutex_);

  done_count_ += size;
  if (done_count_ == nparts_)
    set_done();

  pthread_mutex_unlock(&mutex_);
}
