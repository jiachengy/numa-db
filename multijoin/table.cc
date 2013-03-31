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

  pthread_mutex_destroy(&mutex_);
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


void Partition::Reset() { 	// used by the recycler
  // node cannot be changed
  key_ = -1;
  done_ = false;
  ready_ =false;
  size_ = 0;

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

  buffers_.resize(nnodes_);  
  for (uint32_t node = 0; node < nnodes; ++node) {
    for (uint32_t b = 0; b < nbuffers_; b++)
      buffers_[node].push_back(NULL);
  }

  pthread_mutex_init(&mutex_, NULL);
}

Table::~Table() {
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

  tuple_t *tuple = p->tuples();
  for (uint32_t i = 0; i < p->size(); i++)
    assert(tuple[i].key != 0);
 
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

Table*
Table::BuildTableFromRelation(relation_t *rel)
{
  Table *table = new Table(rel->nnodes, 0);

  size_t ntuples_per_partition = Params::kPartitionSize / sizeof(tuple_t);
  for (uint32_t node = 0; node < rel->nnodes; ++node) {
    node_bind(node); // we ensure all the partition objects are allocated on that node

    size_t ntuples = rel->ntuples_on_node[node];
    tuple_t *tuple = rel->tuples[node];
    while (ntuples > 0) {
      size_t psize = (ntuples > ntuples_per_partition) ? ntuples_per_partition : ntuples;
      Partition *p = new Partition(node, 0, ShareLocal);
      p->set_tuples(tuple);
      p->set_size(psize);

      tuple += psize;
      ntuples -= psize;

      table->AddPartition(p);
    }
  }
  table->set_ready();
  return table;
}
