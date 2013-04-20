#include "params.h"
#include "table.h"
#include "hashtable.h"

int Table::__autoid__ = 0;

partition_t* partition_init(int node)
{
  partition_t * partition = (partition_t*)malloc(sizeof(partition_t));
  partition->node = node;
  partition->radix = -1;
  partition->done = false;
  partition->ready = false;
  partition->tuple = NULL;
  partition->tuples = 0;
  partition->capacity = 0;
  partition->offset = -1;
  partition->hashtable = NULL;
  partition->share = ShareLocal;
  partition->memm = NULL;
  pthread_mutex_init(&partition->mutex, NULL);
  return partition;
} 
void partition_destroy(partition_t * partition)
{
  pthread_mutex_destroy(&partition->mutex);
}

void partition_reset(partition_t * partition)
{
  partition_t * p = partition;
  p->radix = -1;
  p->done = false;
  p->ready = false;
  p->tuples = 0;
  p->hashtable = NULL;
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

  //  nbuffers_ = 0;

  pthread_mutex_init(&mutex_, NULL);
}


Table::Table(OpType type, uint32_t nnodes, uint32_t nkeys /*, size_t nbuffers */)
  : id_(__autoid__++), ready_(false), done_(false),
    nparts_(0), done_count_(0)
{
  type_ = type;

  nkeys_ = nkeys;
  if (nkeys_)
    pkeys_.resize(nkeys);

  nnodes_ = nnodes;
  pnodes_.resize(nnodes);


  pthread_mutex_init(&mutex_, NULL);
}

Table::~Table() {
  for (uint32_t node = 0; node < nnodes_; ++node) {
    list<partition_t*> &pnode = pnodes_[node];
    for (list<partition_t*>::iterator it = pnode.begin();
         it != pnode.end(); ++it) {
      assert(*it != NULL);
      delete *it;
    }
  }
}

void Table::AddPartition(partition_t *p)
{
  pthread_mutex_lock(&mutex_);

  pnodes_[p->node].push_back(p);
  if (nkeys_)
    pkeys_[p->radix].push_back(p);
  nparts_++;
  tuples_ += p->tuples;
 
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
  Table *table = new Table(num_numa_nodes(), 0);

  size_t ntuples_per_partition = Params::kMaxTuples;
  //  size_t ntuples_per_partition = rel->ntuples; 

  uint32_t node;
  for (node = 0; node != rel->nnodes; ++node) {
    node_bind(node); // we ensure all the partition objects are allocated on that node

    size_t ntuples = rel->ntuples_on_node[node];
    tuple_t *tuple = rel->tuples[node];
    while (ntuples > 0) {
      size_t psize = (ntuples > ntuples_per_partition) ? ntuples_per_partition : ntuples;
      partition_t *p = partition_init(node);
      p->tuple = tuple;
      p->tuples = psize;
      p->radix = 0; // to make the radix computation in partitioning easier
      tuple += psize;
      ntuples -= psize;
      table->AddPartition(p);
    }
  }
  
  // initialize all remaining nodes to NULL
  for (; node != num_numa_nodes(); ++node) {
    // do nothing
  }


  table->set_ready();
  return table;
}
