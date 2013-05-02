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
#ifdef COLUMN_WISE
  partition->key = NULL;
  partition->value = NULL;
#else
  partition->tuple = NULL;
#endif
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


Table::Table(OpType type, uint32_t nnodes, uint32_t nkeys)
  : id_(__autoid__++), ready_(false), done_(false),
    nparts_(0), done_count_(0)
{
  type_ = type;

  nkeys_ = nkeys;
  nnodes_ = nnodes;

  mutex_ = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t) * nnodes);
  for (int i = 0; i != nnodes; ++i) {
    blocks_.push_back(BlockList(nkeys));
    pthread_mutex_init(&mutex_[i], NULL);
  }
  pthread_mutex_init(&lock_, NULL);
  
}

Table::~Table() {
}

void Table::BatchAddBlocks(vector<partition_t*> *local_blocks, size_t tuples, int node)
{
  pthread_mutex_lock(&mutex_[node]);
  BlockList& node_blocks = blocks_[node];  
  size_t sum = 0;
  for (vector<partition_t*>::iterator it = local_blocks->begin();
       it != local_blocks->end(); ++it) {

    assert((*it)->radix >= 0);
    sum += (*it)->tuples;
    node_blocks.AddBlock(*it);
  }
  pthread_mutex_unlock(&mutex_[node]);

  assert(sum == tuples);

  pthread_mutex_lock(&lock_);
  nparts_ += local_blocks->size();
  tuples_ += tuples;
  pthread_mutex_unlock(&lock_);
}

void Table::AddPartition(partition_t *p)
{
  pthread_mutex_lock(&lock_);
  blocks_[p->node].AddBlock(p);
  nparts_++;
  tuples_ += p->tuples;
  pthread_mutex_unlock(&lock_);
}

bool Table::Commit(int size)
{
  pthread_mutex_lock(&lock_);
  bool done = false;
  done_count_ += size;

  if (ready_ && done_count_ == nparts_) {
    set_done();
    done = true;
  }
  pthread_mutex_unlock(&lock_);

  return done;
}

Table*
Table::BuildTableFromRelation(relation_t *rel)
{
  Table *table = new Table(OpNone, num_numa_nodes(), 1);

  size_t ntuples_per_partition = Params::kMaxTuples;

  uint32_t node;
  for (node = 0; node != rel->nnodes; ++node) {
    node_bind(node); // we ensure all the partition objects are allocated on that node

    size_t ntuples = rel->ntuples_on_node[node];
#ifdef COLUMN_WISE
	intkey_t * key = rel->key[node];
	value_t * value = rel->value[node];
#else
    tuple_t *tuple = rel->tuples[node];
#endif
    while (ntuples > 0) {
      size_t psize = (ntuples > ntuples_per_partition) ? ntuples_per_partition : ntuples;
      partition_t *p = partition_init(node);
#ifdef COLUMN_WISE
	  p->key = key;
	  p->value = value;
#else
      p->tuple = tuple;
#endif
      p->tuples = psize;
      p->radix = 0; // to make the radix computation in partitioning easier
#ifdef COLUMN_WISE
	  key += psize;
	  value += psize;
#else
      tuple += psize;
#endif
      ntuples -= psize;
      table->AddPartition(p);
    }
  }

  table->set_ready();
  return table;
}
