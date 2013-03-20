#include <glog/logging.h>

#include "params.h"
#include "hashjoin.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

#ifndef NEXT_POW_2
/** 
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif


// add the partition
// schedule the next task
void FinishPartition(Partition *p, Table *table)
{
  p->set_ready(); // ready
  table->AddPartition(p);
}


void PartitionTask::ProcessBlock(thread_t *args, block_t block, uint32_t mask, uint32_t fanout, uint32_t hist[], tuple_t *dst[]) {

  // histogram
  memset(hist, 0, fanout * sizeof(uint32_t)); // clear histogram

  tuple_t *tuple = block.tuples;
  for (uint32_t i = 0; i < block.size; i++) {
    uint32_t idx = HASH_BIT_MODULO((tuple++)->key, mask, offset_);
    hist[idx]++;
  }

  // check output buffer
  for (uint32_t idx = 0; idx < fanout; idx++) {
    int buffer_id = args->tid * fanout + idx;
    Partition *outp = out_->GetBuffer(buffer_id);

    // if buffer does not exists
    // or if not enough space
    if (!outp || Partition::kPartitionSize - outp->size() < hist[idx]) {
      // if the buffer is full
      if (outp) {
        FinishPartition(outp, out_);
      }

      // create a new buffer
      // switch the output buffer to the new one
      Partition *np = new Partition(args->node_id, idx);
      np->Alloc();

      out_->SetBuffer(buffer_id, np);
      outp = np;

    }
    dst[idx] = &outp->tuples()[outp->size()];
    outp->set_size(outp->size() + hist[idx]); // set size at once
  }

  // second scan, partition and scatter
  tuple = block.tuples;
  for (uint32_t i = 0; i < block.size; i++) {
    uint32_t idx = HASH_BIT_MODULO(tuple->key, mask, offset_);
    *(dst[idx]++) = *(tuple++);
  }
}

void PartitionTask::Finish(thread_t* args)
{
  in_->Commit();

  // We are ready to recycle or destroy the partition
  delete part_;

  LOG(INFO) << "Table size: " << in_->done_count() << ":" << in_->nparts();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  LOG(INFO) << "I am the last.";
  LOG(INFO) << "Put buffers.";

  // Finish all remaining buffered partitions
  for (int i = 0; i < out_->nbuffers(); i++) {
    Partition *outp = out_->GetBuffer(i);
    if (outp) {
      FinishPartition(outp, out_);
    }
  }

  LOG(INFO) << "Put buffers done.";

  // set output table to ready
  out_->set_ready();


  // Unblock next operator
  // node_t *nodes = args->env->nodes();
  // for (int node = 0; node < args->env->nnodes(); node++) {
  //   nodes[node].queue->Unblock(out_->id());
  // }


  // BECAUSE WE ARE TESTING PARTITIONING ONLY
  // AFTER WE ARE DONE
  args->env->set_done();
}


void PartitionTask::Run(thread_t *args)
{
  uint32_t mask = ((1 << nbits_) - 1) << offset_;
  uint32_t fanout = 1 << nbits_;
  uint32_t hist[fanout];
  tuple_t *dst[fanout];

  LOG(INFO) << "Thread " << args->tid << " starts a partition.";

  // process the partition in blocks  
  while (!part_->done()) {
    block_t block = part_->NextBlock();
    assert(block.size > 0 && block.size <= Partition::kBlockSize);
    ProcessBlock(args, block, mask, fanout, hist, dst);    
  }

  LOG(INFO) << "Finish all blocks.";
  Finish(args);
  LOG(INFO) << "Thread " << args->tid << " finishes partition.";  
}




void BuildTask::Finish(thread_t* my, hashtable_t *ht)
{
  // hash table partition
  Partition *htp = new Partition(my->node_id, key_);
  htp->set_hashtable(ht);
  out_->AddPartition(htp);

  // Commit
  in_->Commit();

  // We are ready to recycle or destroy the partition
  list<Partition*> &parts = in_->GetPartitionsByKey(key_);
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++) {
    delete *it;
  }

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  LOG(INFO) << "I am the last.";

  // set the output hashtable table to ready
  out_->set_ready();


  // Unblock next operator
  // node_t *nodes = args->env->nodes();
  // for (int node = 0; node < args->env->nnodes(); node++) {
  //   nodes[node].queue->Unblock(out_->id());
  // }


  // BECAUSE WE ARE TESTING PARTITIONING ONLY
  // AFTER WE ARE DONE
  //  args->env->set_done();
}



// They combine building and probing
// Does it mean they will have better cache behavior?
// Does it mean that I need to reorder the task list?
void BuildTask::Run(thread_t *my)
{
  // Step 1: Estimate the size of the hash table
  // Traverse all partitions in the list
  
  uint32_t ntuples = 0;

  list<Partition*> &parts = in_->GetPartitionsByKey(key_);
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++) {
    ntuples += (*it)->size();
  }


  int * next, * bucket;
  uint32_t nbuckets = ntuples;
  NEXT_POW_2(nbuckets);
  nbuckets <<= 1; // make the hash table twice as large
  const uint32_t MASK = (nbuckets-1) << (Params::kNumRadixBits);
  next   = (int*) malloc(sizeof(int) * ntuples);
  bucket = (int*) calloc(nbuckets, sizeof(int));

  hashtable_t *ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  ht->next = next;
  ht->bucket = bucket;
  ht->nbuckets = nbuckets;
  ht->ntuples = ntuples;

  int i = 0;
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++) {
    tuple_t *tuple = (*it)->tuples();
    for (uint32_t j = 0; j < (*it)->size(); j++) {
      uint32_t idx = HASH_BIT_MODULO((tuple++)->key, MASK, Params::kNumRadixBits);
      next[i] = bucket[idx];
      bucket[idx] = ++i;
    }
  }
  Finish(my, ht);
}
