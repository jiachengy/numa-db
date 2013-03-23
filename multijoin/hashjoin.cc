#include <glog/logging.h>

#include "params.h"
#include "hashjoin.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

void PartitionTask::ProcessBlock(thread_t *my, block_t block, uint32_t mask, uint32_t fanout, uint32_t hist[], tuple_t *dst[])
{
  // reset histogram
  memset(hist, 0, fanout * sizeof(uint32_t));

  tuple_t *tuple = block.tuples;
  for (uint32_t i = 0; i < block.size; i++) {
    uint32_t idx = HASH_BIT_MODULO((tuple++)->key, mask, offset_);
    hist[idx]++;
  }

  // check output buffer
  for (uint32_t idx = 0; idx < fanout; idx++) {
    int buffer_id = my->tid * fanout + idx;
    Partition *outp = out_->GetBuffer(buffer_id);

    // if buffer does not exists,  or if not enough space
    if (!outp || Partition::kPartitionSize - outp->size() < hist[idx]) {
      // if the buffer is full
      if (outp) {
        out_->AddPartition(outp);
      }

      // create a new buffer
      // switch the output buffer to the new one
      Partition *np = new Partition(my->node_id, idx);
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

void PartitionTask::Finish(thread_t* my)
{
  in_->Commit();

  // Put back the partition if we use a recycler

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // Finish all remaining buffered partitions
  for (int i = 0; i < out_->nbuffers(); i++) {
    Partition *outp = out_->GetBuffer(i);
    if (outp)
      out_->AddPartition(outp);
  }

  // set output table to ready
  out_->set_ready();

  node_t *nodes = my->env->nodes();
  switch (out_->type()) {
  case OpNone: // Set termination flag
    my->env->set_done();
    return;
  case OpBuild: //  Unblock build
    {
      for (int node = 0; node < my->env->nnodes(); ++node)
        nodes[node].queue->Unblock(out_->id());
    }
    break;
  case OpProbe: // create probe task
    {
      for (int key = 0; key < Params::kFanoutPass1; key++) {
        Tasklist *probelist = my->env->probes()[key];

        list<Partition*> &parts = out_->GetPartitionsByKey(key);
        for (list<Partition*>::iterator it = parts.begin();
             it != parts.end(); it++) {
          Task *task = new UnitProbeTask(OpUnitProbe, *it, probelist->in(), probelist->out(), my->env->build_table());
          probelist->AddTask(task);
        }
      }
      // unblock probing queues
      for (int node = 0; node < my->env->nnodes(); ++node)
        nodes[node].queue->Unblock(out_->id());
    }
    break;
  default:
    assert(false);
    return;
  }
}


void PartitionTask::Run(thread_t *args)
{
  uint32_t mask = ((1 << nbits_) - 1) << offset_;
  uint32_t fanout = 1 << nbits_;
  uint32_t hist[fanout];
  tuple_t *dst[fanout];

  // process the partition in blocks  
  while (!part_->done()) {
    block_t block = part_->NextBlock();
    assert(block.size > 0 && block.size <= Partition::kBlockSize);
    ProcessBlock(args, block, mask, fanout, hist, dst);    
  }

  Finish(args);
}

void BuildTask::Finish(thread_t* my, hashtable_t *ht)
{
  // hash table partition
  Partition *htp = new Partition(my->node_id, key_);
  htp->set_hashtable(ht);
  out_->AddPartition(htp);

  // Commit
  in_->Commit(in_->GetPartitionsByKey(key_).size());

  // We create a new probe task on this node
  Taskqueue *queue = my->node->queue;
  int probe_task_id = probe_->id();

  Task *task = new ProbeTask(OpProbe, key_);
  queue->AddTask(probe_task_id, task);

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // set the output hashtable table to ready
  out_->set_ready();


  if (out_->type() == OpNone) {
    my->env->set_done();
    return;
  }
}



// They combine building and probing
// Does it mean they will have better cache behavior?
// Does it mean that I need to reorder the task list?
void BuildTask::Run(thread_t *my)
{
  uint32_t ntuples = 0;
  list<Partition*> &parts = in_->GetPartitionsByKey(key_);
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++) {
    ntuples += (*it)->size();
  }

  uint32_t nbuckets = ntuples;
  NEXT_POW_2(nbuckets);
  nbuckets <<= 1; // make the hash table twice as large
  const uint32_t MASK = (nbuckets-1) << (Params::kNumRadixBits);

  hashtable_t *ht = hashtable_init(ntuples, nbuckets);
  int *bucket = ht->bucket;
  entry_t *next = ht->next;

  int i = 0;
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++) {
    tuple_t *tuple = (*it)->tuples();
    for (uint32_t j = 0; j < (*it)->size(); j++) {
      uint32_t idx = HASH_BIT_MODULO(tuple->key, MASK, Params::kNumRadixBits);
      next[i].next = bucket[idx];
      next[i].tuple = *tuple;
      bucket[idx] = ++i;
      tuple++;
    }
  }

  Finish(my, ht);
}


void UnitProbeTask::ProbeBlock(thread_t *my, block_t block, hashtable_t *ht)
{
  tuple_t *tuple = block.tuples;
  const uint32_t MASK = (ht->nbuckets-1) << (Params::kNumRadixBits);
  int *bucket = ht->bucket;
  entry_t *next = ht->next;  

  // output buffer
  int buffer_id = my->tid * Params::kFanoutPass1 + part_->key();
  Partition *outp = out_->GetBuffer(buffer_id);

  for(uint32_t i = 0; i < block.size; i++ ){
    uint32_t idx = HASH_BIT_MODULO(tuple->key, MASK, Params::kNumRadixBits);
    for(int hit = bucket[idx]; hit > 0; hit = next[hit-1].next){
      if(tuple->key == next[hit-1].tuple.key){
        // Verion 1: check output for every match

        // there is no space
        if (!outp || Partition::kPartitionSize == outp->size()) {
          // if the buffer is full
          if (outp) {
            out_->AddPartition(outp);
          }

          // create a new buffer
          // switch the output buffer to the new one
          Partition *np = new Partition(my->node_id, part_->key());
          np->Alloc();

          out_->SetBuffer(buffer_id, np);
          outp = np;
        }
        tuple_t jr;
        jr.key = tuple->payload;
        jr.payload = next[hit-1].tuple.payload;
        outp->Append(jr);
      }
    }
  }
}


void UnitProbeTask::Run(thread_t *my)
{
  int key = part_->key();

  hashtable_t *ht = build_->GetPartitionsByKey(key).front()->hashtable();

  // process in blocks
  while (!part_->done()) {
    block_t block = part_->NextBlock();
    assert(block.size > 0 && block.size <= Partition::kBlockSize);
    ProbeBlock(my, block, ht);    
  }

  Finish(my);
}

void UnitProbeTask::Finish(thread_t* my)
{
  in_->Commit();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // Finish all remaining buffered partitions
  for (int i = 0; i < out_->nbuffers(); i++) {
    Partition *outp = out_->GetBuffer(i);
    if (outp) {
      out_->AddPartition(outp);
    }
  }

  // set output table to ready
  out_->set_ready();

  // check if there is no more work
  if (out_->type() == OpNone) {
    my->env->set_done();
    return;
  }

  // Unblock next operator
  // Only used in multi joins
  node_t *nodes = my->env->nodes();
  for (int node = 0; node < my->env->nnodes(); node++)
    nodes[node].queue->Unblock(out_->id());
}


void ProbeTask::Run(thread_t *my)
{
  Tasklist *probes = my->env->probes()[key_];
  my->localtasks = probes;
}
