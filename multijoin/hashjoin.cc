#include <glog/logging.h>

#include "params.h"
#include "hashjoin.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

#ifndef NEXT_POW_2
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

void PartitionTask::ProcessBlock(thread_t *my, block_t block, uint32_t mask, uint32_t fanout, uint32_t hist[], tuple_t *dst[])
{

  // histogram
  memset(hist, 0, fanout * sizeof(uint32_t)); // clear histogram

  tuple_t *tuple = block.tuples;
  for (uint32_t i = 0; i < block.size; i++) {
    uint32_t idx = HASH_BIT_MODULO((tuple++)->key, mask, offset_);
    hist[idx]++;
  }

  // check output buffer
  for (uint32_t idx = 0; idx < fanout; idx++) {
    int buffer_id = my->tid * fanout + idx;
    Partition *outp = out_->GetBuffer(buffer_id);

    // if buffer does not exists
    // or if not enough space
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

  // We are ready to recycle or destroy the partition
  // WARNING: DELETE PARTITION IS DANGEROUS. IT WILL LEAD TO DOUBLE DELETE
  // WHEN WE ARE DELETING THE TABLE
  //  delete part_;

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
      out_->AddPartition(outp);
    }
  }

  LOG(INFO) << "Put buffers done.";

  // set output table to ready
  out_->set_ready();


  // check if there is no more work
  if (out_->type() == OpNone) {
    my->env->set_done();
    return;
  }

  LOG(INFO) << "Unlock build task.";

  //  Unblock next operator
  //  my->env->nodes()[0].queue->Unblock(out_->id());
  
  node_t *nodes = my->env->nodes();
  for (int node = 0; node < my->env->nnodes(); node++) {
    nodes[node].queue->Unblock(out_->id());
    LOG(INFO) << nodes[node].queue->active_size();
  }
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
  in_->Commit(in_->GetPartitionsByKey(key_).size());

  // We create a new probe task on this node
  Taskqueue *queue = my->node->queue;
  int probe_task_id = probe_->id();

  Task *task = new ProbeTask(OpProbe, probe_, probe_out_, out_, key_);
  queue->AddTask(probe_task_id, task);

  LOG(INFO) << "ProbeTask of key " << key_ << " is created";

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  LOG(INFO) << "I am the last.";

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
  LOG(INFO) << "Start building partition " << key_;

  // Step 1: Estimate the size of the hash table
  // Traverse all partitions in the list
  
  uint32_t ntuples = 0;

  list<Partition*> &parts = in_->GetPartitionsByKey(key_);
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++) {
    ntuples += (*it)->size();
  }

  int *bucket;
  entry_t *next;
  uint32_t nbuckets = ntuples;
  NEXT_POW_2(nbuckets);
  nbuckets <<= 1; // make the hash table twice as large
  const uint32_t MASK = (nbuckets-1) << (Params::kNumRadixBits);
  next   = (entry_t*)alloc(sizeof(entry_t) * ntuples);
  bucket = (int*) alloc(nbuckets * sizeof(int));
  assert(next != NULL);
  assert(bucket != NULL);

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
      uint32_t idx = HASH_BIT_MODULO(tuple->key, MASK, Params::kNumRadixBits);
      next[i].next = bucket[idx];
      next[i].tuple = *tuple;
      bucket[idx] = ++i;
      tuple++;
    }
  }
  LOG(INFO) << "Finish building.";

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
  LOG(INFO) << "Unit probing starts.";

  int key = part_->key();

  hashtable_t *ht = build_->GetPartitionsByKey(key).front()->hashtable();

  assert(ht != NULL);

  // process in blocks
  while (!part_->done()) {
    block_t block = part_->NextBlock();
    assert(block.size > 0 && block.size <= Partition::kBlockSize);
    ProbeBlock(my, block, ht);    
  }

  Finish(my);
  LOG(INFO) << "Unit probing ends.";
}

void UnitProbeTask::Finish(thread_t* my)
{
  in_->Commit();

  LOG(INFO) << "done " << in_->done_count()
            << " size " << in_->nparts();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  LOG(INFO) << "I am the last probing.";

  // Finish all remaining buffered partitions
  for (int i = 0; i < out_->nbuffers(); i++) {
    Partition *outp = out_->GetBuffer(i);
    if (outp) {
      out_->AddPartition(outp);
    }
  }

  LOG(INFO) << "Put buffers done.";

  // set output table to ready
  out_->set_ready();

  // check if there is no more work
  if (out_->type() == OpNone) {
    my->env->set_done();
    return;
  }

  // Unblock next operator
  // Only used in multi joins

  assert(out_->type() == OpPartition);
  
  node_t *nodes = my->env->nodes();
  for (int node = 0; node < my->env->nnodes(); node++)
    nodes[node].queue->Unblock(out_->id());
}


void ProbeTask::Run(thread_t *my)
{
  LOG(INFO) << "Probing starts.";

  // the only thing i need to do is to create a
  // local tasklist that contains unit probing tasks
  Tasklist *probes = new Tasklist(in_, out_, ShareLocal);
  list<Partition*> &toprobes = in_->GetPartitionsByKey(key_);

  for (list<Partition*>::iterator it = toprobes.begin();
       it != toprobes.end(); ++it) {
    Task *task = new UnitProbeTask(OpUnitProbe, *it, in_, out_, build_);
    probes->AddTask(task);
  }

  LOG(INFO) << "Local probe list of size " << probes->size() << " is created on node " << my->node_id << " key " << key_;

  my->localtasks = probes;

  LOG(INFO) << "Probing ends.";
}
