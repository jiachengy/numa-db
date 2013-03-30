#include <glog/logging.h>

#include "params.h"
#include "hashjoin.h"

#include "perf.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

void PartitionTask::Finish(thread_t* my)
{
  in_->Commit();

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
    }
    break;
  default:
    break;
  }

  // unblock probing queues
  for (int node = 0; node < my->env->nnodes(); ++node)
    nodes[node].queue->Unblock(out_->id());
}


void PartitionTask::Run(thread_t *my)
{
#if PERF_PARTITION == 1
    perf_reset(my->perf);
#endif

  uint32_t mask = ((1 << nbits_) - 1) << offset_;
  uint32_t fanout = 1 << nbits_;
  uint32_t hist[fanout];
  tuple_t *dst[fanout];

  size_t ntuples_per_iter = Params::kBlockSize / sizeof(tuple_t);

  int iters = part_->size() / ntuples_per_iter;

  tuple_t *tuple = part_->tuples();
  for (int iter = 0; iter < iters; ++iter, tuple += ntuples_per_iter) {
    // reset histogram
    memset(hist, 0, fanout * sizeof(uint32_t));
  
    // first scan: set histogram
    for (uint32_t i = 0; i < ntuples_per_iter; i++) {
      uint32_t idx = HASH_BIT_MODULO(tuple[i].key, mask, offset_);
      hist[idx]++;
    }

    // set output buffer
    for (uint32_t idx = 0; idx < fanout; idx++) {
      int buffer_id = my->tid * fanout + idx;
      Partition *outp = out_->GetBuffer(buffer_id);

      // if buffer does not exists,  or if not enough space
      if (!outp || 
          (Params::kPartitionSize/sizeof(tuple_t)) - outp->size() < hist[idx]) {
        //  if the buffer is full
        if (outp)
          out_->AddPartition(outp);


#ifdef PRE_ALLOC
        Partition *np = my->recycler->GetEmptyPartition();
        np->set_key(idx);
#else
        Partition *np = new Partition(my->node_id, idx);
        np->Alloc();
#endif
        out_->SetBuffer(buffer_id, np);
        outp = np;
      }

      dst[idx] = &outp->tuples()[outp->size()];
      outp->set_size(outp->size() + hist[idx]); // set size at once
    }

    // second scan, partition and scatter
    for (uint32_t i = 0; i < ntuples_per_iter; i++) {
      uint32_t idx = HASH_BIT_MODULO(tuple[i].key, mask, offset_);
      *dst[idx] = tuple[i];
      dst[idx]++;
    }
  }



  size_t remainder = part_->size() - iters * ntuples_per_iter;

  if (remainder) {
    tuple = part_->tuples() + iters * ntuples_per_iter;  

    // reset histogram
    memset(hist, 0, fanout * sizeof(uint32_t));

    // first scan: set histogram
    for (uint32_t i = 0; i < remainder; i++) {
      uint32_t idx = HASH_BIT_MODULO(tuple[i].key, mask, offset_);
      hist[idx]++;
    }

    // set output buffer
    for (uint32_t idx = 0; idx < fanout; idx++) {
      int buffer_id = my->tid * fanout + idx;
      Partition *outp = out_->GetBuffer(buffer_id);

      // if buffer does not exists,  or if not enough space
      if (!outp || (Params::kPartitionSize / sizeof(tuple_t)) - outp->size() < hist[idx]) {
        //  if the buffer is full
        if (outp)
          out_->AddPartition(outp);


#ifdef PRE_ALLOC
        Partition *np = my->recycler->GetEmptyPartition();
        np->set_key(idx);
#else
        Partition *np = new Partition(my->node_id, idx);
        np->Alloc();
#endif
        out_->SetBuffer(buffer_id, np);
        outp = np;
      }

      dst[idx] = &outp->tuples()[outp->size()];
      outp->set_size(outp->size() + hist[idx]); // set size at once
    }

    // second scan, partition and scatter
    for (uint32_t i = 0; i < remainder; i++) {
      uint32_t idx = HASH_BIT_MODULO(tuple[i].key, mask, offset_);
      *dst[idx] = tuple[i];
      dst[idx]++;
    }
  }

  Finish(my);

#if PERF_PARTITION == 1
    perf_accum(my->perf);
#endif

}

void BuildTask::Finish(thread_t* my, Partition *htp)
{
  // hash table partition
  out_->AddPartition(htp);

  // Commit
  in_->Commit(in_->GetPartitionsByKey(key_).size());

  // We create a new probe task on this node
  Taskqueue *queue = my->node->queue;

  Task *task = new ProbeTask(OpProbe, key_);
  queue->AddTask(probe_->id(), task);

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
void BuildTask::Run(thread_t *my)
{
  uint32_t ntuples = 0;
  list<Partition*> &parts = in_->GetPartitionsByKey(key_);
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); it++)
    ntuples += (*it)->size();

  if (ntuples >= Params::kMaxHtTuples)
    LOG(INFO) << ntuples;
  assert(ntuples < Params::kMaxHtTuples);

#ifdef PRE_ALLOC
  Partition *htp = my->recycler->GetEmptyHT();
  hashtable_t *ht = htp->hashtable();
  htp->set_key(key_);
#else
  Partition *htp = new Partition(my->node_id, key_);
  hashtable_t *ht = hashtable_init(ntuples);
  htp->set_hashtable(ht);
#endif

  int *bucket = ht->bucket;
  entry_t *next = ht->next;
  const uint32_t MASK = (ht->nbuckets-1) << (Params::kNumRadixBits);

  // Begin to build the hash table
  uint32_t i = 0;
  for (list<Partition*>::iterator it = parts.begin();
       it != parts.end(); ++it) {

    tuple_t *tuple = (*it)->tuples();

    for (uint32_t j = 0; j < (*it)->size(); j++) {
      uint32_t idx = HASH_BIT_MODULO(tuple->key, MASK, Params::kNumRadixBits);
      next[i].next = bucket[idx];
      next[i].tuple = *tuple;

      bucket[idx] = ++i; // pos starts from 1 instead of 0
      tuple++;
    }
  }

  assert(i == ntuples);

  Finish(my, htp);
}

/*
void UnitProbeTask::ProbeBlock(thread_t *my, block_t block, hashtable_t *ht)
{
  const uint32_t MASK = (ht->nbuckets-1) << (Params::kNumRadixBits);
  int *bucket = ht->bucket;
  entry_t *next = ht->next;  

  // output buffer
  int buffer_id = my->tid * Params::kFanoutPass1 + part_->key();
  Partition *outp = out_->GetBuffer(buffer_id);

  tuple_t *tuple = block.tuples;
  for(uint32_t i = 0; i < block.size; i++, tuple++){
    uint32_t idx = HASH_BIT_MODULO(tuple->key, MASK, Params::kNumRadixBits);
    for(int hit = bucket[idx]; hit > 0; hit = next[hit-1].next){

      if(tuple->key == next[hit-1].tuple.key){
        // V1: check output for every match

        // there is no space
        if (!outp || Params::kPartitionSize == outp->size()) {
          // if the buffer is full
          if (outp)
            out_->AddPartition(outp);

          // create a new buffer
          // switch the output buffer to the new one
#ifdef PRE_ALLOC
          Partition *np = my->recycler->GetEmptyPartition();
          np->set_key(part_->key());
#else
          Partition *np = new Partition(my->node_id, part_->key());
          np->Alloc();
#endif

          out_->SetBuffer(buffer_id, np);
          outp = np;
        }
        tuple_t jr;
        jr.key = tuple->payload;
        jr.payload = next[hit-1].tuple.payload;
        outp->Append(jr);

        // tuple_t *t = &outp->tuples()[outp->size()];
        //   t->key = tuple->payload;
        //   t->payload = next[hit-1].tuple.payload;
        //   outp->set_size(outp->size() + 1);
      }      
    }
  }
}
*/

// TODO: unrolling the loop
void UnitProbeTask::Run(thread_t *my)
{
  // int key = part_->key();

  // hashtable_t *ht = build_->GetPartitionsByKey(key).front()->hashtable();


  // // process in blocks
  // while (!part_->done()) {
  //   block_t block = part_->NextBlock();
  //   ProbeBlock(my, block, ht);    
  // }

  // Finish(my);
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
    if (outp)
      out_->AddPartition(outp);
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
