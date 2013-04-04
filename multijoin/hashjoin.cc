#include <smmintrin.h>

#include "params.h"
#include "hashjoin.h"

#include "perf.h"

inline uint32_t encode_radix(uint32_t low, uint32_t high, int offset)
{
  return low | (high << offset);
}

inline uint32_t mhash(uint32_t key, uint32_t mask, int shift)
{
  return (key & mask) >> shift;
}

void FlushBuffer(Table * table, partition_t *p, Environment *env)
{
  p->ready = true;
  table->AddPartition(p);

  if (table->type() == OpPartition2) {
    PartitionTask *newtask = new PartitionTask(p, Params::kOffsetPass2, Params::kNumBitsPass2);

    P2Task ***p2tasks = env->GetP2TaskByTable(table->id());
    P2Task *p2task = p2tasks[p->node][p->radix];
    p2task->AddSubTask(newtask);
  }
}

void
PartitionTask::DoPartition(thread_t *my)
{
  tuple_t *output = my->memm->baseptr();
  partition_t * inp = part_;

  int shift = shift_;
  int fanout = fanout_;
  int mask = mask_;

  uint64_t block_mask = Params::kMaxTuples - 1;

  // check buffer compatibility
  if (!buffer_compatible(my->buffer, out_, inp->radix)) {
    // flush old buffers
    if (my->buffer) {
      partition_t **buffer = my->buffer->partition;
      for (int i = 0; i != my->buffer->partitions; ++i) {
        if (buffer[i]->tuples != 0)
          FlushBuffer(my->buffer->table, buffer[i], my->env);
      }
      buffer_destroy(my->buffer);
    }
    my->buffer = buffer_init(out_, inp->radix, fanout);
  }
  partition_t **buffer = my->buffer->partition;

  // initialize the output with initial buffer
  for (int i = 0; i != fanout; ++i) {
    // leave enough room for alignment
    if (buffer[i] == NULL) {
      buffer[i] = my->memm->GetPartition();
      buffer[i]->radix = encode_radix(inp->radix, i, shift);
    }
  }

  tuple_t *tuple_end = &inp->tuple[inp->tuples];
  tuple_t *tuple_ptr = inp->tuple;

  // one cache line per partition
  cache_line_t *wc_buf = (cache_line_t*)alloc(fanout * sizeof(cache_line_t));
  uint32_t *wc_count = (uint32_t*)calloc(fanout, sizeof(uint32_t));
  tuple_t **part = (tuple_t**)malloc(fanout * sizeof(tuple_t*));

  for (int i = 0; i != fanout; ++i)
    part[i] = &buffer[i]->tuple[buffer[i]->tuples];

  // copy data to buffers and ensure cache-line aligned writes
  for (int i = 0 ; i != fanout ; ++i) {
    uint64_t o, off = part[i] - output;
    wc_buf[i].data[7] = off;
    off &= 7;
    for (o = 0 ; o != off ; ++o)
      wc_buf[i].data[o] = ((uint64_t*)(part[i]))[o - off];
  }

  // software write combining on page blocks
  do {
    // read and hash row
    tuple_t row = *tuple_ptr;
    uint32_t hash = mhash(row.key, mask, shift);
    // offset in the cache line pair
    uint64_t index = wc_buf[hash].data[7]++;
    uint64_t mod_index = index & 7;
    // write in wc buffer
    wc_buf[hash].data[mod_index] = *(uint64_t*)tuple_ptr; // row; casting the tuple to 64bit
    // cache line is full
    if (mod_index == 7) {
      // use 128-bit registers by default
      __m128i *src = (__m128i*) wc_buf[hash].data;
      __m128i *dest = (__m128i*) &output[index - 7];
      // load cache line from cache to 4 registers
      __m128i r0 = _mm_load_si128(&src[0]);
      __m128i r1 = _mm_load_si128(&src[1]);
      __m128i r2 = _mm_load_si128(&src[2]);
      __m128i r3 = _mm_load_si128(&src[3]);
      // store cache line from registers to memory
      _mm_stream_si128(&dest[0], r0);
      _mm_stream_si128(&dest[1], r1);
      _mm_stream_si128(&dest[2], r2);
      _mm_stream_si128(&dest[3], r3);
      // restore overwritten pointer
      wc_buf[hash].data[7] = index + 1;
      // check for end of buffer
      if (((index + 1) & block_mask) == 0) {
        // flush full buffer
        buffer[hash]->tuples = Params::kMaxTuples;
        FlushBuffer(my->buffer->table, buffer[hash], my->env);
        // create new buffer
        buffer[hash] = my->memm->GetPartition();
        buffer[hash]->radix = encode_radix(inp->radix, hash, shift);
        wc_buf[hash].data[7] = buffer[hash]->offset;
      }
    }
  } while (++tuple_ptr != tuple_end);
  // send remaining items from buffers to output
  for (int i = 0 ; i != fanout ; ++i) {
    uint64_t index = wc_buf[i].data[7];
    part[i] = &output[index];
    int p = index & 7;
    for (int j = 0 ; j != p ; ++j)
      ((uint64_t*)part[i])[j - p] = wc_buf[i].data[j];
    
    buffer[i]->tuples = (index & block_mask); // set the new size
  }

  free(wc_count);
  free(wc_buf);
  free(part);
}



void PartitionTask::Finish(thread_t* my)
{
  in_->Commit();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // set output table to ready
  out_->set_ready();

  if (out_->type() == OpNone) {
    my->env->commit();
  }

  if (my->env->queries() == 0) {
    // force flush only if we are the last operation
    thread_t * thread = my->env->threads();
    for (int tid = 0; tid != my->env->nthreads(); ++tid) {
      buffer_t *buffer = thread[tid].buffer; // NEED a latch to do this work
      // force flush all remaining buffers
      if (buffer) {
        for (int i = 0; i < buffer->partitions; i++) {
          if (buffer->partition[i]->tuples != 0) {
            FlushBuffer(buffer->table, buffer->partition[i], my->env);
          }
        }
        buffer_destroy(buffer);
      }
    }
    my->env->set_done();
    return;
  }

  switch (out_->type()) {
  case OpNone: // Set termination flag
    return;
  case OpPartition:
    return;
  case OpPartition2:
    {
      node_t *nodes = my->env->nodes();
      for (int node = 0; node != my->env->nnodes(); ++node) {
        logging("Unblock task %d\n", out_->id());
        nodes[node].queue->Unblock(out_->id());
      }
    }
    break;
  case OpBuild: //  Unblock build
    break;
  case OpProbe: // create probe task
    {
      node_t *nodes = my->env->nodes();
      for (int key = 0; key < Params::kFanoutPass1; key++) {
        Tasklist *probelist = my->env->probes()[key];

        list<partition_t*> &parts = out_->GetPartitionsByKey(key);
        for (list<partition_t*>::iterator it = parts.begin();
             it != parts.end(); it++) {
          Task *task = new UnitProbeTask(OpUnitProbe, *it, my->env->build_table());
          probelist->AddTask(task);
        }
      }
      // unblock probing queues
      for (int node = 0; node < my->env->nnodes(); ++node)
        nodes[node].queue->Unblock(out_->id());
    }
    break;
  default:
    break;
  }
}


void PartitionTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  DoPartition(my);
  Finish(my);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);

  perf_counter_aggr(&my->stage_counter, state);
}

void BuildTask::Finish(thread_t* my, partition_t *htp)
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
  list<partition_t*> &parts = in_->GetPartitionsByKey(key_);
  for (list<partition_t*>::iterator it = parts.begin();
       it != parts.end(); it++)
    ntuples += (*it)->tuples;

  partition_t *htp = my->memm->GetPartition();
  hashtable_t *ht = htp->hashtable;
  htp->radix = key_;

  int *bucket = ht->bucket;
  entry_t *next = ht->next;
  const uint32_t MASK = (ht->nbuckets-1) << (Params::kNumRadixBits);

  // Begin to build the hash table
  uint32_t i = 0;
  for (list<partition_t*>::iterator it = parts.begin();
       it != parts.end(); ++it) {

    tuple_t *tuple = (*it)->tuple;

    for (uint32_t j = 0; j < (*it)->tuples; j++) {
      uint32_t idx = mhash(tuple->key, MASK, Params::kNumRadixBits);
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
  // for (int node = 0; node < my->env->nnodes(); node++) {
  //   for (int i = 0; i < out_->nbuffers(); i++) {
  //     partition_t *outp = out_->GetBuffer(node, i);
  //     if (outp)
  //       out_->AddPartition(outp);
  //   }
  // }

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
  //  Tasklist *probes = my->env->probes()[key_];
  my->batch_task = this;
}
