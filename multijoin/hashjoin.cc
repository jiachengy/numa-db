#include <smmintrin.h>

#include "params.h"
#include "hashjoin.h"
#include "perf.h"

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
  else if (table->type() == OpProbe) {
    UnitProbeTask *newtask = new UnitProbeTask(p);
    ProbeTask **probetasks = env->GetProbeTaskByTable(table->id());
    probetasks[p->radix]->AddSubTask(newtask);
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
  pthread_mutex_lock(&my->lock);
  if (!buffer_compatible(my->buffer, out_, inp->radix)) {
    // flush old buffers
    if (my->buffer) {
      partition_t **buffer = my->buffer->partition;
      for (int i = 0; i != my->buffer->partitions; ++i)
        if (buffer[i]->tuples != 0)
          FlushBuffer(my->buffer->table, buffer[i], my->env);
      buffer_destroy(my->buffer);
    }
    my->buffer = buffer_init(out_, inp->radix, fanout);
  }
  pthread_mutex_unlock(&my->lock);

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
  cache_line_t *wc_buf = (cache_line_t*)alloc_aligned(fanout * sizeof(cache_line_t), CACHE_LINE_SIZE);
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

  // force flush
  thread_t * thread = my->env->threads();
  for (int tid = 0; tid != my->env->nthreads(); ++tid) {
    pthread_mutex_lock(&thread[tid].lock);
    buffer_t *buffer = thread[tid].buffer; // NEED a latch to do this work
    // force flush all remaining buffers
    if (buffer && buffer->table == out_) {
      for (int i = 0; i != buffer->partitions; ++i)
        if (buffer->partition[i]->tuples != 0)
          FlushBuffer(buffer->table, buffer->partition[i], my->env);
      buffer_destroy(buffer);
      thread[tid].buffer = NULL;
    }
    pthread_mutex_unlock(&thread[tid].lock);
  }

  // set output table to ready
  // only if we have flushed all buffers
  out_->set_ready();

  if (out_->type() == OpNone)
    my->env->commit();

  switch (out_->type()) {
  case OpNone: // Set termination flag
    if (my->env->queries() == 0)
      my->env->set_done();
    return;
  case OpPartition:
    return;
  case OpPartition2:
    {
      node_t *nodes = my->env->nodes();
      for (int node = 0; node != my->env->nnodes(); ++node) {
        nodes[node].queue->Unblock(out_->id());
      }
    }
    break;
  case OpBuild: //  Unblock build
    {
      node_t *nodes = my->env->nodes();
      for (int node = 0; node < my->env->nnodes(); ++node)
        nodes[node].queue->Unblock(out_->id());
    }
    break;
  case OpProbe: // unblock probing queues
    {
      node_t *nodes = my->env->nodes();
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

  perf_counter_aggr(&my->stage_counter[0], state);
}

