#include <smmintrin.h>

#include "params.h"
#include "hashjoin.h"
#include "perf.h"

void FlushEntireBuffer(buffer_t *buffer, int node, Environment *env)
{
  // add the rest blocks into localblocks
  partition_t **blocks = buffer->partition;
  for (int i = 0; i != buffer->partitions; ++i) {
    if (blocks[i]->tuples != 0)
      FlushBuffer(buffer, blocks[i], env);
  }

  // merge with the global table
  Table * table = buffer->table;
  table->BatchAddBlocks(buffer->localblocks, buffer->tuples, node);

  vector<partition_t*> *buffered_blocks = buffer->localblocks;
  if (table->type() == OpPartition2) {
    for (vector<partition_t*>::iterator it = buffered_blocks->begin();
         it != buffered_blocks->end(); ++it) {
      partition_t *p = *it;
      PartitionTask *newtask = new PartitionTask(p, Params::kOffsetPass2, Params::kNumBitsPass2);

      P2Task ***p2tasks = env->GetP2TaskByTable(table->id());
      P2Task *p2task = p2tasks[p->node][p->radix];
      p2task->AddSubTask(newtask);

      if (!p2task->scheduled() && p2task->size() >= Params::kScheduleThreshold) {
        env->nodes()[p->node].queue->AddTask(table->id(), p2task);
        p2task->set_schedule(true);
      }
    }
  }
  else if (table->type() == OpProbe) {
    for (vector<partition_t*>::iterator it = buffered_blocks->begin();
         it != buffered_blocks->end(); ++it) {
      partition_t *p = *it;

      UnitProbeTask *newtask = new UnitProbeTask(p);
      ProbeTask **probetasks = env->GetProbeTaskByTable(table->id());
      ProbeTask *probe = probetasks[p->radix];
      probe->AddSubTask(newtask);
    }
  }
}

void FlushBuffer(buffer_t *buffer, partition_t *p, Environment *env)
{
  p->ready = true;
  buffer->localblocks->push_back(p);
  //  buffer->localblocks->AddBlock(p, index);
  buffer->tuples += p->tuples;

  //  table->AddPartition(p);
}


void
PartitionTask::DoPartition(thread_t *my)
{
  int shift = shift_;
  int fanout = fanout_;
  int mask = mask_;

  Memory * memm = my->memm[pass_-1];
#ifdef COLUMN_WISE
  intkey_t * key_out = memm->keybase();
  value_t * value_out = memm->valbase();
#else
  tuple_t *output = memm->baseptr();
#endif

  size_t block_size = memm->unit_size();
  uint64_t block_mask = block_size - 1;

  // check buffer compatibility
  pthread_mutex_lock(&my->lock);
  if (!buffer_compatible(my->buffer, out_, input_->radix)) {
    // flush old buffers
    if (my->buffer) {
      FlushEntireBuffer(my->buffer, my->node_id, my->env);
      buffer_destroy(my->buffer);
    }
    my->buffer = buffer_init(out_, input_->radix, shift, fanout);
    // initialize the output with initial buffer
    partition_t **buffer = my->buffer->partition;
    for (int i = 0; i != fanout; ++i) {
      assert(buffer[i] == NULL);
      // leave enough room for alignment
      if (buffer[i] == NULL) {
        buffer[i] = memm->GetPartition();
        buffer[i]->radix = encode_radix(input_->radix, i, shift);
      }
    }
  }
  pthread_mutex_unlock(&my->lock);

  partition_t **buffer = my->buffer->partition;


#ifdef COLUMN_WISE
  intkey_t * key_end = &input_->key[input_->tuples];
  intkey_t * key_ptr = input_->key;
  //  value_t * value_end = &input_->value[input_->tuples];
  value_t * value_ptr = input_->value;
#else
  tuple_t *tuple_end = &input_->tuple[input_->tuples];
  tuple_t *tuple_ptr = input_->tuple;
#endif

  uint32_t *wc_count = my->wc_count;
  memset(wc_count, 0x0, fanout * sizeof(uint32_t));
#ifdef COLUMN_WISE
  cache_line_t *wc_buf_key = my->wc_buf_key;
  cache_line_t *wc_buf_value = my->wc_buf_value;
  intkey_t ** part_key = my->wc_part_key;
  value_t ** part_value = my->wc_part_value;
#else
  // one cache line per partition
  cache_line_t *wc_buf = my->wc_buf;
  tuple_t **part = my->wc_part;
#endif

  for (int i = 0; i != fanout; ++i) {
#ifdef COLUMN_WISE
	part_key[i] = &buffer[i]->key[buffer[i]->tuples];
	part_value[i] = &buffer[i]->value[buffer[i]->tuples];
#else
    part[i] = &buffer[i]->tuple[buffer[i]->tuples];
#endif
  }

  // copy data to buffers and ensure cache-line aligned writes
  for (int i = 0 ; i != fanout ; ++i) {
#ifdef COLUMN_WISE
    uint64_t o, off = part_key[i] - key_out;
    wc_buf_key[i].data[15] = off;
    wc_buf_value[i].data[15] = off;
    off &= 15;
    for (o = 0 ; o != off ; ++o) {
      wc_buf_key[i].data[o] = ((uint32_t*)(part_key[i]))[o - off];
      wc_buf_value[i].data[o] = ((uint32_t*)(part_value[i]))[o - off];
	}
#else
    uint64_t o, off = part[i] - output;
    wc_buf[i].data[7] = off;
    off &= 7;
    for (o = 0 ; o != off ; ++o)
      wc_buf[i].data[o] = ((uint64_t*)(part[i]))[o - off];
#endif
  }

  // software write combining on page blocks
  do {
    // read and hash row
#ifdef COLUMN_WISE
    intkey_t key = *key_ptr;
	value_t value = *value_ptr;
    uint32_t hash = mhash(key, mask, shift);
    // offset in the cache line pair
    uint64_t index = wc_buf_key[hash].data[15]++;
    uint64_t mod_index = index & 15;
    // write in wc buffer
    wc_buf_key[hash].data[mod_index] = key;
    wc_buf_value[hash].data[mod_index] = value;
#else
    tuple_t row = *tuple_ptr;
    uint32_t hash = mhash(row.key, mask, shift);
    // offset in the cache line pair
    uint64_t index = wc_buf[hash].data[7]++;
    uint64_t mod_index = index & 7;
    // write in wc buffer
    wc_buf[hash].data[mod_index] = *(uint64_t*)tuple_ptr; // row; casting the tuple to 64bit
#endif
    // cache line is full
#ifdef COLUMN_WISE
    if (mod_index == 15) {
      // use 128-bit registers by default
      __m128i *src_x = (__m128i*) wc_buf_key[hash].data;
      __m128i *dest_x = (__m128i*) &key_out[index - 15];
      __m128i *src_y = (__m128i*) wc_buf_value[hash].data;
      __m128i *dest_y = (__m128i*) &value_out[index - 15];
      // load cache line from cache to 4 registers
      __m128i r0 = _mm_load_si128(&src_x[0]);
      __m128i r1 = _mm_load_si128(&src_x[1]);
      __m128i r2 = _mm_load_si128(&src_x[2]);
      __m128i r3 = _mm_load_si128(&src_x[3]);
      __m128i r4 = _mm_load_si128(&src_y[0]);
      __m128i r5 = _mm_load_si128(&src_y[1]);
      __m128i r6 = _mm_load_si128(&src_y[2]);
      __m128i r7 = _mm_load_si128(&src_y[3]);
      // store cache line from registers to memory
      _mm_stream_si128(&dest_x[0], r0);
      _mm_stream_si128(&dest_x[1], r1);
      _mm_stream_si128(&dest_x[2], r2);
      _mm_stream_si128(&dest_x[3], r3);
      _mm_stream_si128(&dest_y[0], r4);
      _mm_stream_si128(&dest_y[1], r5);
      _mm_stream_si128(&dest_y[2], r6);
      _mm_stream_si128(&dest_y[3], r7);
      // restore overwritten pointer
      wc_buf_key[hash].data[15] = index + 1;
      // check for end of buffer
      if (((index + 1) & block_mask) == 0) {
        // flush full buffer
        buffer[hash]->tuples = block_size;
        FlushBuffer(my->buffer, buffer[hash], my->env);
        // create new buffer
        
        buffer[hash] = memm->GetPartition();
        buffer[hash]->radix = encode_radix(input_->radix, hash, shift);

        wc_buf_key[hash].data[15] = buffer[hash]->offset;
      }
    }
  } while (++key_ptr != key_end);
  // send remaining items from buffers to output
  for (int i = 0 ; i != fanout ; ++i) {
    uint64_t index = wc_buf_key[i].data[15];
    part_key[i] = &key_out[index];
    part_value[i] = &value_out[index];
    int p = index & 15;
    for (int j = 0 ; j != p ; ++j) {
	  part_key[i][j-p] = wc_buf_key[i].data[j];
	  part_value[i][j-p] = wc_buf_value[i].data[j];
	}
    buffer[i]->tuples = (index & block_mask); // set the new size
  }
#else
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
        buffer[hash]->tuples = block_size;
        
        FlushBuffer(my->buffer, buffer[hash], my->env);

        // create new buffer
        buffer[hash] = memm->GetPartition();
        buffer[hash]->radix = encode_radix(input_->radix, hash, shift);
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
#endif
}

void PartitionTask::Finish(thread_t* my)
{
  if (input_->memm)
    input_->memm->Recycle(input_);
 
  // commit and check if I am the last one to finish? 
  if (!in_->Commit())
    return;
  
  // force flush
  thread_t * thread = my->env->threads();
  for (int tid = 0; tid != my->env->nthreads(); ++tid) {
    pthread_mutex_lock(&thread[tid].lock);
    buffer_t *buffer = thread[tid].buffer;
    // force flush all remaining buffers
    if (buffer && buffer->table == out_) {
      thread[tid].buffer = NULL;
      pthread_mutex_unlock(&thread[tid].lock); // release the lock as soon as you reset the buffer   
      FlushEntireBuffer(buffer, thread[tid].node_id, my->env);
      buffer_destroy(buffer);
    }
    else {
      pthread_mutex_unlock(&thread[tid].lock); // release the lock as soon as you reset the buffer      
    }
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
      P2Task ***p2tasks = my->env->GetP2TaskByTable(out_->id());
      for (int radix = 0; radix != Params::kFanoutPass1; ++radix) {
        for (int node = 0; node != my->env->nnodes(); ++node) {
          P2Task *p2task = p2tasks[node][radix];
          if (!p2task->scheduled()) {
            my->env->nodes()[node].queue->AddTask(out_->id(), p2task);
            p2task->set_schedule(true);
          }
        }
      }
      // node_t *nodes = my->env->nodes();
      // for (int node = 0; node != my->env->nnodes(); ++node) {
      //   nodes[node].queue->Unblock(out_->id());
      // }
    }
    break;
  case OpBuild: //  Unblock build
    {
      // node_t *nodes = my->env->nodes();
      // for (int node = 0; node < my->env->nnodes(); ++node)
      //   nodes[node].queue->Unblock(out_->id());
    }
    break;
  case OpProbe: // unblock probing queues
    {
      // node_t *nodes = my->env->nodes();
      // for (int node = 0; node < my->env->nnodes(); ++node)
      //   nodes[node].queue->Unblock(out_->id());
    }
    break;
  default:
    break;
  }

  node_t *nodes = my->env->nodes();
  for (int node = 0; node < my->env->nnodes(); ++node)
    nodes[node].queue->UnblockNext(in_->id());

}


void PartitionTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  DoPartition(my);
  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);

  Finish(my);

  if (pass_ == 1) {
    if (in_->id() == 0)
      perf_counter_aggr(&my->stage_counter[0], state);
    else
      perf_counter_aggr(&my->stage_counter[5], state);
  }
  else if (in_->id() == 1)
    perf_counter_aggr(&my->stage_counter[3], state);
  else
    perf_counter_aggr(&my->stage_counter[4], state);
}

