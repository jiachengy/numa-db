#include <smmintrin.h>

#include "params.h"
#include "hashjoin.h"
#include "perf.h"

inline size_t get_hist_size(size_t tuples) 
{
  NEXT_POW_2(tuples);
  tuples >>= 2;
  if(tuples < 4) tuples = 4; 
  return tuples;
}

hashtable_t * BuildTask::Build(thread_t * my)
{
  list<partition_t*> &parts = in_->GetPartitionsByKey(radix_);

  size_t total_tuples = 0;
  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it )
    total_tuples += (*it)->tuples;

  size_t partitions = get_hist_size(total_tuples);
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  uint32_t *hist   = (uint32_t*) calloc(partitions, sizeof(uint32_t));
  uint32_t *sum   = (uint32_t*) calloc(partitions + 1, sizeof(uint32_t));
  // temp output buffer holder
  tuple_t **part = (tuple_t**)malloc(partitions * sizeof(tuple_t*));

  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple = (*it)->tuple;
    size_t tuples = (*it)->tuples;
    for (uint64_t i = 0; i != tuples ; ++i) {
      uint32_t hash = mhash(tuple[i].key, mask, shift);
      hist[hash]++;
    }
  }

  // TODO: consider overflow hash tables
  assert(total_tuples <= Params::kMaxTuples);
  partition_t * outp = my->memm->GetPartition();
  outp->tuples = total_tuples;

  /* prefix sum on histogram */
  for( uint32_t i = 0; i != partitions; i++ ) {
    sum[i] = i ? sum[i-1] + hist[i-1] : 0;
    part[i] = outp->tuple + sum[i];
  }
  sum[partitions] = total_tuples;

  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple = (*it)->tuple;
    for (uint64_t i = 0; i < (*it)->tuples; ++i) {
      uint32_t hash = mhash(tuple[i].key, mask, shift);
      *(part[hash]++) = tuple[i];
    }
  }

  hashtable_t * ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  ht->data = outp;
  ht->tuples = total_tuples;
  ht->sum = sum;
  ht->partitions = partitions;

  /*
  // validate
  long long sum = 0;
  for(list<partition_t*>::iterator it = parts.begin();
  it != parts.end(); ++it ) {
  tuple_t * tuple = (*it)->tuple;
  size_t tuples = (*it)->tuples;
  for (uint64_t i = 0; i != tuples ; ++i) {
  sum += tuple[i].key;
  }
  }
  long long sum1 = 0;
  tuple_t * tuple = outp->tuple;
  for (uint32_t i = 0 ; i != partitions ; ++i) {
  assert(part_end[i] == part_start[i] + hist[i]);
  for (uint32_t j = part_start[i]; j != part_end[i]; ++j) {
  assert(mhash(tuple[j].key, mask, shift) == i);
  sum1 += tuple[j].key;
  }
  }
  assert(sum == sum1);
  */

  free(part);
  free(hist);
  
  return ht;
}


void BuildTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  hashtable_t *ht = Build(my);
  Finish(my, ht);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);
  perf_counter_aggr(&my->stage_counter[1], state);
}


void BuildTask::Finish(thread_t* my, hashtable_t *ht)
{
  partition_t * htp = partition_init(my->node_id);
  htp->hashtable = ht;
  htp->radix = radix_;
  htp->ready = true;

  // hash table partition
  out_->AddPartition(htp);

  // Commit
  in_->Commit(in_->GetPartitionsByKey(radix_).size());

  // We create a new probe task on this node
  Taskqueue *queue = my->node->queue;
  ProbeTask **probetask = my->env->GetProbeTaskByTable(probe_->id());
  queue->AddTask(probe_->id(), probetask[radix_]);

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // set the output hashtable table to ready
  out_->set_ready();

  if (out_->type() == OpNone) 
    my->env->commit();

  if (my->env->queries() == 0)
    my->env->set_done();
}




#ifdef WRITE_COMBINE
hashtable_t * BuildTask::Build(thread_t * my)
{
  tuple_t * output = my->memm->baseptr();

  list<partition_t*> &parts = in_->GetPartitionsByKey(radix_);

  size_t total_tuples = 0;
  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it )
    total_tuples += (*it)->tuples;

  size_t partitions = get_hist_size(total_tuples);
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  uint64_t *hist   = (uint64_t*) calloc(partitions, sizeof(uint64_t));
  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple = (*it)->tuple;
    size_t tuples = (*it)->tuples;
    for (uint64_t i = 0; i != tuples ; ++i) {
      uint32_t hash = mhash(tuple[i].key, mask, shift);
      hist[hash]++;
    }
  }
  
  // temp output buffer holder
  tuple_t **part = (tuple_t**)malloc(partitions * sizeof(tuple_t*));
  // index start, index end
  uint32_t *part_start = (uint32_t*)malloc(partitions * sizeof(uint32_t));
  uint32_t *part_end = (uint32_t*)malloc(partitions * sizeof(uint32_t));

  // TODO: consider overflow hash tables
  assert(total_tuples <= Params::kMaxTuples);
  partition_t * outp = my->memm->GetPartition();
  outp->tuples = total_tuples;

  /* prefix sum on histogram */
  for( uint32_t i = 0; i != partitions; i++ ) {
    part[i] = i ? part[i-1] + hist[i-1] : outp->tuple;
    part_start[i] = i ? part_start[i-1] + hist[i-1] : 0;
    part_end[i] = part_start[i] + hist[i];
  }

  cache_line_t *wc_buf = (cache_line_t*)alloc(partitions * sizeof(cache_line_t));
  uint32_t *wc_count = (uint32_t*)calloc(partitions, sizeof(uint32_t));

  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple_ptr = (*it)->tuple;
    tuple_t * tuple_end = &(*it)->tuple[(*it)->tuples];

    if (it == parts.begin()) {
      // alignment only at the first loop
      uint64_t i = partitions; 
      do {
		tuple_t row = *tuple_ptr++;
		uint32_t hash = mhash(row.key, mask, shift);
		*part[hash]++ = row;
		if (++wc_count[hash] == 7 && !--i) break;
      } while (tuple_ptr != tuple_end);

      for (uint64_t i = 0 ; i != partitions ; ++i) {
        uint64_t o, off = part[i] - output;
        wc_buf[i].data[7] = off;
        off &= 7;
        for (o = 0 ; o != off ; ++o)
          wc_buf[i].data[o] = ((uint64_t*)part[i])[o - off];
      }
    }

	// main loop
	if (tuple_ptr != tuple_end)
      do {
		// read and hash row
		tuple_t row = *tuple_ptr;
		uint32_t hash = mhash(row.key, mask, shift);
		// offset in wc buffer (loads cache line)
		uint64_t index = wc_buf[hash].data[7]++;
		uint64_t mod_index = index & 7;
		// write in wc buffer
		wc_buf[hash].data[mod_index] = *(uint64_t*)tuple_ptr;
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
		}
      } while (++tuple_ptr != tuple_end);
  }
  
  // send remaining items from buffers to output
  // only when we finish processing all
  for (uint64_t i = 0 ; i != partitions ; ++i) {
    uint64_t j, p = wc_buf[i].data[7];
    part[i] = &output[p];
    p &= 7;
    for (j = 0 ; j != p ; ++j)
      ((uint64_t*)part[i])[j - p] = wc_buf[i].data[j];
  }

  hashtable_t * ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  ht->data = outp;
  ht->tuples = total_tuples;
  ht->start = part_start;
  ht->end = part_end;
  ht->partitions = partitions;

  free(wc_count);
  free(wc_buf);
  free(part);
  free(hist);
  
  return ht;
}
#endif

#ifdef BUCKET_CHAINING
// NOT IMPLEMENTED
hashtable_t* BuildTask::Build(thread_t *my)
{
  /* count number of tuples */
  size_t tuples = 0;
  list<partition_t*> &parts = in_->GetPartitionsByKey(key_);
  for (list<partition_t*>::iterator it = parts.begin(); it != parts.end(); it++)
    tuples += (*it)->tuples;

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
}
#endif

void UnitProbeTask::DoJoin(hashtable_t *hashtable, thread_t *my)
{
  size_t partitions = hashtable->partitions;
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  // check buffer compatibility
  if (!buffer_compatible(my->buffer, out_, input_->radix)) {
    pthread_mutex_lock(&my->lock);
    // flush old buffers
    if (my->buffer) {
      partition_t **buffer = my->buffer->partition;
      for (int i = 0; i != my->buffer->partitions; ++i)
        if (buffer[i]->tuples != 0)
          FlushBuffer(my->buffer->table, buffer[i], my->env);
      buffer_destroy(my->buffer);
    }
    // we only need one buffer
    my->buffer = buffer_init(out_, input_->radix, 1);
    pthread_mutex_unlock(&my->lock);
  }

  if (my->buffer->partition[0] == NULL) {
    my->buffer->partition[0] = my->memm->GetPartition();
    my->buffer->partition[0]->radix = input_->radix;
  }    

  partition_t *outbuf = my->buffer->partition[0];

  tuple_t * stuple = input_->tuple;
  tuple_t * rtuple = hashtable->data->tuple;
  uint32_t * sum = hashtable->sum;

  //  long matches = 0;
  // start from what is remaining
  tuple_t * output = outbuf->tuple + outbuf->tuples;
  tuple_t * outend = outbuf->tuple + Params::kMaxTuples;
  for (uint64_t i = 0; i != input_->tuples; ++i) {
    uint32_t idx = mhash(stuple[i].key, mask, shift);
    uint64_t j = sum[idx], end = sum[idx+1];
    for (; j != end; ++j) {
      if (stuple[i].key == rtuple[j].key) {
        output->key = (intkey_t)stuple[i].payload;
        output->payload = (intkey_t)rtuple[i].payload;
        output++;
        if (output == outend) {
          outbuf->tuples = Params::kMaxTuples;
          FlushBuffer(my->buffer->table, outbuf, my->env);
          
          outbuf = my->memm->GetPartition();
          outbuf->radix = input_->radix;
          output = outbuf->tuple;
          outend = outbuf->tuple + Params::kMaxTuples;
          my->buffer->partition[0] = outbuf;
        }
        //        matches++;
      }
    }
  }

  // set buffer size
  outbuf->tuples = output - outbuf->tuple;
 //  outbuf->tuples = matches;
}


// First write a version without output
void UnitProbeTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  int radix = input_->radix;
  Table *buildtable = my->env->build_table();
  list<partition_t*> &htps = buildtable->GetPartitionsByKey(radix);
  hashtable_t *hashtable = htps.front()->hashtable;

  DoJoin(hashtable, my);

  Finish(my);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);
  perf_counter_aggr(&my->stage_counter[2], state);

}

void UnitProbeTask::Finish(thread_t* my)
{
  in_->Commit();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // Flush buffers
  // force flush only if we are the last operation
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
  out_->set_ready();

  // check if there is no more work
  if (out_->type() == OpNone)
    my->env->commit();

  if (my->env->queries() == 0)
    my->env->set_done();

}
